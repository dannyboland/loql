import asyncio
import os
from dataclasses import dataclass
from multiprocessing import Process, Queue
from typing import Any, List

import duckdb
from rich.console import ConsoleRenderable
from rich.styled import Styled
from rich.table import Table
from rich.text import Text
from textual.message_pump import MessagePump

from clidb import lazy_import
from clidb.events import (
    DatabaseViewsUpdate,
    OpenFile,
    Query,
    QueryResult,
    UpdateTextInput,
)

try:
    pd = lazy_import("pandas")
    _has_pd = True
except ImportError:
    _has_pd = False

try:
    boto3 = lazy_import("boto3")
    _has_boto = True
except ImportError:
    _has_boto = False

MAX_RESULT_ROWS = 100
MAX_CELL_LENGTH = 100


@dataclass
class FileObj:
    filename: str


class QueryError(Text):
    """A query result that reflects an error status"""

    def __init__(self, error: str) -> None:
        super().__init__(error, justify="center")


class DatabaseProcess(Process):
    """Separate process as thin wrapper around duckdb"""

    end_queue_sentinel = object()

    def __init__(self, read_clipboard=False, row_lines=False):
        super().__init__()
        self.query_queue = Queue()
        self.result_queue = Queue()
        self.read_clipboard = read_clipboard
        self.row_lines = row_lines

    def run(self) -> None:
        self.con = duckdb.connect(database=":memory:")
        self.con.view("duckdb_views").create_view("schemas")

        if self.read_clipboard and _has_pd:
            self.__load_clipboard_as_view()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__await_query())

    async def __await_query(self):
        while True:
            result = None
            query_obj = self.query_queue.get()
            if query_obj == self.end_queue_sentinel:
                break
            elif query_obj is None:
                result = ""
            elif isinstance(query_obj, str):
                result = self.__query(query_obj)
            elif isinstance(query_obj, FileObj):
                try:
                    result = self.__load_file_as_view(query_obj.filename)
                except (FileNotFoundError, ValueError, RuntimeError):
                    result = QueryError(f"Failed to load {query_obj.filename}.")
                except ImportError:
                    result = QueryError("Please install clidb[extras].")

            views = self.__get_views()
            self.result_queue.put((result, views))

    def __get_views(self) -> List[str]:
        """Returns a list of defined view names"""
        return [schema[2] for schema in self.con.view("schemas").fetchall()]

    def __load_file_as_view(self, filename: str) -> str:
        """Creates a view for a data file and returns the view's name"""
        file_path, file_type = os.path.splitext(filename)
        view_name = os.path.basename(file_path)

        if file_type == ".gz" and view_name.endswith(".parquet"):
            file_type = ".parquet"
            view_name = view_name[: -len(".parquet")]

        if filename.startswith("s3://") and file_type == ".parquet":
            if _has_pd and _has_boto:
                self.con.register(view_name, pd.read_parquet(filename))
            else:
                raise ImportError
        elif file_type == ".csv":
            self.con.from_csv_auto(filename).create_view(view_name)
        elif file_type == ".parquet":
            self.con.from_parquet(filename).create_view(view_name)
        elif file_type == ".json":
            if _has_pd:
                self.con.register(view_name, pd.read_json(filename))
            else:
                raise ImportError
        elif file_type == ".jsonl":
            if _has_pd:
                self.con.register(view_name, pd.read_json(filename, lines=True))
            else:
                raise ImportError
        elif file_type in (".xls", ".xlsx"):
            if _has_pd:
                self.con.register(view_name, pd.read_excel(filename))
            else:
                raise ImportError
        else:
            raise ValueError

        return view_name

    def __load_clipboard_as_view(self) -> None:
        """Creates a view of data from the clipboard"""
        view_name = "clipboard"
        if _has_pd:
            self.con.register(view_name, pd.read_clipboard())
        else:
            raise ImportError

    @staticmethod
    def __format_cell(value: Any) -> str:
        return str(value)[:MAX_CELL_LENGTH]

    def __query(self, query_str: str) -> ConsoleRenderable:
        """Returns the result of a query as a text table"""

        try:
            result_relation = self.con.query(query_str)

            if result_relation is not None:
                lim_results = result_relation.limit(MAX_RESULT_ROWS)

                columns = lim_results.columns
                rows = lim_results.fetchall()

                table = Table(
                    header_style="green",
                    expand=True,
                    highlight=True,
                    show_lines=self.row_lines,
                )

                for col in columns:
                    table.add_column(col, style="magenta")

                for row in rows:
                    table.add_row(*map(self.__format_cell, row))

                return Styled(table, "")
        except (AttributeError, RuntimeError) as query_error:
            err_msg = QueryError(str(query_error))
            return err_msg


class DatabaseController(MessagePump):
    def __init__(self, name=None, load_clipboard=False, row_lines=False):
        class_name = self.__class__.__name__
        self.name = name or f"{class_name}"
        super().__init__()
        self.database = DatabaseProcess(load_clipboard, row_lines)
        self.database.daemon = True
        self.database.start()

        if load_clipboard:
            self.post_message_no_wait(Query(self, "select * from clipboard"))

    async def handle_query(self, message: Query) -> None:
        self.log(message.query)
        self.database.query_queue.put(message.query)
        loop = asyncio.get_event_loop()
        result, views = await loop.run_in_executor(None, self.database.result_queue.get)
        await self.emit(DatabaseViewsUpdate(self, views))
        await self.emit(QueryResult(self, result))

    async def handle_open_file(self, message: OpenFile) -> None:
        self.database.query_queue.put(FileObj(message.filename))

        loop = asyncio.get_event_loop()
        load_response, views = await loop.run_in_executor(
            None, self.database.result_queue.get
        )

        if isinstance(load_response, QueryError):
            await self.emit(QueryResult(self, load_response))
        else:
            view_name = load_response
            query = f'select * from "{view_name}"'
            await self.emit(UpdateTextInput(self, query))
            await self.post_message(Query(self, query))

        if views:
            await self.emit(DatabaseViewsUpdate(self, views))
