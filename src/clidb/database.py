"""Manage the database backend as a separate process with a controller"""

import asyncio
import multiprocessing as mp
import os
from dataclasses import dataclass
from functools import partial
from multiprocessing import Process, Queue
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

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

pd = lazy_import("pandas")
boto3 = lazy_import("boto3")


MAX_RESULT_ROWS = 100
MAX_CELL_LENGTH = 100

if TYPE_CHECKING:
    from multiprocessing import _QueueType

    # Our results queue is always a tuple of a renderable result and a list of views
    ResultQueueType = _QueueType[Tuple[Union[ConsoleRenderable, str, None], List[str]]]


@dataclass
class FileObj:
    """An object containing a filename to be passed to the database process"""

    filename: str


class QueryError(Text):
    """A query result that reflects an error status"""

    def __init__(self, error: str) -> None:
        super().__init__(error, justify="center")


class DatabaseProcess(Process):
    """Separate process as thin wrapper around duckdb"""

    end_queue_sentinel = object()

    def __init__(self, read_clipboard: bool = False, row_lines: bool = False) -> None:
        super().__init__()
        self.query_queue: "Queue[Any]" = Queue()

        self.result_queue: ResultQueueType = Queue()
        self.read_clipboard = read_clipboard
        self.row_lines = row_lines

        self.con: duckdb.DuckDBPyConnection

    def run(self) -> None:
        self.con = duckdb.connect(database=":memory:")
        self.con.view("duckdb_views").create_view("schemas")

        if self.read_clipboard and pd is not None:
            self.__load_clipboard_as_view()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__await_query())

    async def __await_query(self) -> None:
        while True:
            result: Optional[Union[ConsoleRenderable, str]] = None
            query_obj = self.query_queue.get()
            if query_obj == self.end_queue_sentinel:
                break

            if query_obj is None:
                result = ""
            elif isinstance(query_obj, str):
                result = self.__query(query_obj)
            elif isinstance(query_obj, FileObj):
                try:
                    result = self.__load_file_as_view(query_obj.filename)
                except (FileNotFoundError, KeyError, RuntimeError):
                    result = QueryError(f"Failed to load {query_obj.filename}.")
                except ImportError:
                    result = QueryError("Please install clidb[extras].")

            views = self.__get_views()
            self.result_queue.put((result, views))

    def __get_views(self) -> List[str]:
        """Returns a list of defined view names"""
        return [schema[2] for schema in self.con.view("schemas").fetchall()]

    def __load_file_with_pandas(
        self, filename: str, file_type: str, view_name: str
    ) -> None:
        if not pd:
            raise ImportError
        dispatch_dict = {
            ".parquet": pd.read_parquet,
            ".json": pd.read_json,
            ".jsonl": partial(pd.read_json, lines=True),
            ".xls": pd.read_excel,
            ".xlsx": pd.read_excel,
        }
        load_method = dispatch_dict[file_type]
        self.con.register(view_name, load_method(filename))

    def __load_file_as_view(self, filename: str) -> str:
        """Creates a view for a data file and returns the view's name"""
        file_path, file_type = os.path.splitext(filename)
        view_name = os.path.basename(file_path)

        if file_type == ".gz" and view_name.endswith(".parquet"):
            file_type = ".parquet"
            view_name = view_name[: -len(file_type)]

        if file_type == ".csv":
            self.con.from_csv_auto(filename).create_view(view_name)
        elif file_type == ".parquet" and not filename.startswith("s3://"):
            self.con.from_parquet(filename).create_view(view_name)
        else:
            self.__load_file_with_pandas(filename, file_type, view_name)

        return view_name

    def __load_clipboard_as_view(self) -> None:
        """Creates a view of data from the clipboard"""
        view_name = "clipboard"
        if pd is not None:
            self.con.register(view_name, pd.read_clipboard())
        else:
            raise ImportError

    @staticmethod
    def __format_cell(value: Any) -> str:
        return str(value)[:MAX_CELL_LENGTH]

    def __query(self, query_str: str) -> Optional[ConsoleRenderable]:
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
            return None
        except (AttributeError, RuntimeError) as query_error:
            err_msg = QueryError(str(query_error))
            return err_msg


class DatabaseController(MessagePump):
    """Class to manage a database process and its queues"""

    def __init__(
        self,
        name: Optional[str] = None,
        load_clipboard: bool = False,
        row_lines: bool = False,
    ):
        self.name = name or self.__class__.__name__
        super().__init__()
        mp.set_start_method("spawn")
        self.database = DatabaseProcess(load_clipboard, row_lines)
        self.database.daemon = True
        self.database.start()

        if load_clipboard:
            self.post_message_no_wait(Query(self, "select * from clipboard"))

    async def handle_query(self, message: Query) -> None:
        """Handle a query request, placing the query string in the queue
        and emitting the results"""
        self.log(message.query)
        self.database.query_queue.put(message.query)
        loop = asyncio.get_event_loop()
        result, views = await loop.run_in_executor(None, self.database.result_queue.get)
        await self.emit(DatabaseViewsUpdate(self, views))
        if result:
            await self.emit(QueryResult(self, result))

    async def handle_open_file(self, message: OpenFile) -> None:
        """Handle an OpenFile message,"""
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
