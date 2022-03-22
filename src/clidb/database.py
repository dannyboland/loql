import os
from os import scandir
from typing import List

import duckdb
from rich.console import ConsoleRenderable
from rich.table import Table
from rich.text import Text
from textual._types import MessageTarget
from textual.message import Message
from textual.widgets import DirectoryTree, TreeClick, TreeControl, TreeNode
from textual.widgets._directory_tree import DirEntry

try:
    import pandas as pd
except ImportError:
    _has_pd = False
else:
    _has_pd = True

MAX_RESULT_LEN = 1024
FILETYPES = [".csv", ".parquet", ".gz", ".json", ".jsonl"]


class DataFileTree(DirectoryTree):
    """A view for navigating relevant data files"""

    async def load_directory(self, node: TreeNode[DirEntry]):
        path = node.data.path
        directory = sorted(
            list(scandir(path)), key=lambda entry: (not entry.is_dir(), entry.name)
        )
        for entry in directory:
            if entry.is_dir() or os.path.splitext(entry.path)[1] in FILETYPES:
                await node.add(entry.name, DirEntry(entry.path, entry.is_dir()))
        node.loaded = True
        await node.expand()
        self.refresh(layout=True)


class DatabaseView(TreeControl):
    """Rendered list of loaded data views"""

    async def add_view(self, view_name) -> None:
        """Add a view to the list of views"""
        if view_name not in [node.data for node in self.nodes.values()]:
            await self.root.add(view_name, view_name)

    async def refresh_views(self) -> None:
        """Update view list with views defined in db schema"""
        await self.root.expand()
        for view_name in self.data.get_views():
            await self.add_view(view_name)

    async def handle_tree_click(self, message: TreeClick) -> None:
        """Emit a ViewClick event if a view is clicked"""
        if message.node.parent == self.root:
            view_name = message.node.data
            await self.emit(ViewClick(self, view_name))


class ViewClick(Message):
    """View click event containing view name"""

    def __init__(self, sender: MessageTarget, view_name: str) -> None:
        self.view_name = view_name
        super().__init__(sender)


class DatabaseAdapter:
    """Thin wrapper around duckdb"""

    def __init__(self) -> None:
        self.con = duckdb.connect(database=":memory:")
        self.con.view("duckdb_views").create_view("schemas")

    def get_views(self) -> List[str]:
        """Returns a list of defined view names"""
        return [schema[2] for schema in self.con.view("schemas").fetchall()]

    def load_file_as_view(self, filename: str) -> str:
        """Creates a view for a data file and returns the view's name"""
        file_path, file_extension = os.path.splitext(filename)
        view_name = os.path.basename(file_path)

        if file_extension == ".csv":
            self.con.from_csv_auto(filename).create_view(view_name)
        elif file_extension == ".parquet":
            self.con.from_parquet(filename).create_view(view_name)
        elif file_extension == ".gz" and view_name.endswith(".parquet"):
            view_name = view_name[: -len(".parquet")]
            self.con.from_parquet(filename).create_view(view_name)
        elif file_extension == ".json":
            if _has_pd:
                self.con.register(view_name, pd.read_json(filename))
            else:
                raise ImportError("pandas")
        elif file_extension == ".jsonl":
            if _has_pd:
                self.con.register(view_name, pd.read_json(filename, lines=True))
            else:
                raise ImportError("pandas")
        else:
            raise ValueError

        return view_name

    def query(self, query_str: str) -> ConsoleRenderable:
        """Returns the result of a query as a text table"""
        table = Table(header_style="green", expand=True, highlight=True)

        try:
            result_relation = self.con.query(query_str)

            if result_relation is not None:
                lim_results = result_relation.limit(MAX_RESULT_LEN)
                for col in lim_results.columns:
                    table.add_column(col, style="magenta")
                for row in lim_results.fetchall():
                    table.add_row(*map(str, row))

                return table
        except (AttributeError, RuntimeError) as query_error:
            return Text(str(query_error), justify="center")
