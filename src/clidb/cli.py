import argparse
import os
import platform
from typing import Any, List, Optional

from rich.text import Text
from textual.app import App
from textual.views import DockView
from textual.widgets import FileClick, ScrollView

from clidb import __version__
from clidb.database import DatabaseController
from clidb.events import (
    DatabaseViewsUpdate,
    OpenFile,
    Query,
    QueryResult,
    UpdateTextInput,
    ViewClick,
)
from clidb.views import DatabaseView, DataFileTree, QueryInput, ResultsView


def run(argv: Optional[List[str]] = None) -> None:
    """Entrypoint for clidb, parses arguments and initiates the app"""
    parser = argparse.ArgumentParser(description="cli sql client for local data files.")
    parser.add_argument(
        "path", nargs="?", default=os.getcwd(), help="path to display", type=str
    )
    parser.add_argument(
        "--clipboard",
        action="store_true",
        help="Display clipboard contents as view.",
    )
    parser.add_argument(
        "--row-lines", action="store_true", help="Render row separator lines."
    )

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=_get_version(),
        help="Display version information.",
    )

    parser.add_argument(
        "--log",
        "-l",
        type=str,
        default=None,
        help="Log file for debug messages.",
    )

    args = parser.parse_args(argv)
    CliDB.run(**vars(args))


class CliDB(App):
    """CLI interface for a local db sql client"""

    def __init__(self, **kwargs: Any) -> None:
        """
        log: log file to write debug messages to
        clipboard: flag to enable creating a view of clipboard contents
        row_lines: flag to print lines between rows
        """
        self.clipboard = kwargs.pop("clipboard")
        self.row_lines = kwargs.pop("row_lines")
        self.path = kwargs.pop("path")
        super().__init__(title="clidb", **kwargs)

    async def on_load(self) -> None:
        """Sent before going in to application mode."""
        await self.bind("q", "quit", "Quit")
        await self.bind("enter", "query", "Query")

        self.database = DatabaseController(
            load_clipboard=self.clipboard, row_lines=self.row_lines
        )
        self.register(self.database, self)

    async def on_mount(self) -> None:
        """Build the clidb interface"""
        self.query_view = QueryInput(
            name="QueryView",
            placeholder="select *",
            title="SQL Query",
            syntax="sql",
        )

        self.results_view = ResultsView(auto_width=True, name="ResultsView")
        self.database_view = DatabaseView("Views", name="DatabaseView")

        if os.path.isdir(self.path) or self.path.startswith("s3://"):
            view_dir = self.path
        else:
            view_dir = os.path.dirname(self.path) or os.getcwd()
            await self.database.post_message(OpenFile(self, self.path))

        self.directory_view = DataFileTree(str(view_dir), "DirTree")
        self.sidebar = DockView(name="Sidebar")

        await self.view.dock(self.query_view, edge="top", size=3)
        await self.view.dock(
            self.sidebar,
            edge="left",
            size=40,
        )

        await self.sidebar.dock(
            ScrollView(self.directory_view), ScrollView(self.database_view), edge="top"
        )

        # TODO: fetch the initial list properly
        await self.database_view.root.add("schemas", "schemas")
        await self.database_view.root.expand()

        self.database_view.style = "green"

        await self.view.dock(self.results_view, edge="top")

    async def handle_view_click(self, message: ViewClick) -> None:
        """Catch view clicks from the Database view and run query on view"""
        query = f'select * from "{message.view_name}"'
        await self.query_view.post_message(UpdateTextInput(self, query))
        await self.database.post_message(Query(self, query))

    async def handle_update_text_input(self, message: UpdateTextInput) -> None:
        """Catch a request to update the query input and pass to the input"""
        await self.query_view.post_message(message)

    async def handle_query_result(self, message: QueryResult) -> None:
        """Handle a result table from a query"""
        await self.results_view.post_message(message)

    async def handle_database_views_update(self, message: DatabaseViewsUpdate) -> None:
        """Handle an updated list of views"""
        await self.database_view.post_message(message)

    async def action_query(self) -> None:
        """Submit a query and render result's contents"""
        await self.database.post_message(Query(self, self.query_view.value))

    async def handle_file_click(self, message: FileClick) -> None:
        """Handle a file click event, rendering a loading message pending a reesult"""
        await self.results_view.post_message(
            QueryResult(self, Text("Loading...", justify="center"))
        )
        self.log("Opening", message.path)
        await self.database.post_message(OpenFile(self, message.path))


def _get_version() -> str:
    return f"""
    clidb {__version__} [Python {platform.python_version()}]
    Copyright 2022 Danny Boland
    """


if __name__ == "__main__":
    run(["--log", "textual.log"])
