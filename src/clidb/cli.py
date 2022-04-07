import argparse
import os
from logging import getLogger

from rich.text import Text
from textual.app import App
from textual.views import DockView
from textual.widgets import FileClick, ScrollView

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

logger = getLogger()


class CliDB(App):
    """CLI interface for a local db sql client"""

    async def on_load(self) -> None:
        """Sent before going in to application mode."""
        await self.bind("q", "quit", "Quit")
        await self.bind("enter", "query", "Query")

        parser = argparse.ArgumentParser(
            description="cli sql client for local data files."
        )
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
        args = parser.parse_args()

        self.path = args.path

        self.database = DatabaseController(
            load_clipboard=args.clipboard, row_lines=args.row_lines
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
        await self.database_view.post_message(message)

    async def action_query(self) -> None:
        """Submit a query and render result's contents"""
        await self.database.post_message(Query(self, self.query_view.value))

    async def handle_file_click(self, message: FileClick) -> None:
        await self.results_view.post_message(
            QueryResult(self, Text("Loading...", justify="center"))
        )
        self.log("Opening", message.path)
        await self.database.post_message(OpenFile(self, message.path))


if __name__ == "__main__":
    CliDB.run(
        title="clidb", log="textual.log"
    )  # pylint: disable=no-value-for-parameter
