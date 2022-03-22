import os
import sys

from rich.text import Text
from textual.app import App
from textual.views import DockView
from textual.widgets import FileClick, ScrollView
from textual_inputs import TextInput

from clidb.database import DatabaseAdapter, DatabaseView, DataFileTree, ViewClick


class CliDB(App):
    """CLI interface for a local db sql client"""

    async def on_load(self) -> None:
        """Sent before going in to application mode."""
        await self.bind("q", "quit", "Quit")
        await self.bind("enter", "query", "Query")

        try:
            self.path = sys.argv[1]
            if os.path.isdir(self.path):
                self.dir = self.path
            else:
                self.dir = os.path.dirname(self.path) or os.getcwd()
        except IndexError:
            self.path = ""
            self.dir = os.getcwd()

        self.database = DatabaseAdapter()

    async def on_mount(self) -> None:
        """Build the clidb interface"""
        self.query_view = TextInput(
            name="query",
            placeholder="select *",
            title="SQL Query",
            syntax="sql",
        )

        self.results_view = ScrollView(auto_width=True)
        self.database_view = DatabaseView("Views", data=self.database)

        self.directory_view = DataFileTree(self.dir, "DirTree")
        self.sidebar = DockView()

        await self.view.dock(self.query_view, edge="top", size=3)
        await self.view.dock(
            self.sidebar,
            edge="left",
            size=40,
        )

        await self.sidebar.dock(
            ScrollView(self.directory_view), ScrollView(self.database_view), edge="top"
        )
        await self.database_view.refresh_views()

        self.database_view.style = "green"

        await self.view.dock(self.results_view, edge="top")
        if self.path:
            await self.call_later(self.read_file, self.path)

    async def handle_view_click(self, message: ViewClick) -> None:
        """Catch view clicks from the Database view and run query on view"""
        await self.update_query(f'select * from "{message.view_name}"')
        await self.action_query()

    async def update_query(self, query_text: str) -> None:
        """Hacky override of TextInput value as not directly supported yet"""
        self.query_view.value = query_text
        await self.query_view.emit(
            self.query_view._on_change_message_class(self.query_view)
        )

    async def action_query(self) -> None:
        """Render a file or query result's contents"""
        result = self.database.query(self.query_view.value)
        await self.database_view.refresh_views()

        if result:
            await self.results_view.update(result)

    async def read_file(self, filename: str) -> None:
        """Render a data file's contents"""
        try:
            view_name = self.database.load_file_as_view(filename)
        except ImportError as import_error:
            await self.results_view.update(
                Text(f"Please install clidb[{import_error.msg}]", justify="center")
            )
        except ValueError:
            return

        await self.update_query(f'select * from "{view_name}"')
        await self.action_query()

    async def handle_file_click(self, message: FileClick) -> None:
        """A message sent by the directory tree when a file is clicked."""

        await self.read_file(message.path)


if __name__ == "__main__":
    CliDB.run()  # pylint: disable=no-value-for-parameter
