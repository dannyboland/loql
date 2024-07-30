import csv
from collections import namedtuple
from functools import partial
from pathlib import Path
from typing import Any, List

import duckdb
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, TextArea, Tree

from loql import config, lazy_import
from loql.views import OpenFileModal

pd = lazy_import("pandas")


MAX_RESULT_ROWS = 1000
View = namedtuple("View", ["name", "type"])
Column = namedtuple("Column", ["name", "type"])


class LoQL(App[None]):
    "A Textual app for querying data files"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        Binding("ctrl+m", "toggle_dark", "Toggle dark mode"),
        Binding("ctrl+o", "open_file", "Open file"),
        Binding("ctrl+r", "execute_query", "Execute query"),
        Binding("ctrl+s", "write_results", "Write results"),
        Binding("ctrl+q", "quit", "Quit"),
        Binding(key="ctrl+c", action="clear", description="Clear", show=False),
    ]

    CSS_PATH = "style.tcss"

    def on_mount(self) -> None:
        """Mount the app"""
        self.set_up_database()
        table = self.query_one(DataTable)
        table.add_columns("Commands", "Keys")
        table.add_rows(
            [
                (
                    "Open file",
                    "^o",
                ),
                (
                    "Execute query",
                    "^r",
                ),
                (
                    "Write results",
                    "^s",
                ),
                (
                    "Toggle dark mode",
                    "^m",
                ),
                (
                    "Quit",
                    "^q",
                ),
            ]
        )

    def set_up_database(self) -> None:
        """Open duckdb connection"""
        self.con = duckdb.connect(database=":memory:")
        self.con.query(
            """select view_name as name, 'view' as type
             from duckdb_views union select table_name, 'table' from duckdb_tables"""
        ).create_view("schemas")

        if config.clipboard:
            self.load_clipboard()

        if config.path.is_file():
            self.open_file(config.path)

    def compose(self) -> ComposeResult:
        """Create child widgets"""
        tree: Tree[View] = Tree("Data Sources", id="data_sources")
        tree.root.expand()

        yield Horizontal(
            Vertical(tree, Tree("Schema", id="schema"), classes="nav-column"),
            Vertical(
                TextArea.code_editor(
                    "select * from ...",
                    language="sql",
                    id="sql_input",
                    theme=self.code_theme,
                ),
                DataTable(id="data_table"),
                classes="data-column",
            ),
        )

        yield Footer()

    @property
    def code_theme(self) -> str:
        return "vscode_dark" if self.dark else "github_light"

    @property
    def data_table(self) -> DataTable[Any]:
        """Get the data table"""
        return self.query_one("DataTable", DataTable)

    # --- UI Actions ---

    def action_toggle_dark(self) -> None:
        """Toggle dark mode"""
        self.dark = not self.dark
        self.query_one("#sql_input", TextArea).theme = self.code_theme

    def action_clear(self) -> None:
        """Clear query input if selected and cancel any work"""
        sql_input = self.query_one("#sql_input", TextArea)
        if sql_input.has_focus:
            sql_input.clear()

        self.workers.cancel_all()
        self.data_table.loading = False

    def action_open_file(self) -> None:
        """Open a file"""
        self.push_screen(OpenFileModal(), self.open_file)

    def action_execute_query(self, save: bool = False) -> None:
        """Execute a query"""
        query_box = self.query_one("#sql_input", TextArea)
        query = (
            query_box.selected_text
            if (query_box.selected_text != "")
            else query_box.text
        )

        self.data_table.loading = True
        self.execute_query(query, save)

    def action_write_results(self) -> None:
        """Write results to a file"""
        self.action_execute_query(save=True)

    def on_tree_node_selected(self, event: Tree.NodeSelected[View]) -> None:
        """Handle tree node selection events"""
        self.load_metadata(event.node.data)

    # --- UI Update methods ---

    def update_results(self, columns: List[str], rows: List[Any]) -> None:
        """Update a table"""
        table = self.data_table
        table.clear(columns=True)
        table.add_columns(*columns)
        table.add_rows(rows)
        table.loading = False
        if len(rows) == config.max_rows:
            self.notify(f"Results limited to {config.max_rows} rows.", title="Warning")

    def update_metadata(self, views: List[View], columns: List[Column] | None) -> None:
        """Update the data sources"""
        tree = self.query_one("#data_sources", Tree)
        tree.clear()
        for view in views:
            if view.name != "schemas":
                tree.root.add_leaf(f"{view.name} ({view.type})", data=view.name)
        tree.root.expand()

        if columns is not None:
            schema = self.query_one("#schema", Tree)
            schema.clear()
            for col in columns:
                schema.root.add_leaf(f"{col.name} ({col.type})", data=col.name)
            schema.root.expand()

    def notify_complete(self, message: str, title: str) -> None:
        """Notify the user"""
        self.data_table.loading = False
        self.notify(message, title=title)

    # --- Data handling methods ---

    @work(exclusive=True, thread=True)
    async def load_clipboard(self) -> None:
        """Load the clipboard"""
        self.con.register("clipboard", pd.read_clipboard())
        self.query_one("#data_sources", Tree).root.add_leaf(
            "clipboard", data="clipboard"
        )
        self.load_metadata("clipboard")

    @work(exclusive=True, thread=True)
    async def open_file(self, path: Path) -> None:
        """Open a file"""
        file_type = path.suffix
        view_name = path.stem.lower()

        if file_type == ".gz" and view_name.endswith(".parquet"):
            file_type = ".parquet"
            view_name = view_name[: -len(file_type)]

        if file_type == ".csv":
            self.con.from_csv_auto(str(path)).create_view(view_name)
        elif file_type == ".parquet" and not str(path).startswith("s3://"):
            self.con.from_parquet(str(path)).create_view(view_name)
        else:
            self.__load_file_with_pandas(str(path), file_type, view_name)

        self.load_metadata(view_name)

    @work(exclusive=True, thread=True)
    async def load_metadata(self, view: str | None = None) -> None:
        """Refresh the metadata for data sources and selected schema"""

        views = [View(view[0], view[1]) for view in self.con.view("schemas").fetchall()]
        if view is None:
            self.call_from_thread(self.update_metadata, views, None)
            return

        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{view}'
            ORDER BY ordinal_position;
        """

        column_tuples = self.con.query(query).fetchall()
        columns = [Column(column[0], column[1].lower()) for column in column_tuples]

        self.call_from_thread(self.update_metadata, views, columns)

    @work(exclusive=True, thread=True)
    async def execute_query(self, query: str, save: bool = False) -> None:
        """Execute a query"""
        try:
            result_relation = self.con.query(query)
            if result_relation is None:
                # This may have been a DDL statement
                self.con.view("schemas").fetchall()
                self.call_from_thread(self.load_metadata)
                self.call_from_thread(
                    self.notify_complete, "Statement successful", "Success"
                )
                return None

            if not save:
                result_relation = result_relation.limit(config.max_rows)

            columns = [col[0] for col in result_relation.description]
            rows = result_relation.fetchall()
        except (AttributeError, RuntimeError, ValueError, duckdb.Error) as query_error:
            self.call_from_thread(self.notify_complete, str(query_error), "Error")
            return

        if save:
            self.write_results(columns, rows)
        else:
            self.call_from_thread(self.update_results, columns, rows)

    @work(exclusive=True, thread=True)
    def write_results(self, columns: List[str], rows: List[Any]) -> None:
        """Write results to a file"""
        with open("results.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)

        self.call_from_thread(
            self.notify_complete, "Results written to results.csv", "Success"
        )

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
