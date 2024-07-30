from pathlib import Path
from typing import Iterable

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import DirectoryTree, Input, Label

from loql import config


class DataFileTree(DirectoryTree):
    """A DirectoryTree that filters supported filetypes"""

    LOCAL_FILETYPES = [".csv", ".parquet", ".gz", ".json", ".jsonl", ".xls", ".xlsx"]

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [
            path
            for path in paths
            if self._safe_is_dir(path) or path.suffix in self.LOCAL_FILETYPES
        ]


class OpenFileModal(ModalScreen[Path]):
    """A modal screen for opening a file"""

    BINDINGS = [
        Binding("ctrl+c", "clear", "Clear"),
        Binding("escape", "dismiss", "Cancel"),
    ]

    def compose(self) -> ComposeResult:
        yield Label("Select a file:", id="question")
        yield Input(id="file_path", value=str(config.path))
        yield DataFileTree(id="file_tree", path=config.path)

    @on(DataFileTree.FileSelected)
    def on_file_selected(self, event: DataFileTree.FileSelected) -> None:
        if not event.node.data:
            return

        path = event.node.data.path
        if path.is_file():
            self.dismiss(path)

    @on(Input.Submitted)
    def on_path_submitted(self, event: Input.Submitted) -> None:
        path = Path(self.query_one("#file_path", Input).value)
        if path.is_file():
            self.dismiss(path)
        elif path.is_dir():
            self.query_one("#file_tree", DataFileTree).path = path

    def action_clear(self) -> None:
        """Clear query input if selected and cancel any work"""
        file_input: Input = self.query_one("#file_path", Input)
        if file_input.has_focus:
            file_input.clear()
