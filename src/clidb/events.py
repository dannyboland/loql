from typing import List

from rich.console import ConsoleRenderable
from textual._types import MessageTarget
from textual.message import Message


class ViewClick(Message):
    """View click event containing view name"""

    def __init__(self, sender: MessageTarget, view_name: str) -> None:
        self.view_name = view_name
        super().__init__(sender)


class UpdateTextInput(Message, bubble=False):  # type: ignore
    """Update TextInput value event"""

    def __init__(self, sender: MessageTarget, text: str) -> None:
        self.text = text
        super().__init__(sender)


class Query(Message):
    """Query event containing query string"""

    def __init__(self, sender: MessageTarget, query: str) -> None:
        self.query = query
        super().__init__(sender)


class QueryResult(Message, bubble=False):  # type: ignore
    """Query result event containing results table"""

    def __init__(self, sender: MessageTarget, result: ConsoleRenderable) -> None:
        self.result = result
        super().__init__(sender)


class DatabaseViewsUpdate(Message, bubble=False):  # type: ignore
    """Views updated event following a database action"""

    def __init__(self, sender: MessageTarget, views: List[str]) -> None:
        self.views = views
        super().__init__(sender)


class OpenFile(Message):
    """Open file event containing filename string"""

    def __init__(self, sender: MessageTarget, filename: str) -> None:
        self.filename = filename
        super().__init__(sender)
