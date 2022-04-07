from typing import List, Union

from rich.console import ConsoleRenderable
from rich.text import Text
from textual._types import MessageTarget
from textual.message import Message


class ViewClick(Message):
    """View click event containing view name"""

    def __init__(self, sender: MessageTarget, view_name: str) -> None:
        self.view_name = view_name
        super().__init__(sender)


class UpdateTextInput(Message, bubble=False):
    """Update TextInput value event"""

    def __init__(self, sender: MessageTarget, text: str) -> None:
        self.text = text
        super().__init__(sender)


class Query(Message):
    """Query event containing query string"""

    def __init__(self, sender: MessageTarget, query: str) -> None:
        self.query = query
        super().__init__(sender)


class QueryResult(Message, bubble=False):
    """Query result event containing results table"""

    def __init__(
        self, sender: MessageTarget, result: Union[ConsoleRenderable, str]
    ) -> None:
        self.result: ConsoleRenderable
        if isinstance(result, str):
            self.result = Text(result, justify="center")
        else:
            self.result = result
        super().__init__(sender)


class DatabaseViewsUpdate(Message, bubble=False):
    """Views updated event following a database action"""

    def __init__(self, sender: MessageTarget, views: List[str]) -> None:
        self.views = views
        super().__init__(sender)


class OpenFile(Message):
    """Open file event containing filename string"""

    def __init__(self, sender: MessageTarget, filename: str) -> None:
        self.filename = filename
        super().__init__(sender)
