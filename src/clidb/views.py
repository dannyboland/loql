import os
from typing import Optional
from urllib.parse import urlparse

from textual.widgets import DirectoryTree, ScrollView, TreeClick, TreeControl, TreeNode
from textual.widgets._directory_tree import DirEntry
from textual_inputs import TextInput

from clidb import lazy_import
from clidb.database import QueryError
from clidb.events import DatabaseViewsUpdate, QueryResult, UpdateTextInput, ViewClick

try:
    boto3 = lazy_import("boto3")
    _has_boto = True
except ImportError:
    _has_boto = False


class DataFileTree(DirectoryTree):
    """A DirectoryTree that filters supported filetypes and also handles S3"""

    LOCAL_FILETYPES = [".csv", ".parquet", ".gz", ".json", ".jsonl", ".xls", ".xlsx"]
    S3_FILETYPES = [".parquet", ".gz"]
    """A view for navigating relevant data files"""

    @staticmethod
    async def get_child_node_with_path(
        node: TreeNode[DirEntry], path: str
    ) -> Optional[TreeNode[DirEntry]]:
        """Returns the first child node of a node with a given path"""
        for child in node.children:
            if child.data.path == path:
                return child
        # No matching children
        return None

    async def load_local_directory(self, node: TreeNode[DirEntry]):
        """Add entries for contents of a local directory"""
        path = node.data.path

        directory = sorted(
            list(os.scandir(path)), key=lambda entry: (not entry.is_dir(), entry.name)
        )
        for entry in directory:
            if (
                entry.is_dir()
                or os.path.splitext(entry.path)[1] in self.LOCAL_FILETYPES
            ):
                await node.add(entry.name, DirEntry(entry.path, entry.is_dir()))
        node.loaded = True

    async def preload_s3_directory(self, node: TreeNode[DirEntry]):
        """Pre-populate all entries for contents of an s3 path"""
        if not _has_boto:
            raise ImportError("boto3")

        s3_url = urlparse(node.data.path)
        bucket = s3_url.netloc
        key = s3_url.path.lstrip("/")
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key)

        for path in response["Contents"]:
            path_parts = path["Key"].split("/")
            path_dirs = path_parts[:-1]
            filename = path_parts[-1]

            current_node = node
            for s3_dir in path_dirs:
                new_path = current_node.data.path + s3_dir + "/"
                if not await self.get_child_node_with_path(current_node, new_path):
                    await current_node.add(s3_dir, DirEntry(new_path, True))
                current_node = await self.get_child_node_with_path(
                    current_node, new_path
                )
                current_node.loaded = True
            if os.path.splitext(filename)[1] in self.S3_FILETYPES:
                new_path = current_node.data.path + filename
                await current_node.add(filename, DirEntry(new_path, False))

    async def load_directory(self, node: TreeNode[DirEntry]):
        if node.data.path.startswith("s3://") and not node.loaded:
            if _has_boto:
                await self.preload_s3_directory(self.root)
            else:
                await self.app.post_message(
                    QueryResult(self, QueryError("Please install clidb[extras]."))
                )
        else:
            await self.load_local_directory(node)
        await node.expand()


class QueryInput(TextInput):
    """A TextInput that handles UpdateTextInput events"""

    async def handle_update_text_input(self, message: UpdateTextInput):
        """Handle a request to update the text input value"""
        self.value = message.text
        await self.emit(self._on_change_message_class(self))


class ResultsView(ScrollView):
    """A ScrollView that handles QueryResult events"""

    async def handle_query_result(self, message: QueryResult):
        """Render a query result in response to a QueryResult event"""
        if message.result:
            await self.update(message.result)


class DatabaseView(TreeControl):
    """Rendered list of loaded data views"""

    def __init__(self, label, name=None, data="root"):
        super().__init__(label=label, data=data, name=name)

    async def handle_database_views_update(self, message: DatabaseViewsUpdate) -> None:
        """Update the list of views in response to an update event"""
        await self.root.expand()
        current_views = [node.data for node in self.nodes.values()]
        new_views = set(message.views) - set(current_views)

        # TODO: remove retired views for list of views
        retired_views = set(current_views) - set(message.views)
        retired_views.remove(self.data)

        for view in new_views:
            await self.root.add(view, view)
            self._update_size(self.size + (0, 1))

    async def handle_tree_click(self, message: TreeClick) -> None:
        """Emit a ViewClick event if a view is clicked"""
        if message.node.parent == self.root:
            view_name = message.node.data
            await self.emit(ViewClick(self, view_name))
