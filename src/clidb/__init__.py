import importlib.util
import sys
from importlib import metadata
from importlib.abc import Loader
from types import ModuleType

__version__ = metadata.version(__name__)


def lazy_import(name: str) -> ModuleType:
    """Delay importing heavy libraries until we need them"""
    spec = importlib.util.find_spec(name)
    if not spec or not spec.loader:
        raise ImportError

    assert isinstance(spec.loader, Loader)
    loader = importlib.util.LazyLoader(spec.loader)

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
