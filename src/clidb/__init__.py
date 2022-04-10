"""clidb is a command line sql client for individual data files."""

import importlib.util
import sys
from importlib import metadata
from importlib.abc import Loader
from types import ModuleType
from typing import Optional

__version__ = metadata.version(__name__)


def lazy_import(name: str) -> Optional[ModuleType]:
    """Delay importing heavy libraries until we need them
    Args
        name of module to lazy import
    Returns
        boolean flag whether module is available
        the module as if loaded"""
    spec = importlib.util.find_spec(name)
    if not spec or not spec.loader:
        return None

    assert isinstance(spec.loader, Loader)
    loader = importlib.util.LazyLoader(spec.loader)

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
