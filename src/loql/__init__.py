"""LoQL is a terminal based sql client for local data files."""

import importlib.util
import sys
from importlib import metadata
from importlib.abc import Loader
from pathlib import Path
from types import ModuleType
from typing import Any, Dict

__all__ = ["config"]


class Config:
    clipboard: bool = False
    path: Path = Path.cwd()
    max_rows: int = 1000

    def update(self, kwargs: Dict[str, Any]) -> None:
        config_keys = set(self.__annotations__.keys())
        for key in config_keys.intersection(kwargs.keys()):
            setattr(self, key, kwargs[key])


__version__ = metadata.version(__name__)
config = Config()


def lazy_import(name: str) -> ModuleType:
    """Delay importing heavy libraries until we need them
    Args
        name of module to lazy import
    Returns
        boolean flag whether module is available
        the module as if loaded"""
    spec = importlib.util.find_spec(name)
    if not spec or not spec.loader:
        raise ImportError(f"Module {name} not found")

    assert isinstance(spec.loader, Loader)
    loader = importlib.util.LazyLoader(spec.loader)

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
