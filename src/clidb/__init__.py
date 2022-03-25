import importlib.util
import sys


def lazy_import(name):
    """Delay importing heavy libraries until we need them"""
    spec = importlib.util.find_spec(name)
    if not spec:
        raise ImportError

    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
