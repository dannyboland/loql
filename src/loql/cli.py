import argparse
import os
import platform
from pathlib import Path
from typing import List, Optional

from loql import __version__, config
from loql.app import LoQL


def run(argv: Optional[List[str]] = None) -> None:
    """Entrypoint for LoQL, parses arguments and initiates the app"""
    parser = argparse.ArgumentParser(description="cli sql client for local data files.")
    parser.add_argument(
        "path", nargs="?", default=os.getcwd(), help="path to display", type=str
    )
    parser.add_argument(
        "--clipboard",
        action="store_true",
        help="Display clipboard contents as view.",
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=1000,
        help="Maximum number of rows to display.",
    )

    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=_get_version(),
        help="Display version information.",
    )

    args = parser.parse_args(argv)
    if args.path:
        args.path = Path(args.path)
        if not args.path.exists():
            parser.error(f"Path {args.path} does not exist.")

    config.update(vars(args))

    app = LoQL()
    app.run()


def _get_version() -> str:
    return f"""
    LoQL {__version__} [Python {platform.python_version()}]
    Copyright 2024 Danny Boland
    """


if __name__ == "__main__":
    run()
