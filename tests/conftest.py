"""Shared fixtures and helpers for the test suite.

`scripts/` is not an importable package (no `__init__.py`, no packaging), so
tests that need to exercise functions from a script must load it directly
from its file path via `importlib`.
"""

import importlib.util
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / "data"


def load_script(name: str) -> ModuleType:
    """Import ``scripts/<name>.py`` as a module and return it.

    Example: ``load_script("parse_nave")`` imports
    ``REPO_ROOT / "scripts" / "parse_nave.py"``.
    """
    script_path = REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
