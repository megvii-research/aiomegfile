from aiomegfile.__version__ import __version__  # noqa: F401
from aiomegfile.filesystem import (
    LocalFileSystem,
)
from aiomegfile.smart_path import SmartPath

__all__ = [
    "SmartPath",
    "LocalFileSystem",
]
