import os
import typing as T


def fspath(path: T.Union[str, os.PathLike]) -> str:
    path = os.fspath(path)
    if isinstance(path, bytes):
        path = path.decode()
    return path
