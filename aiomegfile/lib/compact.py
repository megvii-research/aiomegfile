import os

from aiomegfile.pathlike import PathLike


def fspath(path: PathLike) -> str:
    result = os.fspath(path)
    if isinstance(result, bytes):
        return result.decode()
    return result
