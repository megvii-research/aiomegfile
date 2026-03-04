import typing as T

from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.path import PathLike, fspath, split_uri

__all__ = [
    "is_stdio",
    "stdio_open",
]


def is_stdio(path: PathLike) -> bool:
    """Return whether the path is a ``stdio://`` URI.

    :param path: Path to test.
    :return: True if path uses stdio protocol, otherwise False.
    :rtype: bool
    """
    path_str = fspath(path)
    if not isinstance(path_str, str) or not path_str.startswith("stdio://"):
        return False
    protocol, _, _ = split_uri(path_str)
    return protocol == "stdio"


def stdio_open(
    path: PathLike,
    mode: str = "rb",
    *,
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    **kwargs: T.Any,
) -> T.AsyncContextManager[T.Any]:
    """Open stdio stream with async context manager.

    :param path: Stdio URI path.
    :param mode: Open mode.
    :param encoding: Text encoding for text modes.
    :param errors: Error handling strategy for encoding/decoding.
    :param kwargs: Extra options for compatibility.
    :raises ValueError: If ``path`` is not a stdio URI.
    :return: Async context manager for stdio stream.
    :rtype: T.AsyncContextManager[T.Any]
    """
    if not is_stdio(path):
        raise ValueError("unacceptable path: %r" % fspath(path))
    return SmartPath(path).open(
        mode=mode,
        encoding=encoding,
        errors=errors,
        **kwargs,
    )
