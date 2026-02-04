import os
import typing as T
import uuid

PathLike = T.Union[str, os.PathLike]


def fspath(path: PathLike) -> str:
    path = os.fspath(path)  # pyre-ignore[6]
    if isinstance(path, bytes):
        path = path.decode()
    return path


def split_uri(uri: PathLike) -> T.Tuple[str, str, T.Optional[str]]:
    """split uri to three parts.

    :param uri: The URI to split.
    :type uri: PathLike
    :return: protocol, path, profile_name
    :rtype: T.Tuple[str, str, T.Optional[str]]
    """
    uri = fspath(uri)

    if "://" in uri:
        protocol, path = uri.split("://", 1)
    else:
        protocol = "file"
        path = uri
    if "+" in protocol:
        protocol, profile_name = protocol.split("+", 1)
    else:
        profile_name = None
    return protocol, path, profile_name


def generate_cache_path(filename: str, cache_dir: str = "/tmp") -> str:
    suffix = os.path.splitext(filename)[1]
    return os.path.join(cache_dir, str(uuid.uuid4()) + suffix)
