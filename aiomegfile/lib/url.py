import os
import typing as T


def fspath(path: T.Union[str, os.PathLike]) -> str:
    path = os.fspath(path)
    if isinstance(path, bytes):
        path = path.decode()
    return path


def split_uri(uri: T.Union[str, os.PathLike]) -> T.Tuple[str, str, T.Optional[str]]:
    """split uri to three parts.

    :param uri: The URI to split.
    :type uri: T.Union[str, os.PathLike]
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
