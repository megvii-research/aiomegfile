import os
import typing as T
import uuid

from aiomegfile.config import READER_BLOCK_SIZE

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


def generate_cache_path(filename: str, cache_dir: T.Optional[str] = None) -> str:
    if cache_dir is None:
        cache_dir = "/tmp"
    suffix = os.path.splitext(filename)[1]
    return os.path.join(cache_dir, str(uuid.uuid4()) + suffix)


async def copyfileobj(
    fsrc,
    fdst,
    callback: T.Optional[T.Callable[[int], None]] = None,
    buffer: int = READER_BLOCK_SIZE,
) -> None:
    """Copy data from fsrc to fdst with optional progress callback.

    This is similar to shutil.copyfileobj but with callback support.

    Args:
        fsrc: Source file-like object (opened for reading)
        fdst: Destination file-like object (opened for writing)
        callback: Optional callback function called with number of bytes written
        buffer: Buffer size for copying (default: READER_BLOCK_SIZE)
    """
    while True:
        buf = await fsrc.read(buffer)
        if not buf:
            break
        await fdst.write(buf)
        if callback:
            callback(len(buf))
