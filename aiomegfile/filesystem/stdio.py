import asyncio
import io
import typing as T

from aiomegfile.interfaces import (
    AioReadable,
    AioWritable,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.utils.path import split_uri

_VALID_STDIO_PATHS = {"-", "0", "1", "2"}
_VALID_MODES = {"rb", "wb", "rt", "wt", "r", "w"}


class AioSTDReader(AioReadable[T.AnyStr]):
    """Async wrapper around ``sys.stdin``."""

    def __init__(self, mode: str) -> None:
        """Initialize stdio reader.

        :param mode: Reader mode.
        """
        import sys

        self._mode = mode
        if "b" in mode:
            self._handler = sys.stdin.buffer
        else:
            self._handler = sys.stdin

    @property
    def name(self) -> str:
        """Return reader name.

        :return: Stream name.
        :rtype: str
        """
        return "stdin"

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: Open mode.
        :rtype: str
        """
        return self._mode

    def _check_closed(self) -> None:
        """Raise when stream is already closed.

        :raises IOError: If stream is closed.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read data from stdin.

        :param size: Maximum size to read.
        :return: Read data.
        :rtype: T.AnyStr
        """
        self._check_closed()
        if size is None:
            return await asyncio.to_thread(self._handler.read)
        return await asyncio.to_thread(self._handler.read, size)

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read one line from stdin.

        :param size: Maximum size to read.
        :return: Line data.
        :rtype: T.AnyStr
        """
        self._check_closed()
        if size is None:
            return await asyncio.to_thread(self._handler.readline)
        return await asyncio.to_thread(self._handler.readline, size)

    async def tell(self) -> int:
        """Return current offset.

        :raises io.UnsupportedOperation: Always raised for stdio stream.
        """
        raise io.UnsupportedOperation("not tellable")

    async def close(self) -> None:
        """Close reader handle.

        ``stdin`` itself is not closed.
        """
        return None


class AioSTDWriter(AioWritable[T.AnyStr]):
    """Async wrapper around ``sys.stdout`` or ``sys.stderr``."""

    def __init__(self, path: str, mode: str) -> None:
        """Initialize stdio writer.

        :param path: stdio path without protocol.
        :param mode: Writer mode.
        """
        import sys

        self._mode = mode
        if path == "2":
            self._name = "stderr"
            handler = sys.stderr
        else:
            self._name = "stdout"
            handler = sys.stdout

        if "b" in mode:
            handler = handler.buffer

        self._handler = handler

    @property
    def name(self) -> str:
        """Return writer name.

        :return: Stream name.
        :rtype: str
        """
        return self._name

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: Open mode.
        :rtype: str
        """
        return self._mode

    def _check_closed(self) -> None:
        """Raise when stream is already closed.

        :raises IOError: If stream is closed.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

    async def write(self, data: T.AnyStr) -> int:
        """Write data to stdout or stderr.

        :param data: Data to write.
        :return: Number of written items.
        :rtype: int
        """
        self._check_closed()
        return await asyncio.to_thread(self._handler.write, data)

    async def flush(self) -> None:
        """Flush writer buffer."""
        self._check_closed()
        await asyncio.to_thread(self._handler.flush)

    async def tell(self) -> int:
        """Return current offset.

        :raises io.UnsupportedOperation: Always raised for stdio stream.
        """
        raise io.UnsupportedOperation("not tellable")

    async def close(self) -> None:
        """Close writer handle.

        ``stdout``/``stderr`` itself is not closed.
        """
        await self.flush()


class StdioFileSystem(BaseFileSystem):
    """Filesystem adapter for ``stdio://`` paths."""

    protocol = "stdio"

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return False for stdio streams.

        :param path: Path without protocol.
        :param followlinks: Unused compatibility argument.
        :return: Always False.
        :rtype: bool
        """
        _ = path, followlinks
        return False

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True when path is a valid stdio stream.

        :param path: Path without protocol.
        :param followlinks: Unused compatibility argument.
        :return: Whether path is valid stdio path.
        :rtype: bool
        """
        _ = followlinks
        return path in _VALID_STDIO_PATHS

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return True when path is a valid stdio stream.

        :param path: Path without protocol.
        :param followlinks: Unused compatibility argument.
        :return: Whether path is valid stdio path.
        :rtype: bool
        """
        _ = followlinks
        return path in _VALID_STDIO_PATHS

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Stat operation is unsupported for stdio stream.

        :param path: Path without protocol.
        :param followlinks: Unused compatibility argument.
        :raises OSError: Always raised.
        """
        _ = path, followlinks
        raise OSError("stdio path does not support stat")

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove operation is unsupported for stdio stream.

        :param path: Path without protocol.
        :param missing_ok: Unused compatibility argument.
        :raises OSError: Always raised.
        """
        _ = path, missing_ok
        raise OSError("stdio path cannot be removed")

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Directory operation is unsupported for stdio stream.

        :param path: Path without protocol.
        :param mode: Unused compatibility argument.
        :param parents: Unused compatibility argument.
        :param exist_ok: Unused compatibility argument.
        :raises NotADirectoryError: Always raised.
        """
        _ = path, mode, parents, exist_ok
        raise NotADirectoryError("stdio path is not a directory")

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        **kwargs: T.Any,
    ) -> T.AsyncContextManager[T.Any]:
        """Open stdio stream in read or write mode.

        :param path: Path without protocol.
        :param mode: Stream mode. Supports ``rb``, ``wb``, ``rt``, ``wt``, ``r``, ``w``.
        :param buffering: Unused compatibility argument.
        :param encoding: Unused compatibility argument.
        :param errors: Unused compatibility argument.
        :param newline: Unused compatibility argument.
        :param kwargs: Unused compatibility arguments.
        :raises ValueError: If mode/path is not acceptable.
        :return: Async stdio reader or writer.
        :rtype: T.AsyncContextManager[T.Any]
        """
        _ = buffering, encoding, errors, newline, kwargs
        if mode not in _VALID_MODES:
            raise ValueError("unacceptable mode: %r" % mode)

        uri = self.build_uri(path)
        if path not in _VALID_STDIO_PATHS:
            raise ValueError("unacceptable path: %r" % uri)

        if path in {"1", "2"} and "r" in mode:
            raise ValueError("cannot open for reading: %r" % uri)

        if path == "0" and "w" in mode:
            raise ValueError("cannot open for writing: %r" % uri)

        if "r" in mode:
            return AioSTDReader(mode)
        return AioSTDWriter(path, mode)

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Directory scan is unsupported for stdio stream.

        :param path: Path without protocol.
        :raises NotADirectoryError: Always raised.
        """
        _ = path
        raise NotADirectoryError("stdio path is not a directory")

    def scanfile(
        self,
        path: str,
        sort: bool = False,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """File scan is unsupported for stdio stream.

        :param path: Path without protocol.
        :param sort: Compatibility flag for protocol-aligned scanfile APIs.
        :raises NotADirectoryError: Always raised.
        """
        _ = path
        _ = sort
        raise NotADirectoryError("stdio path is not a directory")

    async def absolute(self, path: str) -> str:
        """Return path unchanged for stdio stream.

        :param path: Path without protocol.
        :return: Original path.
        :rtype: str
        """
        return path

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """Return whether filesystem points to same stdio endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True when both are ``StdioFileSystem``.
        :rtype: bool
        """
        return isinstance(other_filesystem, StdioFileSystem)

    def parse_uri(self, uri: str) -> str:
        """Parse path part from URI.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        _, path, _ = split_uri(uri)
        return path

    def build_uri(self, path: str) -> str:
        """Build stdio URI from path part.

        :param path: Path without protocol.
        :return: URI string.
        :rtype: str
        """
        return f"{self.protocol}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "StdioFileSystem":
        """Create a new filesystem instance from URI.

        :param uri: URI string.
        :return: New filesystem instance.
        :rtype: StdioFileSystem
        """
        _ = uri
        return cls()
