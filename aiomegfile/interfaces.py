import io
import logging
import os
import stat
import typing as T
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from types import TracebackType

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.utils.parse import fullname
from aiomegfile.utils.path import split_uri

Self = T.TypeVar("Self")
logger = logging.getLogger(__name__)


class StatResult(T.NamedTuple):
    st_size: int = 0
    st_ctime: float = 0.0
    st_mtime: float = 0.0
    isdir: bool = False
    islnk: bool = False
    extra: T.Any = None

    @property
    def st_mode(self) -> int:
        """
        File mode: file type and file mode bits (permissions).
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_mode"):
            return self.extra.st_mode
        if self.islnk:
            return stat.S_IFLNK
        elif self.isdir:
            return stat.S_IFDIR
        return stat.S_IFREG

    @property
    def st_ino(self) -> int:
        """
        Platform dependent, but if non-zero, uniquely identifies the file for
        a given value of st_dev. Typically:

        the inode number on Unix,
        the file index on Windows,
        the decimal of etag on oss.
        """
        if self.extra:
            if hasattr(self.extra, "st_ino"):
                return self.extra.st_ino
            elif isinstance(self.extra, dict) and self.extra.get("ETag"):
                return int(self.extra["ETag"][1:-1], 16)
        return 0

    @property
    def st_dev(self) -> int:
        """
        Identifier of the device on which this file resides.
        """
        if self.extra:
            if hasattr(self.extra, "st_dev"):
                return self.extra.st_dev
        return 0

    @property
    def st_nlink(self) -> int:
        """
        Number of hard links.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_nlink"):
            return self.extra.st_nlink
        return 0

    @property
    def st_uid(self) -> int:
        """
        User identifier of the file owner.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_uid"):
            return self.extra.st_uid
        return 0

    @property
    def st_gid(self) -> int:
        """
        Group identifier of the file owner.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_gid"):
            return self.extra.st_gid
        return 0

    @property
    def st_atime(self) -> float:
        """
        Time of most recent access expressed in seconds.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_atime"):
            return self.extra.st_atime
        return 0.0

    @property
    def st_atime_ns(self) -> int:
        """
        Time of most recent access expressed in nanoseconds as an integer.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_atime_ns"):
            return self.extra.st_atime_ns
        return 0

    @property
    def st_mtime_ns(self) -> int:
        """
        Time of most recent content modification expressed in nanoseconds as an integer.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_mtime_ns"):
            return self.extra.st_mtime_ns
        return 0

    @property
    def st_ctime_ns(self) -> int:
        """
        Platform dependent:

            the time of most recent metadata change on Unix,
            the time of creation on Windows, expressed in nanoseconds as an integer.

        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_ctime_ns"):
            return self.extra.st_ctime_ns
        return 0


class FileEntry(T.NamedTuple):
    name: str
    path: str
    stat: StatResult

    def inode(self) -> T.Optional[T.Union[int, str]]:
        return self.stat.st_ino

    def is_file(self) -> bool:
        return not self.stat.isdir or self.stat.islnk

    def is_dir(self) -> bool:
        return self.stat.isdir and not self.stat.islnk

    def is_symlink(self) -> bool:
        return self.stat.islnk


class AsyncIOManager(AbstractAsyncContextManager):
    """
    Async-compatible wrapper around `AsyncReadable` or `AsyncWritable`
    """

    def __init__(self, thing):
        self._thing = thing

    def __await__(self):
        """Return self to provide a minimal awaitable interface."""

        async def dummy():
            return self._thing

        return dummy().__await__()

    async def __aenter__(self):
        """Return the underlying iterator."""
        return self._thing

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close the underlying iterator."""
        if self._thing:
            await self._thing.close()


class AsyncScannableManager(AbstractAsyncContextManager):
    """
    Async-compatible wrapper around ``scandir`` or ``scanfile``
    that yields ``FileEntry`` objects.
    """

    def __init__(
        self,
        aiterator: T.AsyncIterator,
        aexit: T.Optional[
            T.Callable[
                [
                    T.Type[BaseException] | None,
                    BaseException | None,
                    TracebackType | None,
                ],
                T.Awaitable[None],
            ]
        ] = None,
    ):
        """Initialize the iterator for a directory path.

        :param path: Directory path to scan.
        """
        self._aiterator = aiterator
        self._aexit = aexit

    async def __anext__(self) -> FileEntry:
        """Return the next directory entry or raise ``StopAsyncIteration``."""
        return await anext(self._aiterator)

    def __aiter__(self):
        """Return self to support ``async for`` iteration."""
        return self

    def __await__(self):
        """Return self to provide a minimal awaitable interface."""

        async def dummy():
            return self

        return dummy().__await__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close the underlying iterator."""
        if self._aexit:
            await self._aexit(exc_type, exc_value, traceback)


FILE_SYSTEMS = {}


class BaseFileSystem(ABC):
    """Abstract base class for all file system implementations.

    .. note ::

        For performance reasons, some interface definitions are different.

            1. The ``copy``, ``upload``, and ``download`` operations only handle
                single files, as they typically require processing each file's data.
                Therefore, providing low-level APIs in the ``FileSystem`` is sufficient.

            2. The ``move`` and ``remove`` operations should support both files and
                directories to allow for file system optimizations.
                In a local file system, for instance, moving a directory typically only
                requires updating the directory entry.
    """

    protocol = ""

    def __init_subclass__(cls):
        if not cls.protocol:
            raise ValueError(
                f"Subclasses({cls.__name__}) of BaseFileSystem must define a protocol"
            )
        if cls.protocol in FILE_SYSTEMS:
            raise ValueError(
                f"File system protocol '{cls.protocol}' "
                f"already registered by {FILE_SYSTEMS[cls.protocol]!r}"
            )
        FILE_SYSTEMS[cls.protocol] = cls

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param path: The path to check.
        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path is a directory, otherwise False.
        """
        raise NotImplementedError('method "is_dir" not implemented: %r' % self)

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param path: The path to check.
        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path is a regular file, otherwise False.
        """
        raise NotImplementedError('method "is_file" not implemented: %r' % self)

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param path: The path to check.
        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path exists, otherwise False.
        """
        raise NotImplementedError('method "exists" not implemented: %r' % self)

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Get the status of the path.

        :param path: File path.
        :param followlinks: Whether to follow symbolic links when
            resolving the path.
        :return: StatResult information for the path.
        """
        raise NotImplementedError('method "stat" not implemented: %r' % self)

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file or directory.

        If path is a file, remove it directly.
        If path is a directory, remove it and all its contents recursively.

        :param path: The file or directory path to remove.
        :param missing_ok: If False, raise FileNotFoundError when the path is missing.
        :raises FileNotFoundError: When path is missing and missing_ok is False.
        """
        raise NotImplementedError('method "remove" not implemented: %r' % self)

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param path: The directory path to create.
        :param mode: Permission bits for the new directory.
        :param parents: Whether to create parent directories as needed.
        :param exist_ok: Whether to ignore if the directory already exists.
        """
        raise NotImplementedError('method "mkdir" not implemented: %r' % self)

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
    ) -> T.AsyncContextManager:
        """Open the file with mode.

        :param path: File path.
        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding when opening in text mode.
        :param errors: Error handling strategy for encoding/decoding.
        :param newline: Newline handling in text mode.
        :return: Async file context manager.
        """
        raise NotImplementedError('method "open" not implemented: %r' % self)

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an iterator of ``FileEntry`` objects corresponding to the entries
            in the directory given by path.

        :param path: Directory path to scan.
        :type path: str
        :return: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        raise NotImplementedError('method "scandir" not implemented: %r' % self)

    def scanfile(
        self,
        path,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """
        Iteratively traverse only files in given directory.
        Every iteration on generator yields FileEntry object.

        :returns: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        raise NotImplementedError('method "scanfile" not implemented: %r' % self)

    async def upload(self, src_path: str, dst_path: str) -> None:
        """
        upload file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        raise NotImplementedError(f"'upload' is unsupported on '{type(self)}'")

    async def download(self, src_path: str, dst_path: str) -> None:
        """
        download file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        raise NotImplementedError(f"'download' is unsupported on '{type(self)}'")

    async def copy(self, src_path: str, dst_path: str) -> str:
        """
        copy single file, not directory

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: Destination path after copy.
        """
        raise NotImplementedError(f"'copy' is unsupported on '{type(self)}'")

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        move file or directory

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :return: Destination path after move.
        :raises FileExistsError: If destination exists and overwrite is False.
        """
        raise NotImplementedError(f"'move' is unsupported on '{type(self)}'")

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param src_path: The source path the symbolic link points to.
        :param dst_path: The symbolic link path.
        """
        raise NotImplementedError(f"'symlink' is unsupported on '{type(self)}'")

    async def readlink(self, path: str) -> str:
        """
        Return a new path representing the symbolic link's target.

        :param path: The symbolic link path.
        :return: Target path of the symbolic link.
        """
        raise NotImplementedError(f"'readlink' is unsupported on '{type(self)}'")

    async def is_symlink(self, path: str) -> bool:
        """
        Return True if the path points to a symbolic link.

        :param path: The path to check.
        :return: True if the path is a symbolic link, otherwise False.
        """
        raise NotImplementedError(f"'is_symlink' is unsupported on '{type(self)}'")

    async def absolute(self, path: str) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object

        :param path: The path to make absolute.
        :return: Absolute path string.
        """
        raise NotImplementedError(f"'absolute' is unsupported on '{type(self)}'")

    async def samefile(self, path: str, other_path: str) -> bool:
        """
        Return whether this path points to the same file

        :param path: Path to compare.
        :param other_path: Path to compare.
        :return: True if both represent the same file.
        """
        raise NotImplementedError(f"'samefile' is unsupported on '{type(self)}'")

    @abstractmethod
    def same_endpoint(self, other_filesystem: "BaseFileSystem") -> bool:
        """
        Return whether this filesystem points to the same endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True if both represent the same endpoint.
        """

    @abstractmethod
    def parse_uri(self, uri: str) -> str:
        """
        Parse the path part from a URI.

        :param uri: URI string.
        :return: Path part string.
        """

    @abstractmethod
    def build_uri(self, path: str) -> str:
        """
        Build URI for the filesystem by path part.

        :param path: Path without protocol.
        :return: Generated URI string.
        """
        return f"{self.protocol}://{path}"

    @classmethod
    @abstractmethod
    def from_uri(
        cls: T.Type[Self],
        uri: str,
    ) -> Self:
        """Return new instance of this class

        :param uri: URI string.
        :return: new instance of new path
        """


def get_filesystem_by_uri(
    uri: str,
) -> BaseFileSystem:
    protocol, _, _ = split_uri(uri)
    if protocol not in FILE_SYSTEMS:
        raise ProtocolNotFoundError(f"protocol {protocol!r} not found")
    path_class = FILE_SYSTEMS[protocol]
    return path_class.from_uri(
        uri,
    )


class AsyncClosable(ABC):
    """Async closable base class for file-like objects."""

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        close_impl = cls.__dict__.get("close")
        if close_impl is None:
            return

        async def _wrapped_close(self) -> None:
            if not getattr(self, "__closed__", False):
                await close_impl(self)
                setattr(self, "__closed__", True)

        _wrapped_close.__name__ = close_impl.__name__
        _wrapped_close.__qualname__ = close_impl.__qualname__
        _wrapped_close.__doc__ = close_impl.__doc__
        cls.close = _wrapped_close  # type: ignore[assignment]

    @property
    def closed(self) -> bool:
        """Return True if the file-like object is closed."""
        return getattr(self, "__closed__", False)

    @abstractmethod
    async def close(self) -> None:
        """Flush and close the file-like object.

        This method has no effect if the file is already closed.
        """
        pass  # pragma: no cover

    async def __aenter__(self: Self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class AsyncFileLike(AsyncClosable, T.Generic[T.AnyStr], ABC):
    """Async file-like base class."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the file."""

    @property
    @abstractmethod
    def mode(self) -> str:
        """Return the mode of the file."""

    def fileno(self) -> int:
        raise io.UnsupportedOperation("not a local file")

    async def isatty(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "<%s name=%r mode=%r>" % (
            fullname(self),
            self.name,
            self.mode,
        )

    async def seekable(self) -> bool:
        """Return True if the file-like object can be sought."""
        return False

    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte `offset` relative to position indicated by `whence`:
            0  Start of stream (the default).  `offset` should be >= 0;
            1  Current position - `offset` may be negative;
            2  End of stream - `offset` usually negative.

        Return the new absolute position.
        """
        raise io.UnsupportedOperation("not seekable")  # pragma: no cover

    async def readable(self) -> bool:
        """Return True if the file-like object can be read."""
        return False  # pragma: no cover

    async def writable(self) -> bool:
        """Return True if the file-like object can be written."""
        return False

    async def flush(self) -> None:
        """Flush write buffers, if applicable.

        This is not implemented for read-only and non-blocking streams.
        """

    @abstractmethod
    async def tell(self) -> int:
        """Return the current stream position."""


class AsyncSeekable(AsyncFileLike, ABC):
    """Async seekable file-like base class."""

    async def seekable(self) -> bool:
        """Return True if the file-like object can be sought."""
        return True

    @abstractmethod
    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte offset `cookie` relative to position indicated by `whence`:
            0  Start of stream (the default).  `cookie` should be >= 0;
            1  Current position - `cookie` may be negative;
            2  End of stream - `cookie` usually negative.

        Return the new absolute position.
        """


class AsyncReadable(AsyncFileLike[T.AnyStr], ABC):
    """Async readable file-like base class."""

    async def readable(self) -> bool:
        """Return True if the file-like object can be read."""
        return True

    @abstractmethod
    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read at most `size` bytes, returned as a bytes object.

        If the `size` argument is negative, read until EOF is reached.
        Return an empty bytes object at EOF.
        """

    @abstractmethod
    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Next line from the file, as a bytes object.

        Retain newline. A non-negative `size` argument limits the maximum number of
        bytes to return (an incomplete line may be returned then).
        Return an empty bytes object at EOF.
        """

    async def readlines(self, hint: T.Optional[int] = None) -> T.List[T.AnyStr]:
        """Return a list of lines from the stream."""
        data = await self.read(size=hint)
        return data.splitlines(True)  # pyre-ignore[7]

    async def readinto(self, buffer: bytearray) -> int:
        """Read bytes into buffer.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        if "b" not in self.mode:
            raise OSError("'readinto' only works on binary files")

        data = await self.read(len(buffer))
        size = len(data)
        buffer[:size] = data  # pyre-ignore[6]
        return size

    async def __anext__(self) -> T.AnyStr:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    def __aiter__(self: Self) -> Self:
        return self

    async def truncate(self, size: T.Optional[int] = None) -> int:
        raise OSError("not writable")

    async def write(self, data: T.AnyStr) -> int:
        raise OSError("not writable")

    async def writelines(self, lines: T.Iterable[T.AnyStr]) -> None:
        raise OSError("not writable")


class AsyncWritable(AsyncFileLike[T.AnyStr], ABC):
    """Async writable file-like base class."""

    async def writable(self) -> bool:
        """Return True if the file-like object can be written."""
        return True

    @abstractmethod
    async def write(self, data: T.AnyStr) -> int:
        """Write bytes to file.

        Return the number of bytes written.
        """

    async def writelines(self, lines: T.Iterable[T.AnyStr]) -> None:
        """Write `lines` to the file.

        Note that newlines are not added.
        `lines` can be any iterable object producing bytes-like objects.
        This is equivalent to calling write() for each element.
        """
        for line in lines:
            await self.write(line)

    async def truncate(self, size: T.Optional[int] = None) -> int:
        """
        Resize the stream to the given size in bytes.

        :param size: resize size, defaults to None
        :type size: int, optional

        :raises OSError: When the stream is not support truncate.
        :return: The new file size.
        :rtype: int
        """
        raise io.UnsupportedOperation("not support truncate")

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        raise OSError("not readable")

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        raise OSError("not readable")

    async def readlines(self, hint: T.Optional[int] = None) -> T.List[T.AnyStr]:
        raise OSError("not readable")
