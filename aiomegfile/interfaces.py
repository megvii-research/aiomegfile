import stat
import typing as T
from abc import ABC, abstractmethod

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.lib.url import split_uri

Self = T.TypeVar("Self")


class StatResult(T.NamedTuple):
    size: int = 0
    ctime: float = 0.0
    mtime: float = 0.0
    isdir: bool = False
    islnk: bool = False
    extra: T.Any = None

    def is_file(self) -> bool:
        return not self.isdir or self.islnk

    def is_dir(self) -> bool:
        return self.isdir and not self.islnk

    def is_symlink(self) -> bool:
        return self.islnk

    @property
    def st_mode(self) -> int:
        """
        File mode: file type and file mode bits (permissions).
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_mode"):
            return self.extra.st_mode
        if self.is_symlink():
            return stat.S_IFLNK
        elif self.is_dir():
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
    def st_size(self) -> int:
        """
        Size of the file in bytes.
        """
        if self.extra and hasattr(self.extra, "st_size"):
            return self.extra.st_size
        return self.size

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
    def st_mtime(self) -> float:
        """
        Time of most recent content modification expressed in seconds.
        """
        if self.extra and hasattr(self.extra, "st_mtime"):
            return self.extra.st_mtime
        return self.mtime

    @property
    def st_ctime(self) -> float:
        """
        Platform dependent:

            the time of most recent metadata change on Unix,
            the time of creation on Windows, expressed in seconds,
            the time of file created on oss;
            if is dir, return the latest ctime of the files in dir.
        """
        if self.extra and hasattr(self.extra, "st_ctime"):
            return self.extra.st_ctime
        return self.ctime

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


FILE_SYSTEMS = {}


class BaseFileSystem(ABC):
    protocol = ""

    def __init_subclass__(cls):
        if not cls.protocol:
            raise ValueError(
                f"Subclasses({cls.__name__}) of "
                "BaseFileSystem must define a profile_name"
            )
        if cls.protocol in FILE_SYSTEMS:
            raise ValueError(
                f"File system protocol '{cls.protocol}' "
                f"already registered by {FILE_SYSTEMS[cls.protocol]!r}"
            )
        FILE_SYSTEMS[cls.protocol] = cls

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path is a directory, otherwise False.
        """
        raise NotImplementedError('method "is_dir" not implemented: %r' % self)

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path is a regular file, otherwise False.
        """
        raise NotImplementedError('method "is_file" not implemented: %r' % self)

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param followlinks: Whether to follow symbolic links when checking.
        :return: True if the path exists, otherwise False.
        """
        raise NotImplementedError('method "exists" not implemented: %r' % self)

    async def stat(self, path: str, follow_symlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param follow_symlinks: Whether to follow symbolic links when
            resolving the path.
        :return: StatResult information for the path.
        """
        raise NotImplementedError('method "stat" not implemented: %r' % self)

    async def unlink(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param missing_ok: If False, raise FileNotFoundError when the file is missing.
        :raises FileNotFoundError: When file is missing and missing_ok is False.
        """
        raise NotImplementedError('method "unlink" not implemented: %r' % self)

    async def rmdir(self, path: str) -> None:
        """Remove (delete) the directory."""
        raise NotImplementedError('method "rmdir" not implemented: %r' % self)

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

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

        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding when opening in text mode.
        :param errors: Error handling strategy for encoding/decoding.
        :param newline: Newline handling in text mode.
        :return: Async file context manager.
        """
        raise NotImplementedError('method "open" not implemented: %r' % self)

    async def walk(
        self, path: str, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param followlinks: Whether to traverse symbolic links to directories.
        :return: Async iterator of (root, dirs, files).
        """
        raise NotImplementedError('method "walk" not implemented: %r' % self)
        yield

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        move file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :return: Destination path after move.
        :raises FileExistsError: If destination exists and overwrite is False.
        """
        raise NotImplementedError(f"'move' is unsupported on '{type(self)}'")

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param dst_path: The symbolic link path.
        """
        raise NotImplementedError(f"'symlink' is unsupported on '{type(self)}'")

    async def readlink(self, path: str) -> str:
        """
        Return a new path representing the symbolic link's target.

        :return: Target path of the symbolic link.
        """
        raise NotImplementedError(f"'readlink' is unsupported on '{type(self)}'")

    async def is_symlink(self, path: str) -> bool:
        """
        Return True if the path points to a symbolic link.
        """
        raise NotImplementedError(f"'is_symlink' is unsupported on '{type(self)}'")

    async def iterdir(self, path: str) -> T.AsyncIterator[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :return: All contents have in the path in ascending alphabetical order
        """
        raise NotImplementedError(f"'iterdir' is unsupported on '{type(self)}'")
        yield

    async def absolute(self, path: str) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object

        :return: Absolute path string.
        """
        raise NotImplementedError(f"'absolute' is unsupported on '{type(self)}'")

    async def samefile(self, path: str, other_path: str) -> bool:
        """
        Return whether this path points to the same file

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
    def get_path_from_uri(self, uri: str) -> str:
        """
        Get the path part from uri.

        :param uri: URI string.
        :return: Path part string.
        """

    @abstractmethod
    def generate_uri(self, path: str) -> str:
        """
        Generate URI for the filesystem.

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
