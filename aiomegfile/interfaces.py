import stat
import typing as T
from abc import ABC, abstractmethod

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.lib.url import split_uri

Self = T.TypeVar("Self")


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

    async def stat(self, path: str, followlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param path: File path.
        :param followlinks: Whether to follow symbolic links when
            resolving the path.
        :return: StatResult information for the path.
        """
        raise NotImplementedError('method "stat" not implemented: %r' % self)

    async def unlink(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param path: The file path to remove.
        :param missing_ok: If False, raise FileNotFoundError when the file is missing.
        :raises FileNotFoundError: When file is missing and missing_ok is False.
        """
        raise NotImplementedError('method "unlink" not implemented: %r' % self)

    async def rmdir(self, path: str, missing_ok: bool = False) -> None:
        """
        Remove (delete) the directory.

        :param path: The directory path to remove.
        :param missing_ok: If False,
            raise FileNotFoundError when the directory is missing.
        :raises FileNotFoundError: When directory is missing and missing_ok is False.
        """
        raise NotImplementedError('method "rmdir" not implemented: %r' % self)

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

    async def walk(
        self, path: str, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param path: Root directory path to start walking.
        :param followlinks: Whether to traverse symbolic links to directories.
        :return: Async iterator of (root, dirs, files).
        """
        raise NotImplementedError('method "walk" not implemented: %r' % self)
        yield

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an iterator of ``FileEntry`` objects corresponding to the entries
            in the directory given by path.

        :param path: Directory path to scan.
        :type path: str
        :return: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        raise NotImplementedError('method "scandir" not implemented: %r' % self)

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
        move file

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

    async def iterdir(self, path: str) -> T.AsyncIterator[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :param path: The directory path to list contents.
        :return: All contents have in the path in ascending alphabetical order
        """
        raise NotImplementedError(f"'iterdir' is unsupported on '{type(self)}'")
        yield

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


# TODO: cache filesystem instances
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
