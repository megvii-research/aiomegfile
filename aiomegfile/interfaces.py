import stat
import typing as T
from functools import cached_property


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


class BaseFileSystem:
    protocol = ""

    def __init__(
        self,
        path_without_protocol: str,
        profile_name: T.Optional[str] = None,
    ):
        self.path_without_protocol = path_without_protocol
        self.profile_name = profile_name

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

    @cached_property
    def path_with_protocol(self) -> str:
        """Return path with protocol, like file:///root, s3://bucket/key"""
        protocol_prefix = self.protocol + "://"
        return protocol_prefix + self.path_without_protocol

    async def is_dir(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param followlinks: Whether to follow symbolic links when checking.
        """
        raise NotImplementedError('method "is_dir" not implemented: %r' % self)

    async def is_file(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param followlinks: Whether to follow symbolic links when checking.
        """
        raise NotImplementedError('method "is_file" not implemented: %r' % self)

    async def exists(self, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param followlinks: Whether to follow symbolic links when checking.
        """
        raise NotImplementedError('method "exists" not implemented: %r' % self)

    async def stat(self, follow_symlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param follow_symlinks: Whether to follow symbolic links when
            resolving the path.
        """
        raise NotImplementedError('method "stat" not implemented: %r' % self)

    async def unlink(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param missing_ok: If False, raise FileNotFoundError when the file is missing.
        """
        raise NotImplementedError('method "unlink" not implemented: %r' % self)

    async def rmdir(self) -> None:
        """Remove (delete) the directory."""
        raise NotImplementedError('method "rmdir" not implemented: %r' % self)

    async def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a directory.

        :param mode: Permission bits for the new directory.
        :param parents: Whether to create parent directories as needed.
        :param exist_ok: Whether to ignore if the directory already exists.
        """
        raise NotImplementedError('method "mkdir" not implemented: %r' % self)

    def open(
        self,
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
        """
        raise NotImplementedError('method "open" not implemented: %r' % self)

    async def walk(
        self, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param followlinks: Whether to traverse symbolic links to directories.
        """
        raise NotImplementedError('method "walk" not implemented: %r' % self)
        yield

    async def iglob(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> T.AsyncIterator[str]:
        """Return an iterator of files whose paths match the glob pattern.

        :param pattern: Glob pattern to match.
        :param recursive: Whether to allow recursive "**" matching.
        :param missing_ok: Whether to suppress errors when nothing matches.
        """
        raise NotImplementedError('method "iglob" not implemented: %r' % self)
        yield


    async def move(self, dst_path: str, overwrite: bool = True) -> str:
        """
        move file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        raise NotImplementedError(f"'move' is unsupported on '{type(self)}'")

    async def symlink(self, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param dst_path: The symbolic link path.
        """
        raise NotImplementedError(f"'symlink' is unsupported on '{type(self)}'")

    async def readlink(self) -> str:
        """
        Return a new path representing the symbolic link's target.
        """
        raise NotImplementedError(f"'readlink' is unsupported on '{type(self)}'")

    async def is_symlink(self) -> bool:
        """
        Return True if the path points to a symbolic link.
        """
        raise NotImplementedError(f"'is_symlink' is unsupported on '{type(self)}'")

    async def iterdir(self) -> T.AsyncIterator[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        raise NotImplementedError(f"'iterdir' is unsupported on '{type(self)}'")
        yield

    async def absolute(self) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        raise NotImplementedError(f"'absolute' is unsupported on '{type(self)}'")

    def __eq__(self, other: "BaseFileSystem") -> bool:
        return (
            self.protocol == other.protocol
            and self.profile_name == other.profile_name
            and self.path_without_protocol == other.path_without_protocol
        )
