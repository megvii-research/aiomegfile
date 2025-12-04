import os
import typing as T
from collections.abc import Sequence
from functools import cached_property

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.interfaces import PROTOCOLS, BaseProtocol, StatResult
from aiomegfile.lib.fnmatch import _compile_pattern


def fspath(path: T.Union[str, os.PathLike]) -> str:
    path = os.fspath(path)
    if isinstance(path, bytes):
        path = path.decode()
    return path


class URIPathParents(Sequence):
    def __init__(self, path):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        parts = path.parts
        if len(parts) > 0 and parts[0] == path.protocol.protocol_name + "://":
            self.prefix = parts[0]
            self.parts = parts[1:]
        else:
            self.prefix = ""
            self.parts = parts

    def __len__(self):
        return max(len(self.parts) - 1, 0)

    def __getitem__(self, idx):
        if idx < 0 or idx > len(self):
            raise IndexError(idx)

        if len(self.parts[: -idx - 1]) > 1:
            other_path = os.path.join(*self.parts[: -idx - 1])
        elif len(self.parts[: -idx - 1]) == 1:
            other_path = self.parts[: -idx - 1][0]
        else:
            other_path = ""
        return self.cls(self.prefix + other_path)


class SmartPath:
    def __init__(self, path: T.Union[str, "SmartPath", os.PathLike]):
        if isinstance(path, SmartPath):
            self.path = path.path
        else:
            self.path = fspath(path)
        self.protocol = self._create_protocol(self.path)

    @classmethod
    def _split_path(cls, path: str) -> T.Tuple[str, T.Optional[str], str]:
        if isinstance(path, str):
            if "://" in path:
                protocol_name, path_without_protocol = path.split("://", 1)
            else:
                protocol_name = "file"
                path_without_protocol = path
            if "+" in protocol_name:
                protocol_name, profile_name = protocol_name.split("+", 1)
            else:
                profile_name = None
            return protocol_name, profile_name, path_without_protocol
        raise ProtocolNotFoundError("protocol not found: %r" % path)

    @classmethod
    def _create_protocol(cls, path: str) -> BaseProtocol:
        protocol_name, profile_name, path_without_protocol = cls._split_path(path)
        if protocol_name.startswith("s3+"):
            protocol_name = "s3"

        if protocol_name not in PROTOCOLS:
            raise ProtocolNotFoundError(
                "protocol %r not found: %r" % (protocol_name, path)
            )
        path_class = PROTOCOLS[protocol_name]
        return path_class(
            path_without_protocol,
            profile_name,
        )

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash(fspath(self))

    def __eq__(self, other_path: "SmartPath") -> bool:
        return self.protocol == other_path.protocol

    def __lt__(self, other_path: "SmartPath") -> bool:
        if not isinstance(other_path, SmartPath):
            raise TypeError("%r is not 'SmartPath'" % other_path)
        if self.protocol.protocol_name != other_path.protocol.protocol_name:
            raise TypeError(
                "'<' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return fspath(self) < fspath(other_path)

    def __le__(self, other_path: "SmartPath") -> bool:
        if not isinstance(other_path, SmartPath):
            raise TypeError("%r is not 'SmartPath'" % other_path)
        if self.protocol.protocol_name != other_path.protocol.protocol_name:
            raise TypeError(
                "'<=' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) <= str(other_path)

    def __gt__(self, other_path: "SmartPath") -> bool:
        if not isinstance(other_path, SmartPath):
            raise TypeError("%r is not 'SmartPath'" % other_path)
        if self.protocol.protocol_name != other_path.protocol.protocol_name:
            raise TypeError(
                "'>' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) > str(other_path)

    def __ge__(self, other_path: "SmartPath") -> bool:
        if not isinstance(other_path, SmartPath):
            raise TypeError("%r is not 'SmartPath'" % other_path)
        if self.protocol.protocol_name != other_path.protocol.protocol_name:
            raise TypeError(
                "'>=' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) >= str(other_path)

    def __truediv__(
        self, other_path: T.Union["SmartPath", os.PathLike, str]
    ) -> "SmartPath":
        if isinstance(other_path, SmartPath):
            if self.protocol.protocol_name != other_path.protocol.protocol_name:
                raise TypeError(
                    "'/' not supported between instances of %r and %r"
                    % (type(self), type(other_path))
                )
        elif isinstance(other_path, os.PathLike):
            other_path = fspath(other_path)
        elif not isinstance(other_path, str):
            raise TypeError("%r is not 'PathLike' object" % other_path)
        return self.joinpath(other_path)

    @cached_property
    def path_with_protocol(self) -> str:
        """Return path with protocol, like file:///root, s3://bucket/key"""
        path = self.path
        protocol_prefix = self.protocol.protocol_name + "://"
        if path.startswith(protocol_prefix):
            return path
        return protocol_prefix + path.lstrip("/")

    @cached_property
    def path_without_protocol(self) -> str:
        """
        Return path without protocol, example: if path is s3://bucket/key,
        return bucket/key
        """
        path = self.path
        protocol_prefix = self.protocol.protocol_name + "://"
        if path.startswith(protocol_prefix):
            path = path[len(protocol_prefix) :]
        return path

    async def as_uri(self) -> str:
        return self.path_with_protocol

    async def as_posix(self) -> str:
        """Return a string representation of the path with forward slashes (/)"""
        return self.path_with_protocol

    @classmethod
    def from_path(cls, path: T.Union[str, "SmartPath", os.PathLike]) -> "SmartPath":
        """Return new instance of this class

        :param path: new path

        :return: new instance of new path
        :rtype: "SmartPath"
        """
        return cls(path)

    @classmethod
    def from_uri(cls, path: T.Union[str, "SmartPath", os.PathLike]) -> "SmartPath":
        return cls.from_path(path)

    @cached_property
    def name(self) -> str:
        """
        A string representing the final path component, excluding the drive and root
        """
        parts = self.parts
        if len(parts) == 1 and parts[0] == self.protocol.protocol_name + "://":
            return ""
        return parts[-1]

    @cached_property
    def suffix(self) -> str:
        """The file extension of the final component"""
        name = self.name
        i = name.rfind(".")
        if 0 < i < len(name) - 1:
            return name[i:]
        return ""

    @cached_property
    def suffixes(self) -> T.List[str]:
        """A list of the path’s file extensions"""
        name = self.name
        if name.endswith("."):
            return []
        name = name.lstrip(".")
        return ["." + suffix for suffix in name.split(".")[1:]]

    @cached_property
    def stem(self) -> str:
        """The final path component, without its suffix"""
        name = self.name
        i = name.rfind(".")
        if 0 < i < len(name) - 1:
            return name[:i]
        return name

    # TODO: support fs
    async def is_reserved(self) -> bool:
        return False

    async def is_relative_to(self, *other) -> bool:
        try:
            await self.relative_to(*other)
            return True
        except Exception:
            return False

    async def relative_to(self, *other: str) -> "SmartPath":
        """
        Compute a version of this path relative to the path represented by other.
        If it's impossible, ValueError is raised.
        """
        if not other:
            raise TypeError("need at least one argument")

        other_path = self.from_path(other[0])
        if len(other) > 1:
            other_path = await other_path.joinpath(*other[1:])
        other_path_str = other_path.path_with_protocol
        path = self.path_with_protocol

        if path.startswith(other_path_str):
            relative = path[len(other_path_str) :]
            relative = relative.lstrip("/")
            return type(self)(relative)  # pyre-ignore[19]

        raise ValueError("%r does not start with %r" % (path, other))

    async def with_name(self, name: str) -> "SmartPath":
        """Return a new path with the name changed"""
        path = str(self)
        raw_name = self.name
        return self.from_path(path[: len(path) - len(raw_name)] + name)

    async def with_stem(self, stem: str) -> "SmartPath":
        """Return a new path with the stem changed"""
        return await self.with_name("".join([stem, self.suffix]))

    async def with_suffix(self, suffix: str) -> "SmartPath":
        """Return a new path with the suffix changed"""
        path = str(self)
        raw_suffix = self.suffix
        return self.from_path(path[: len(path) - len(raw_suffix)] + suffix)

    async def relpath(self, start: T.Optional[str] = None):
        """Return the relative path."""
        if start is None:
            raise TypeError("start is required")

        other_path = self.from_path(start).path_with_protocol
        path = self.path_with_protocol

        if path.startswith(other_path):
            relative = path[len(other_path) :]
            relative = relative.lstrip("/")
            return relative

        raise ValueError("%r does not start with %r" % (path, other_path))

    async def is_absolute(self) -> bool:
        return True

    async def is_mount(self) -> bool:
        """Test whether a path is a mount point

        :returns: True if a path is a mount point, else False
        """
        return False

    async def is_socket(self) -> bool:
        """
        Return True if the path points to a Unix socket (or a symbolic link pointing
        to a Unix socket), False if it points to another kind of file.

        False is also returned if the path doesn't exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    async def is_fifo(self) -> bool:
        """
        Return True if the path points to a FIFO (or a symbolic link pointing to a
        FIFO), False if it points to another kind of file.

        False is also returned if the path doesn't exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    async def is_block_device(self) -> bool:
        """
        Return True if the path points to a block device (or a symbolic link pointing
        to a block device), False if it points to another kind of file.

        False is also returned if the path doesn't exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    async def is_char_device(self) -> bool:
        """
        Return True if the path points to a character device (or a symbolic link
        pointing to a character device), False if it points to another kind of file.

        False is also returned if the path doesn't exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    async def abspath(self) -> str:
        """Return a normalized absolute version of the path."""
        return self.path_with_protocol

    async def realpath(self) -> str:
        """Return the canonical path of the path."""
        return self.path_with_protocol

    async def resolve(self, strict=False):
        """Alias of realpath."""
        return self.path_with_protocol

    async def read_bytes(self) -> bytes:
        """Return the binary contents of the pointed-to file as a bytes object"""
        async with await self.open(mode="rb") as f:
            return await f.read()  # pytype: disable=bad-return-type

    async def read_text(self) -> str:
        """Return the decoded contents of the pointed-to file as a string"""
        async with await self.open(mode="r") as f:
            return await f.read()  # pytype: disable=bad-return-type

    async def rglob(self, pattern: str) -> T.List["SmartPath"]:
        """
        This is like calling Path.glob() with "**/" added in front of
        the given relative pattern
        """
        if not pattern:
            pattern = ""
        pattern = "**/" + pattern.lstrip("/")
        return await self.glob(pattern=pattern)

    async def samefile(self, other_path) -> bool:
        """
        Return whether this path points to the same file
        """
        if hasattr(other_path, "protocol"):
            if (
                other_path.protocol.protocol_name != self.protocol.protocol_name
                or other_path.protocol.profile_name != self.protocol.profile_name
            ):
                return False

        stat_result = await self.stat()
        if hasattr(other_path, "stat"):
            other_path_stat = await other_path.stat()
        else:
            other_path_stat = await self.from_path(other_path).stat()

        return (
            stat_result.st_ino == other_path_stat.st_ino
            and stat_result.st_dev == other_path_stat.st_dev
        )

    async def touch(self):
        async with await self.open("w"):
            pass

    async def makedirs(self, exist_ok: bool = False) -> None:
        """
        Recursive directory creation function. Like mkdir(), but makes all
        intermediate-level directories needed to contain the leaf directory.
        """
        await self.mkdir(parents=True, exist_ok=exist_ok)

    async def write_bytes(self, data: bytes):
        """
        Open the file pointed to in bytes mode, write data to it, and close the file
        """
        async with await self.open(mode="wb") as f:
            return await f.write(data)

    async def write_text(self, data: str, encoding=None, errors=None, newline=None):
        """
        Open the file pointed to in text mode, write data to it, and close the file.
        The optional parameters have the same meaning as in open().
        """
        async with await self.open(
            mode="w", encoding=encoding, errors=errors, newline=newline
        ) as f:
            return await f.write(data)

    @cached_property
    def drive(self) -> str:
        return ""

    @cached_property
    def root(self) -> str:
        return self.protocol.protocol_name + "://"

    @cached_property
    def anchor(self) -> str:
        return self.root

    async def joinpath(
        self, *other_paths: T.Union[str, "SmartPath", os.PathLike]
    ) -> "SmartPath":
        """
        Calling this method is equivalent to combining the path
        with each of the other arguments in turn
        """
        if len(other_paths) == 0:
            return self

        first_path = self.path
        if first_path.endswith("/"):
            first_path = first_path[:-1]

        other_paths = list(map(fspath, other_paths))

        last_path = other_paths[-1]
        if last_path.startswith("/"):
            last_path = last_path[1:]

        middle_paths = []
        for other_path in other_paths[:-1]:
            if other_path.startswith("/"):
                other_path = other_path[1:]
            if other_path.endswith("/"):
                other_path = other_path[:-1]
            middle_paths.append(other_path)

        return self.from_path("/".join([first_path, *middle_paths, last_path]))

    @cached_property
    def parts(self) -> T.Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        parts = [self.root]
        path = self.path_without_protocol
        path = path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)

    @cached_property
    def parents(self) -> "URIPathParents":
        """
        An immutable sequence providing access to the logical ancestors of the path
        """
        return URIPathParents(self)

    @cached_property
    def parent(self) -> "SmartPath":
        """The logical parent of the path"""
        if self.path_without_protocol == "/":
            return self
        elif len(self.parents) > 0:
            return self.parents[0]
        return self.from_path("")

    async def is_dir(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory."""
        return await self.protocol.is_dir(followlinks=followlinks)

    async def is_file(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file."""
        return await self.protocol.is_file(followlinks=followlinks)

    async def is_symlink(self) -> bool:
        return False

    async def exists(self, followlinks: bool = False) -> bool:
        """Whether the path points to an existing file or directory."""
        return await self.protocol.exists(followlinks=followlinks)

    async def listdir(self) -> T.List[str]:
        """Return the names of the entries in the directory the path points to."""
        result = []
        async for item in self.iterdir():
            result.append(str(item))
        return result

    async def stat(self, follow_symlinks=True) -> StatResult:
        """Get the status of the path."""
        return await self.protocol.stat(follow_symlinks=follow_symlinks)

    async def lstat(self) -> StatResult:
        """
        Like stat() but, if the path points to a symbolic link,
        return the symbolic link's information rather than its target's.
        """
        return await self.stat(follow_symlinks=False)

    async def match(self, pattern) -> bool:
        """
        Match this path against the provided glob-style pattern.
        Return True if matching is successful, False otherwise
        """
        match = _compile_pattern(pattern)
        for index in range(len(self.parts), 0, -1):
            path = "/".join(self.parts[index:])
            if match(path) is not None:
                return True
        return match(self.path_with_protocol) is not None

    async def remove(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""
        return await self.protocol.remove(missing_ok=missing_ok)

    async def unlink(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""
        return await self.protocol.unlink(missing_ok=missing_ok)

    async def mkdir(
        self, mode=0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a directory."""
        return await self.protocol.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    async def rmdir(self) -> None:
        """Remove (delete) the directory."""
        return await self.protocol.rmdir()

    async def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd: bool = True,
    ) -> T.IO:
        """Open the file with mode."""
        return await self.protocol.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
        )

    async def walk(
        self, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree."""
        async for item in self.protocol.walk(followlinks=followlinks):
            yield item

    async def glob(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> T.List["SmartPath"]:
        """Return files whose paths match the glob pattern."""
        result = []
        async for item in self.iglob(
            pattern=pattern, recursive=recursive, missing_ok=missing_ok
        ):
            result.append(item)
        return result

    async def iglob(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> T.AsyncIterator["SmartPath"]:
        """Return an iterator of files whose paths match the glob pattern."""
        async for path_str in self.protocol.iglob(
            pattern=pattern, recursive=recursive, missing_ok=missing_ok
        ):
            yield self.from_path(path_str)

    async def chmod(self, mode: int, *, follow_symlinks: bool = True):
        raise NotImplementedError(f"'chmod' is unsupported on '{type(self)}'")

    async def lchmod(self, mode: int):
        """
        Like chmod() but, if the path points to a symbolic link, the symbolic
        link's mode is changed rather than its target's.
        """
        return await self.chmod(mode=mode, follow_symlinks=False)

    async def rename(
        self, dst_path: T.Union[str, "SmartPath", os.PathLike], overwrite: bool = True
    ) -> "SmartPath":
        """
        rename file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        result = await self.protocol.rename(
            dst_path=fspath(dst_path), overwrite=overwrite
        )
        return self.from_path(result)

    async def replace(
        self, dst_path: T.Union[str, "SmartPath", os.PathLike], overwrite: bool = True
    ) -> "SmartPath":
        """
        move file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        return await self.rename(dst_path=dst_path, overwrite=overwrite)

    async def symlink(self, dst_path: T.Union[str, "SmartPath", os.PathLike]) -> None:
        """symlink file

        :param dst_path: Given destination path
        :type dst_path: T.Union[str, SmartPath, os.PathLike]
        """
        return await self.protocol.symlink(dst_path=dst_path)

    async def symlink_to(self, target, target_is_directory=False):
        """
        Make this path a symbolic link to target.
        symlink_to's arguments is the reverse of symlink's.
        Target_is_directory's value is ignored, only be compatible with pathlib.Path
        """
        return await self.from_path(target).symlink(dst_path=self.path)

    async def readlink(self) -> "SmartPath":
        """
        Return a new path representing the symbolic link's target.
        """
        result = await self.protocol.readlink()
        return self.from_path(result)

    async def hardlink_to(self, target):
        """
        Make this path a hard link to the same file as target.
        """
        raise NotImplementedError(f"'hardlink_to' is unsupported on '{type(self)}'")

    async def home(self):
        """Return the home directory

        returns: Home directory path
        """
        raise NotImplementedError(f"'home' is unsupported on '{type(self)}'")

    async def group(self):
        """
        Return the name of the group owning the file.
        """
        raise NotImplementedError(f"'group' is unsupported on '{type(self)}'")

    async def expanduser(self):
        """
        Return a new path with expanded ~ and ~user constructs, as returned by
        os.path.expanduser().

        Only fs path support this method.
        """
        raise NotImplementedError(f"'expanduser' is unsupported on '{type(self)}'")

    async def cwd(self) -> "SmartPath":
        """Return current working directory

        returns: Current working directory
        """
        raise NotImplementedError(f"'cwd' is unsupported on '{type(self)}'")

    async def iterdir(self) -> T.AsyncIterator["SmartPath"]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        async for path_str in self.protocol.iterdir():
            yield self.from_path(path_str)

    async def owner(self) -> str:
        """
        Return the name of the user owning the file.
        """
        raise NotImplementedError(f"'owner' is unsupported on '{type(self)}'")

    async def absolute(self) -> "SmartPath":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        result = await self.protocol.absolute()
        return self.from_path(result)

    async def full_match(self, pattern, *, case_sensitive=None):
        """
        Return a function that matches the entire path against the provided
        glob-style pattern.

        :param pattern: The glob-style pattern to match against.
        :type pattern: str
        :param case_sensitive: Whether the matching should be case-sensitive.
            If None, the default behavior of fnmatch is used.
        :type case_sensitive: Optional[bool]
        :return: A function that takes a path string and returns True if it
            matches the pattern, False otherwise.
        :rtype: Callable[[str], bool]
        """
        match = _compile_pattern(pattern, case_sensitive=case_sensitive)
        return match(self.path) is not None

    async def is_junction(self) -> bool:
        """
        Return True if the path points to a Windows junction (or a symbolic link
        pointing to a Windows junction), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False
