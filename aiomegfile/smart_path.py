import asyncio
import hashlib
import inspect
import io
import os
import typing as T
from collections.abc import Sequence
from functools import cached_property

from aiomegfile.config import (
    DEFAULT_COPY_BUFFER_SIZE,
    DEFAULT_HASH_BUFFER_SIZE,
    GLOBAL_MAX_WORKERS,
)
from aiomegfile.interfaces import Access, FileEntry, StatResult, get_filesystem_by_uri
from aiomegfile.lib.fnmatch import fnmatch, fnmatchcase
from aiomegfile.lib.glob import FSFunc, iglob
from aiomegfile.utils.alias import resolve_alias
from aiomegfile.utils.path import PathLike, fspath


class URIPathParents(Sequence):
    def __init__(self, path: "SmartPath"):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        self.protocol = path.protocol
        parts = path.parts
        if len(parts) > 0 and parts[0] == self.protocol + "://":
            self.prefix = parts[0]
            self.parts = parts[1:]
        else:
            self.prefix = ""
            self.parts = parts

    def __len__(self) -> int:
        if (
            (self.prefix == "" or "://" in self.prefix)
            and len(self.parts) > 0
            and self.parts[0] not in (f"{self.protocol}:///", "/")
        ):
            return len(self.parts)
        return max(len(self.parts) - 1, 0)

    def _get(self, idx: int) -> "SmartPath":
        if idx < 0:
            idx += len(self)
        if idx < 0 or idx >= len(self):
            raise IndexError(idx)

        parent_parts = self.parts[: len(self.parts) - idx - 1]
        if len(parent_parts) > 1:
            other_path = os.path.join(*parent_parts)
        elif len(parent_parts) == 1:
            other_path = parent_parts[0]
        else:
            other_path = ""
        return self.cls(self.prefix + other_path)

    def __getitem__(
        self, idx: T.Union[int, slice]
    ) -> T.Union["SmartPath", T.Tuple["SmartPath", ...]]:
        if isinstance(idx, slice):
            return tuple(self._get(i) for i in range(*idx.indices(len(self))))
        return self._get(idx)


class SmartPath(os.PathLike):
    def __init__(self, uri: PathLike):
        if isinstance(uri, SmartPath):
            self.filesystem = uri.filesystem
            self._path = uri._path
        else:
            uri = fspath(uri)
            unaliased_uri, _ = resolve_alias(uri)
            self.filesystem = get_filesystem_by_uri(uri)
            self._path = self.filesystem.parse_uri(unaliased_uri)

    @property
    def protocol(self) -> str:
        """Return the protocol for this path, preferring aliases when configured.

        :return: Protocol string for display.
        :rtype: str
        """
        alias_info = getattr(self.filesystem, "_alias_info", None)
        if alias_info is not None:
            return alias_info.alias
        return self.filesystem.protocol

    def __str__(self) -> str:
        return fspath(self)

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        return self.filesystem.build_uri(self._path)

    def __hash__(self) -> int:
        return hash(fspath(self))

    def __eq__(self, other_path: T.Union[str, "SmartPath"]) -> bool:
        if isinstance(other_path, str):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            raise TypeError(
                "'==' not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other_path.filesystem.protocol)
            )
        return fspath(self) == fspath(other_path)

    def __lt__(self, other_path: T.Union[str, "SmartPath"]) -> bool:
        if isinstance(other_path, str):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            raise TypeError(
                "'<' not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other_path.filesystem.protocol)
            )
        return fspath(self) < fspath(other_path)

    def __le__(self, other_path: T.Union[str, "SmartPath"]) -> bool:
        if isinstance(other_path, str):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            raise TypeError(
                "'<=' not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other_path.filesystem.protocol)
            )
        return str(self) <= str(other_path)

    def __gt__(self, other_path: T.Union[str, "SmartPath"]) -> bool:
        if isinstance(other_path, str):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            raise TypeError(
                "'>' not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other_path.filesystem.protocol)
            )
        return str(self) > str(other_path)

    def __ge__(self, other_path: T.Union[str, "SmartPath"]) -> bool:
        if isinstance(other_path, str):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            raise TypeError(
                ">= not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other_path.filesystem.protocol)
            )
        return str(self) >= str(other_path)

    def __truediv__(self, other_path: PathLike) -> "SmartPath":
        if isinstance(other_path, SmartPath):
            if self.filesystem.protocol != other_path.filesystem.protocol:
                raise TypeError(
                    "'/' not supported between filesystem of %r and %r"
                    % (self.filesystem.protocol, other_path.filesystem.protocol)
                )

        first_path = fspath(self)
        other_path = fspath(other_path)

        if first_path.endswith("/"):
            first_path = first_path[:-1]
        if other_path.startswith("/"):
            other_path = other_path[1:]

        return self.from_uri("/".join([first_path, other_path]))

    async def as_uri(self) -> str:
        """Return the path with its protocol prefix (e.g., file:///root)."""
        uri = fspath(self)
        if "://" not in uri:
            uri = self.filesystem.protocol + "://" + uri
        return uri

    async def as_posix(self) -> str:
        """Return a string representation of the path with forward slashes (/)"""
        return fspath(self)

    async def expanduser(self) -> "SmartPath":
        """Return a new path with expanded ``~`` and ``~user`` constructs.

        :return: Expanded SmartPath.
        :rtype: SmartPath
        :raises NotImplementedError: If protocol is not ``file``.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'expanduser' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        return self.from_uri(os.path.expanduser(fspath(self)))

    async def home(self) -> "SmartPath":
        """Return the home directory path.

        :return: Home directory SmartPath.
        :rtype: SmartPath
        :raises NotImplementedError: If protocol is not ``file``.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'home' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        return self.from_uri(os.path.expanduser("~"))

    async def cwd(self) -> "SmartPath":
        """Return current working directory.

        :return: Current working directory SmartPath.
        :rtype: SmartPath
        :raises NotImplementedError: If protocol is not ``file``.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'cwd' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        return self.from_uri(os.getcwd())

    @classmethod
    def from_uri(cls, uri: PathLike) -> "SmartPath":
        """Return new instance of this class

        :param uri: new path

        :return: new instance of new path
        :rtype: "SmartPath"
        """
        return cls(uri)

    @cached_property
    def name(self) -> str:
        """
        A string representing the final path component, excluding the drive and root
        """
        parts = self.parts
        if len(parts) == 1 and parts[0] == self.protocol + "://":
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

    async def is_relative_to(self, other: PathLike) -> bool:
        """Return True if this path is relative to the given path.

        :param other: Target path to compare against.
        :return: True if relative, otherwise False.
        """
        try:
            await self.relative_to(other)
            return True
        except Exception:
            return False

    async def relative_to(self, other: PathLike) -> str:
        """
        Compute a version of this path relative to the path represented by other.
        If it's impossible, ValueError is raised.

        :param other: Target path to compute the relative path against.
        :return: Relative path string.
        :raises TypeError: If other is missing.
        :raises ValueError: If this path is not under the given other path.
        """
        if not other:
            raise ValueError("other is required")

        if not isinstance(other, SmartPath):
            other = self.from_uri(other)
        if self.filesystem.protocol != other.filesystem.protocol:
            raise ValueError(
                "'relative_to' not supported between filesystem of %r and %r"
                % (self.filesystem.protocol, other.filesystem.protocol)
            )
        if self.filesystem.same_endpoint(other.filesystem) is False:
            raise ValueError("'relative_to' not supported between different endpoints")
        other_path_str = await other.filesystem.absolute(other._path)
        path = await self.filesystem.absolute(self._path)

        if path.startswith(other_path_str):
            relative = path[len(other_path_str) :]
            relative = relative.lstrip("/")
            return relative

        raise ValueError("%r does not start with %r" % (path, other))

    async def relpath(self, start: PathLike) -> str:
        """Return the relative path from ``start`` to this path.

        :param start: Base path to compute the relative path against.
        :return: Relative path string.
        :rtype: str
        :raises TypeError: If ``start`` is not provided.
        :raises ValueError: If this path is not under ``start``.
        """
        if start is None:
            raise TypeError("start is required")
        return await self.relative_to(start)

    async def is_absolute(self) -> bool:
        """Return True if the path is absolute.

        :return: True if path is absolute, otherwise False.
        :rtype: bool
        """
        if hasattr(self.filesystem, "is_absolute"):
            return await self.filesystem.is_absolute(self._path)
        path_str = fspath(self)
        if "://" in path_str:
            return True
        return os.path.isabs(path_str)

    async def with_name(self, name: str) -> "SmartPath":
        """Return a new path with the name changed.

        :param name: New file or directory name.
        :return: SmartPath with the name changed.
        """
        path = str(self)
        raw_name = self.name
        return self.from_uri(path[: len(path) - len(raw_name)] + name)

    async def with_stem(self, stem: str) -> "SmartPath":
        """Return a new path with the stem changed.

        :param stem: New stem (basename without suffix).
        :return: SmartPath with updated stem.
        """
        return await self.with_name("".join([stem, self.suffix]))

    async def with_suffix(self, suffix: str) -> "SmartPath":
        """Return a new path with the suffix changed.

        :param suffix: New suffix including leading dot.
        :return: SmartPath with the suffix changed.
        """
        path = str(self)
        raw_suffix = self.suffix
        return self.from_uri(path[: len(path) - len(raw_suffix)] + suffix)

    async def resolve(self, strict: bool = False) -> "SmartPath":
        """Alias of realpath.

        :param strict: Whether to raise if a symlink points to itself.
        :return: Resolved absolute SmartPath.
        :raises OSError: If a symlink points to itself and strict is True.
        """
        path = self
        while await path.is_symlink():
            try:
                path = await path.readlink()
            except OSError:
                break
            if path == self:
                if strict:
                    raise OSError("Symlink points to itself")
                break
        return await path.absolute()

    async def realpath(self) -> str:
        """Return the canonical path of the path.

        :return: Canonical path string.
        :rtype: str
        """
        result = await self.resolve()
        return str(result)

    async def read_bytes(self) -> bytes:
        """Return the binary contents of the pointed-to file as a bytes object.

        :return: File content in bytes.
        """
        async with self.open(mode="rb") as f:
            return await f.read()  # pytype: disable=bad-return-type

    async def load(self) -> T.BinaryIO:
        """Read all content in binary into memory.

        Caller is responsible for closing the returned BinaryIO.

        :return: BinaryIO containing file content.
        :rtype: T.BinaryIO
        """
        content = await self.read_bytes()
        return io.BytesIO(content)

    async def save(self, file_object: T.BinaryIO) -> None:
        """Write an opened binary stream to the path.

        The input stream will not be closed.

        :param file_object: Stream to be read.
        """
        async with self.open("wb") as f:
            while True:
                chunk = file_object.read(DEFAULT_COPY_BUFFER_SIZE)
                if not chunk:
                    break
                await f.write(chunk)

    async def read_text(
        self,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
    ) -> str:
        """Return the decoded contents of the pointed-to file as a string.

        :param encoding: Optional text encoding.
        :param errors: Optional error handling strategy.
        :param newline: Optional newline handling policy.
        :return: File content as text.
        """
        async with self.open(
            mode="r", encoding=encoding, errors=errors, newline=newline
        ) as f:
            return await f.read()  # pytype: disable=bad-return-type

    async def samefile(self, other_path: PathLike) -> bool:
        """
        Return whether this path points to the same file

        :param other_path: Path to compare.
        :return: True if both represent the same file.
        """
        if not isinstance(other_path, SmartPath):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
            return False
        elif self.filesystem.same_endpoint(other_path.filesystem) is False:
            return False
        return await self.filesystem.samefile(self._path, other_path=other_path._path)

    async def touch(self, exist_ok: bool = True) -> None:
        """Create the file if missing, optionally raising on existence.

        :param exist_ok: Whether to skip raising if the file already exists.
        """
        if await self.exists():
            if not exist_ok:
                raise FileExistsError(f"File exists: {fspath(self)}")
            return
        async with self.open("w"):
            pass

    async def write_bytes(self, data: bytes):
        """
        Open the file pointed to in bytes mode, write data to it, and close the file

        :param data: Bytes to write to the file.
        :return: Number of bytes written.
        """
        async with self.open(mode="wb") as f:
            return await f.write(data)

    async def write_text(
        self,
        data: str,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
    ):
        """
        Open the file pointed to in text mode, write data to it, and close the file.
        The optional parameters have the same meaning as in open().

        :param data: Text content to write.
        :param encoding: Optional text encoding.
        :param errors: Optional error handling strategy.
        :param newline: Optional newline handling policy.
        :return: Number of characters written.
        """
        async with self.open(
            mode="w", encoding=encoding, errors=errors, newline=newline
        ) as f:
            return await f.write(data)

    @cached_property
    def root(self) -> str:
        """Return the protocol root for this path.

        :return: Protocol root string.
        :rtype: str
        """
        return self.protocol + "://"

    @cached_property
    def anchor(self) -> str:
        return self.root

    async def joinpath(self, *other_paths: PathLike) -> "SmartPath":
        """
        Calling this method is equivalent to combining the path
        with each of the other arguments in turn

        :param other_paths: Additional path components to join.
        :return: A new SmartPath representing the combined path.
        """
        path = self
        for other_path in other_paths:
            path = path / other_path
        return path

    @cached_property
    def parts(self) -> T.Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        parts = []
        path = fspath(self)
        if path.startswith(self.root):
            parts.append(self.root)
            path = path[len(self.root) :]

        if path.startswith("/"):
            if len(parts) == 0:
                parts.append("/")
            else:
                parts[-1] += "/"
        path = path.lstrip("/")

        if path:
            parts.extend([p for p in path.split("/") if p not in {"", "."}])
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
        if self._path in {"", "/"}:
            return self
        elif len(self.parents) > 0:
            return self.parents[0]  # pytype: disable=bad-return-type # pyre-ignore[7]
        return self.from_uri(self.filesystem.build_uri(""))

    async def is_dir(self, followlinks: bool = True) -> bool:
        """Return True if the path points to a directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a directory, otherwise False.
        """
        return await self.filesystem.is_dir(self._path, followlinks=followlinks)

    async def is_file(self, followlinks: bool = True) -> bool:
        """Return True if the path points to a regular file.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a regular file, otherwise False.
        """
        return await self.filesystem.is_file(self._path, followlinks=followlinks)

    async def is_symlink(self) -> bool:
        """Return True if the path points to a symbolic link.

        :return: True if the path is a symlink, otherwise False.
        """
        return await self.filesystem.is_symlink(self._path)

    async def exists(self, *, followlinks: bool = True) -> bool:
        """Return whether the path points to an existing file or directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path exists, otherwise False.
        """
        return await self.filesystem.exists(self._path, followlinks=followlinks)

    async def stat(self, *, follow_symlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param follow_symlinks: Whether to follow symbolic links when resolving.
        :return: StatResult for the path.
        """
        return await self.filesystem.stat(self._path, followlinks=follow_symlinks)

    async def lstat(self) -> StatResult:
        """
        Like stat() but, if the path points to a symbolic link,
        return the symbolic link's information rather than its target's.

        :return: StatResult for the link itself.
        """
        return await self.stat(follow_symlinks=False)

    async def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        """Change the permission bits of the path.

        :param mode: New permission bits.
        :param follow_symlinks: Whether to follow symbolic links.
        :raises NotImplementedError: If protocol is not ``file``.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'chmod' is unsupported on '{self.filesystem.protocol}' protocol"
            )

        await asyncio.to_thread(
            os.chmod, self._path, mode, follow_symlinks=follow_symlinks
        )

    async def lchmod(self, mode: int) -> None:
        """Change permissions of a symbolic link without following it.

        :param mode: New permission bits.
        """
        await self.chmod(mode=mode, follow_symlinks=False)

    async def owner(self) -> str:
        """Return the name of the user owning the file.

        :return: Owner username.
        :rtype: str
        :raises NotImplementedError: If protocol is not ``file`` or platform
            does not support user lookups.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'owner' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        import pathlib

        path = pathlib.Path(self._path)
        return path.owner()

    async def group(self) -> str:
        """Return the name of the group owning the file.

        :return: Group name.
        :rtype: str
        :raises NotImplementedError: If protocol is not ``file`` or platform
            does not support group lookups.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'group' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        import pathlib

        return pathlib.Path(self._path).group()

    async def utime(
        self, atime: T.Union[float, int], mtime: T.Union[float, int]
    ) -> None:
        """Set the access and modified times of the file.

        :param atime: The access time to be set.
        :param mtime: The modification time to be set.
        :raises NotImplementedError: If protocol is not ``file``.
        """
        if self.filesystem.protocol != "file":
            raise NotImplementedError(
                f"'utime' is unsupported on '{self.filesystem.protocol}' protocol"
            )
        await asyncio.to_thread(os.utime, self._path, (atime, mtime))

    async def getmtime(self, *, follow_symlinks: bool = True) -> float:
        """Return the time of last modification of the file as a timestamp."""
        stat_result = await self.stat(follow_symlinks=follow_symlinks)
        return stat_result.st_mtime

    async def getsize(self, *, follow_symlinks: bool = True) -> int:
        """Return the size of the file in bytes."""
        stat_result = await self.stat(follow_symlinks=follow_symlinks)
        return stat_result.st_size

    async def md5(self, recalculate: bool = False, followlinks: bool = False) -> str:
        """Return the MD5 checksum for the path.

        If the filesystem provides an optimized ``md5`` implementation, it will be
        used. Otherwise, this method reads file contents to compute the checksum,
        matching the behavior of megfile's ``FSPath.md5``.

        :param recalculate: Whether to force recalculation when filesystem supports
            cached MD5 metadata.
        :param followlinks: Whether to follow symbolic links when supported.
        :return: MD5 hex digest.
        :rtype: str
        """
        md5_func = getattr(self.filesystem, "md5", None)
        if callable(md5_func):
            result = md5_func(
                self._path,
                recalculate=recalculate,
                followlinks=followlinks,
            )
            if inspect.isawaitable(result):
                return await result
            return T.cast(str, result)
        return await self._default_md5(recalculate=recalculate, followlinks=followlinks)

    async def _default_md5(
        self, recalculate: bool = False, followlinks: bool = False
    ) -> str:
        """Compute MD5 checksum by reading file contents.

        :param recalculate: Unused, kept for compatibility.
        :param followlinks: Unused, kept for compatibility.
        :return: MD5 hex digest.
        :rtype: str
        """
        if await self.is_dir(followlinks=True):
            hash_md5 = hashlib.md5()  # nosec
            names: T.List[str] = []
            async for entry in self.iterdir():
                names.append(entry.name)
            for name in sorted(names):
                child = await self.joinpath(name)
                child_md5 = await child.md5(
                    recalculate=recalculate,
                    followlinks=followlinks,
                )
                hash_md5.update(child_md5.encode())
            return hash_md5.hexdigest()

        hash_md5 = hashlib.md5()  # nosec
        async with self.open("rb") as fileobj:
            while True:
                chunk = await fileobj.read(DEFAULT_HASH_BUFFER_SIZE)
                if not chunk:
                    break
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    async def match(
        self, pattern: str, *, case_sensitive: T.Optional[bool] = None
    ) -> bool:
        """
        Match this path against the provided glob-style pattern.
        Return True if matching is successful, False otherwise.

        This method is similar to ``full_match()``,
        but the recursive wildcard “**” isn’t supported (it acts like non-recursive “*”)

        :param pattern: Glob pattern to match against the full URI.
        :param case_sensitive: Whether matching should be case sensitive.
        :return: True if the path matches the pattern, otherwise False.
        """
        pattern = pattern.replace("**", "*")
        return await self.full_match(pattern=pattern, case_sensitive=case_sensitive)

    async def unlink(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param missing_ok: If False, raise when the path does not exist.
        :raises IsADirectoryError: If the target is a directory.
        """
        if await self.is_dir(followlinks=False):
            raise IsADirectoryError(f"Is a directory: {fspath(self)}")
        return await self.filesystem.remove(self._path, missing_ok=missing_ok)

    async def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a directory.

        :param mode: Permission bits for the new directory.
        :param parents: Whether to create parents as needed.
        :param exist_ok: Whether to ignore if the directory exists.
        """
        return await self.filesystem.mkdir(
            self._path, mode=mode, parents=parents, exist_ok=exist_ok
        )

    async def rmdir(self) -> None:
        """Remove (delete) the empty directory.

        :raises NotADirectoryError: If the target is not a directory.
        """
        if not await self.is_dir():
            raise NotADirectoryError(f"Not a directory: {fspath(self)}")
        async with self.filesystem.scandir(self._path) as iterator:
            async for _ in iterator:
                raise OSError(f"Directory not empty: {fspath(self)}")
        return await self.filesystem.remove(self._path)

    async def remove(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file or directory.

        :param missing_ok: If False, raise when the path does not exist.
        """
        return await self.filesystem.remove(self._path, missing_ok=missing_ok)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        **kwargs: T.Any,
    ) -> T.AsyncContextManager:
        """Open the file with mode.

        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding in text mode.
        :param errors: Error handling strategy.
        :param newline: Newline handling policy in text mode.
        :param kwargs: Extra open options for compatibility with megfile.
        """
        return self.filesystem.open(
            self._path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )

    async def walk(
        self, follow_symlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param follow_symlinks: Whether to traverse symbolic links to directories.
        :return: Async iterator of (root, dirs, files).
        """
        if not await self.filesystem.is_dir(self._path, followlinks=follow_symlinks):
            return

        root = self._path
        if follow_symlinks:
            try:
                root = (await self.readlink())._path  # pytype: disable=attribute-error
            except OSError:
                pass

        pending = [(root, False)]
        while pending:
            root, root_is_symlink = pending.pop()
            if follow_symlinks and root_is_symlink:
                root = (await self.readlink())._path

            dirs: T.List[str] = []
            files: T.List[str] = []
            to_traverse: T.List[T.Tuple[str, bool]] = []

            async with self.filesystem.scandir(root) as iterator:
                entries: T.List[T.List[T.Any]] = []
                symlink_entries: T.List[T.Tuple[int, str]] = []
                async for entry in iterator:
                    entry_path = entry.path
                    is_symlink = entry.is_symlink()
                    is_dir = entry.is_dir()
                    entries.append([entry, entry_path, is_symlink, is_dir])
                    if is_symlink:
                        symlink_entries.append((len(entries) - 1, entry_path))

                if symlink_entries:
                    max_workers = max(GLOBAL_MAX_WORKERS, 1)
                    semaphore = asyncio.Semaphore(max_workers)

                    async def _fetch_symlink_dir(path: str) -> bool:
                        """Resolve whether a symlink points to a directory.

                        :param path: Symlink path to check.
                        :return: True if symlink resolves to a directory.
                        """
                        async with semaphore:
                            return await self.filesystem.is_dir(path, followlinks=True)

                    tasks = [
                        asyncio.create_task(_fetch_symlink_dir(path))
                        for _, path in symlink_entries
                    ]
                    try:
                        results = await asyncio.gather(*tasks)
                    except Exception:
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        raise

                    for (index, _), result in zip(symlink_entries, results):
                        entries[index][3] = result

                for entry, entry_path, is_symlink, is_dir in entries:
                    if is_dir:
                        dirs.append(entry.name)
                        to_traverse.append((entry_path, is_symlink))
                    else:
                        files.append(entry.name)

            yield root, dirs, files

            for entry_path, is_symlink in to_traverse:
                if not follow_symlinks and is_symlink:
                    continue
                pending.append((entry_path, is_symlink))

    async def scan(
        self,
        missing_ok: bool = True,
        followlinks: bool = False,
        sort: bool = False,
    ) -> T.AsyncIterator[str]:
        """
        Iteratively traverse only files in the given directory.
        Every iteration on the generator yields a path string.

        If path is a file path, yields the file only
        If path is a non-existent path, return an empty generator
        If path is a bucket path, return all file paths in the bucket

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError.
        :param followlinks: Whether to follow symbolic links.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :raises FileNotFoundError: If no matches and missing_ok is False.
        :return: Async iterator of file path strings.
        :rtype: T.AsyncIterator[str]
        """
        async for file_entry in self.scan_stat(
            missing_ok=missing_ok,
            followlinks=followlinks,
            sort=sort,
        ):
            yield file_entry.path

    async def scan_stat(
        self,
        missing_ok: bool = True,
        followlinks: bool = False,
        sort: bool = False,
    ) -> T.AsyncIterator[FileEntry]:
        """
        Iteratively traverse only files in the given directory.
        Every iteration on the generator yields a tuple of path string and file stat.

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError.
        :param followlinks: Whether to follow symbolic links.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :raises FileNotFoundError: If no matches and missing_ok is False.
        :return: Async iterator of FileEntry objects.
        :rtype: T.AsyncIterator[FileEntry]
        """

        async def _iter_entries() -> T.AsyncIterator[FileEntry]:

            async with self.filesystem.scanfile(self._path, sort=sort) as iterator:
                async for entry in iterator:
                    if followlinks and entry.is_symlink():
                        resolved_path = await self.filesystem.readlink(entry.path)
                        resolved_name = os.path.basename(resolved_path)
                        resolved_stat = await self.filesystem.stat(
                            resolved_path, followlinks=followlinks
                        )
                        yield FileEntry(
                            name=resolved_name,
                            path=self.filesystem.build_uri(resolved_path),
                            stat=resolved_stat,
                        )
                        continue
                    yield FileEntry(
                        name=entry.name,
                        path=self.filesystem.build_uri(entry.path),
                        stat=entry.stat,
                    )

        iterator = _iter_entries()
        if missing_ok:
            async for entry in iterator:
                yield entry
            return

        try:
            first = await anext(iterator)
        except StopAsyncIteration as exc:
            raise FileNotFoundError(f"No match any file in: {fspath(self)}") from exc
        yield first
        async for entry in iterator:
            yield entry

    async def iglob(
        self,
        pattern: str,
        recursive: bool = True,
        missing_ok: bool = True,
        sort: bool = False,
    ) -> T.AsyncIterator["SmartPath"]:
        """Return an iterator of files whose paths match the glob pattern.

        :param pattern: Glob pattern to match relative to this path.
        :param recursive: If False, `**` will not search directory recursively.
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :return: Async iterator of matching SmartPath objects.
        """

        if hasattr(self.filesystem, "glob_stat"):
            glob_path = self._path
            if pattern:
                glob_path = os.path.join(self._path, pattern)
            iterator = self.filesystem.glob_stat(
                glob_path,
                recursive=recursive,
                missing_ok=missing_ok,
                sort=sort,
            )
            matched = False
            async for file_entry in iterator:
                matched = True
                yield self.from_uri(self.filesystem.build_uri(file_entry.path))
            if not matched and not missing_ok:
                glob_path = self._path
                if pattern:
                    glob_path = os.path.join(self._path, pattern)
                raise FileNotFoundError(
                    f"No match file: {self.filesystem.build_uri(glob_path)}"
                )
            return
        fs_func = FSFunc(
            exists=self.filesystem.exists,
            isdir=self.filesystem.is_dir,
            scandir=self.filesystem.scandir,
        )
        path = self._path
        if pattern:
            path = os.path.join(self._path, pattern)
        matched = False
        async for path in iglob(path, fs=fs_func, recursive=recursive):
            matched = True
            yield self.from_uri(self.filesystem.build_uri(path))
        if not matched and not missing_ok:
            glob_path = self._path
            if pattern:
                glob_path = os.path.join(self._path, pattern)
            raise FileNotFoundError(
                f"No match file: {self.filesystem.build_uri(glob_path)}"
            )

    async def glob_stat(
        self,
        pattern: str,
        recursive: bool = True,
        missing_ok: bool = True,
        sort: bool = False,
    ) -> T.AsyncIterator[FileEntry]:
        """Return entries whose paths match the glob pattern with stats.

        :param pattern: Glob pattern to match relative to this path.
        :param recursive: If False, `**` will not search directory recursively.
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :return: Async iterator of matching FileEntry objects.
        :rtype: T.AsyncIterator[FileEntry]
        :raises FileNotFoundError: If no matches and missing_ok is False.
        """
        if hasattr(self.filesystem, "glob_stat"):
            glob_path = self._path
            if pattern:
                glob_path = os.path.join(self._path, pattern)
            async for file_entry in self.filesystem.glob_stat(
                glob_path,
                recursive=recursive,
                missing_ok=missing_ok,
                sort=sort,
            ):
                entry_path = file_entry.path
                if "://" not in entry_path:
                    entry_path = self.filesystem.build_uri(entry_path)
                yield FileEntry(
                    name=file_entry.name,
                    path=entry_path,
                    stat=file_entry.stat,
                )
            return

        async def _iter_entries() -> T.AsyncIterator[FileEntry]:
            path_iter = self.iglob(pattern=pattern, recursive=recursive, sort=sort)

            async for path_obj in path_iter:
                stat_result = await path_obj.lstat()
                yield FileEntry(
                    name=path_obj.name,
                    path=fspath(path_obj),
                    stat=stat_result,
                )

        iterator = _iter_entries()
        if missing_ok:
            async for entry in iterator:
                yield entry
            return

        try:
            first = await anext(iterator)
        except StopAsyncIteration as exc:
            glob_path = self._path
            if pattern:
                glob_path = os.path.join(self._path, pattern)
            raise FileNotFoundError(
                f"No match file: {self.filesystem.build_uri(glob_path)}"
            ) from exc

        yield first
        async for entry in iterator:
            yield entry

    async def glob(
        self,
        pattern: str,
        recursive: bool = True,
        missing_ok: bool = True,
        sort: bool = False,
    ) -> T.List["SmartPath"]:
        """Return files whose paths match the glob pattern.

        :param pattern: Glob pattern to match relative to this path.
        :param recursive: If False, `**` will not search directory recursively.
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :return: List of matching SmartPath instances.
        """
        result = []
        async for item in self.iglob(
            pattern=pattern,
            recursive=recursive,
            missing_ok=missing_ok,
            sort=sort,
        ):
            result.append(item)
        return result

    async def rglob(
        self, pattern: str, recursive: bool = True, sort: bool = False
    ) -> T.List["SmartPath"]:
        """
        This is like calling ``Path.glob()`` with ``**/`` added in front of
        the given relative pattern

        :param pattern: Glob pattern to match recursively.
        :param recursive: If False, `**` will not search directory recursively.
        :param sort: Whether to request sorted traversal when supported by the
            filesystem.
        :return: List of matching SmartPath instances.
        """
        if not pattern:
            pattern = ""
        pattern = "**/" + pattern.lstrip("/")
        return await self.glob(pattern=pattern, recursive=recursive, sort=sort)

    async def copy_file(
        self,
        target: PathLike,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> "SmartPath":
        """
        copy file only

        :param target: Given destination path
        :param callback: Called periodically during copy with bytes written.
        :return: Target SmartPath.
        """
        target_path = self.from_uri(target)

        if target_path.filesystem.same_endpoint(self.filesystem):
            await self.filesystem.copy(
                src_path=self._path,
                dst_path=target_path._path,
                callback=callback,
            )
            return target_path

        if self.filesystem.protocol == "file":
            try:
                await target_path.filesystem.upload(
                    src_path=self._path,
                    dst_path=target_path._path,
                    callback=callback,
                )
                return target_path
            except NotImplementedError:
                pass

        if target_path.filesystem.protocol == "file":
            try:
                await self.filesystem.download(
                    src_path=self._path,
                    dst_path=target_path._path,
                    callback=callback,
                )
                return target_path
            except NotImplementedError:
                pass

        async with self.open("rb") as src_file:
            async with target_path.open("wb") as dst_file:
                while True:
                    chunk = await src_file.read(DEFAULT_COPY_BUFFER_SIZE)
                    if not chunk:
                        break
                    await dst_file.write(chunk)
                    if callback:
                        callback(len(chunk))

        return target_path

    async def copy(
        self,
        target: PathLike,
        *,
        callback: T.Optional[T.Callable[[int], None]] = None,
        follow_symlinks: bool = True,
    ) -> "SmartPath":
        """
        copy file

        :param target: Given destination path
        :param callback: Called periodically during copy with bytes written.
        :param follow_symlinks: whether or not follow symbolic link
        :return: Target SmartPath.
        """

        if follow_symlinks:
            src_path = await self.resolve()
            return await src_path.copy(
                target=target,
                callback=callback,
                follow_symlinks=False,
            )

        target_path = self.from_uri(target)

        if await self.is_dir(followlinks=follow_symlinks):
            max_workers = max(GLOBAL_MAX_WORKERS, 1)
            semaphore = asyncio.Semaphore(max_workers)
            max_in_flight = max_workers * 2
            copy_tasks: set[asyncio.Task[None]] = set()

            async def _copy_entry(entry: FileEntry) -> None:
                """Copy a single file entry to the target directory.

                :param entry: File entry to copy.
                """
                async with semaphore:
                    current_src = entry.path
                    current_src_path = self.from_uri(
                        self.filesystem.build_uri(current_src)
                    )
                    relative_path = await current_src_path.relative_to(self)
                    current_target_path = await target_path.joinpath(relative_path)
                    await current_target_path.parent.mkdir(parents=True, exist_ok=True)
                    await current_src_path.copy_file(
                        target=current_target_path,
                        callback=callback,
                    )

            async def _drain_copy_tasks(
                tasks: set[asyncio.Task[None]],
            ) -> set[asyncio.Task[None]]:
                """Wait for at least one copy task and propagate errors.

                :param tasks: Active copy tasks.
                :return: Remaining pending tasks.
                """
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for completed_task in done:
                    try:
                        await completed_task
                    except Exception:
                        for pending_task in pending:
                            pending_task.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        raise
                return set(pending)

            async with self.filesystem.scanfile(self._path) as iterator:
                async for file_entry in iterator:
                    copy_tasks.add(asyncio.create_task(_copy_entry(file_entry)))
                    if len(copy_tasks) >= max_in_flight:
                        copy_tasks = await _drain_copy_tasks(copy_tasks)

            if copy_tasks:
                try:
                    await asyncio.gather(*copy_tasks)
                except Exception:
                    for pending_task in copy_tasks:
                        if not pending_task.done():
                            pending_task.cancel()
                    await asyncio.gather(*copy_tasks, return_exceptions=True)
                    raise
            return target_path

        await self.copy_file(target=target_path, callback=callback)
        return target_path

    async def copy_into(
        self,
        target_dir: PathLike,
        *,
        callback: T.Optional[T.Callable[[int], None]] = None,
        follow_symlinks: bool = True,
    ) -> "SmartPath":
        """
        copy file or directory into dst directory

        :param target_dir: Given destination path
        :param callback: Called periodically during copy with bytes written.
        :param follow_symlinks: whether or not follow symbolic link
        :return: Target SmartPath.
        """
        target = await self.from_uri(target_dir).joinpath(self.name)
        await target.parent.mkdir(parents=True, exist_ok=True)
        await self.copy(
            target=target,
            callback=callback,
            follow_symlinks=follow_symlinks,
        )
        return target

    async def _move(self, target: PathLike) -> "SmartPath":
        """
        move file only

        :param target: Given destination path
        :return: Target SmartPath after move.
        """
        target_path = self.from_uri(target)

        if target_path.filesystem.same_endpoint(self.filesystem):
            await self.filesystem.move(self._path, dst_path=target_path._path)
        else:
            await self.copy(target=target_path)
            await self.filesystem.remove(self._path)
        return target_path

    async def rename(self, target: PathLike) -> "SmartPath":
        """
        rename file

        :param target: Given destination path
        :return: Target SmartPath after rename.
        """
        return await self._move(target=target)

    async def replace(self, target: PathLike) -> "SmartPath":
        """
        move file

        :param target: Given destination path
        :return: Destination SmartPath after replace.
        """
        return await self._move(target=target)

    async def move(
        self,
        target: PathLike,
    ) -> "SmartPath":
        """
        move file

        :param target: Given destination path
        :return: Destination SmartPath after move.
        """
        return await self._move(target=target)

    async def move_into(
        self,
        target_dir: PathLike,
    ) -> "SmartPath":
        """
        move file or directory into dst directory

        :param target_dir: Given destination path
        :return: Destination SmartPath inside the target directory.
        """
        target = await self.from_uri(target_dir).joinpath(self.name)
        return await self.move(target=target)

    async def symlink(self, dst_path: PathLike) -> None:
        """Create a symbolic link pointing to this path named ``dst_path``.

        :param dst_path: Path of the symlink to create.
        :raises TypeError: If filesystems differ.
        """
        target_path = self.from_uri(dst_path)
        if not target_path.filesystem.same_endpoint(self.filesystem):
            raise TypeError("'symlink' not supported between different filesystems")
        return await self.filesystem.symlink(
            src_path=self._path,
            dst_path=target_path._path,
        )

    async def symlink_to(
        self, target: PathLike, target_is_directory: bool = False
    ) -> None:
        """
        Make this path a symbolic link to target.
        symlink_to's arguments is the reverse of symlink's.

        :param target: Destination the new link should point to.
        :param target_is_directory: Compatibility argument, ignored.
        """
        _ = target_is_directory
        target_path = self.from_uri(target)
        if not target_path.filesystem.same_endpoint(self.filesystem):
            raise TypeError("'symlink_to' not supported between different filesystems")
        return await self.filesystem.symlink(
            src_path=target_path._path,
            dst_path=self._path,
        )

    async def readlink(self) -> "SmartPath":
        """
        Return a new path representing the symbolic link's target.
        """
        result = await self.filesystem.readlink(self._path)
        return self.from_uri(result)

    async def hardlink_to(self, target: PathLike) -> None:
        """
        Make this path a hard link to the same file as target.

        :param target: Existing path to hard link to.
        :raises NotImplementedError: If protocol does not support hard links.
        """
        if self.filesystem.protocol == "file":
            return await asyncio.to_thread(os.link, target, self._path)
        raise NotImplementedError(
            f"'hardlink_to' is unsupported on '{self.filesystem.protocol}' protocol"
        )

    async def iterdir(self) -> T.AsyncIterator["SmartPath"]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :return: All contents have in the path in ascending alphabetical order
        """
        async with self.filesystem.scandir(self._path) as iterator:
            async for file_entry in iterator:
                path_str = self.filesystem.build_uri(file_entry.path)
                yield self.from_uri(path_str)

    async def listdir(self) -> T.List[str]:
        """Return the names of the entries in the directory this path points to.

        :return: List of entry names.
        :rtype: T.List[str]
        """
        names: T.List[str] = []
        async for entry in self.iterdir():
            names.append(entry.name)
        return names

    def scandir(self) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an async context manager for directory entries.

        :return: Async context manager yielding FileEntry items.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        return self.filesystem.scandir(self._path)

    async def absolute(self) -> "SmartPath":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object

        :return: Absolute SmartPath without symlink resolution.
        """
        result = await self.filesystem.absolute(self._path)
        return self.from_uri(self.filesystem.build_uri(result))

    async def abspath(self) -> str:
        """Return a normalized absolute version of the path.

        :return: Absolute path string.
        :rtype: str
        """
        return str(await self.absolute())

    async def full_match(
        self, pattern: str, *, case_sensitive: T.Optional[bool] = None
    ) -> bool:
        """
        Return a function that matches the entire path against the provided
        glob-style pattern.

        :param pattern: The glob-style pattern to match against.
        :type pattern: str
        :param case_sensitive: Whether the matching should be case-sensitive.
            If None, the default behavior of fnmatch is used.
        :type case_sensitive: Optional[bool]
        :return: Returns True if it matches the pattern, False otherwise.
        :rtype: bool
        """
        path_str = fspath(self)
        if case_sensitive is True:
            return fnmatchcase(path_str, pattern)
        return fnmatch(path_str, pattern)

    async def access(self, mode: Access = Access.READ) -> bool:
        """Test if path has access permission described by mode

        :param mode: access mode, defaults to Access.READ
        :type mode: Access, optional
        :raises NotImplementedError: If the filesystem does not support access checks.
        :return: True if the path has the specified access permission, False otherwise.
        :rtype: bool
        """
        if not hasattr(self.filesystem, "access"):
            raise NotImplementedError(
                f"'access' is not implemented for '{self.filesystem.protocol}' protocol"
            )
        return await self.filesystem.access(self._path, mode=mode)
