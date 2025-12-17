import asyncio
import os
import typing as T
from collections.abc import Sequence
from functools import cached_property

from aiomegfile.interfaces import StatResult, get_filesystem_by_uri
from aiomegfile.lib.fnmatch import fnmatch, fnmatchcase
from aiomegfile.lib.url import fspath


class URIPathParents(Sequence):
    def __init__(self, path: "SmartPath"):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        self.protocol = path.filesystem.protocol
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
    def __init__(self, uri: T.Union[str, os.PathLike]):
        if isinstance(uri, SmartPath):
            self.filesystem = uri.filesystem
            self._path = uri._path
        else:
            uri = fspath(uri)
            self.filesystem = get_filesystem_by_uri(uri)
            self._path = self.filesystem.get_path_from_uri(uri)

    def __str__(self) -> str:
        return fspath(self)

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        return self.filesystem.generate_uri(self._path)

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

    def __truediv__(self, other_path: T.Union[os.PathLike, str]) -> "SmartPath":
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

    @classmethod
    def from_uri(cls, uri: T.Union[str, os.PathLike]) -> "SmartPath":
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
        if len(parts) == 1 and parts[0] == self.filesystem.protocol + "://":
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

    async def is_relative_to(self, other: T.Union[str, os.PathLike]) -> bool:
        """Return True if this path is relative to the given path.

        :param other: Target path to compare against.
        :return: True if relative, otherwise False.
        """
        try:
            await self.relative_to(other)
            return True
        except Exception:
            return False

    async def relative_to(self, other: T.Union[str, os.PathLike]) -> str:
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
            path = await path.readlink()
            if path == self:
                if strict:
                    raise OSError("Symlink points to itself")
                break
        return await path.absolute()

    async def read_bytes(self) -> bytes:
        """Return the binary contents of the pointed-to file as a bytes object.

        :return: File content in bytes.
        """
        async with self.open(mode="rb") as f:
            return await f.read()  # pytype: disable=bad-return-type

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

    async def samefile(self, other_path: T.Union[str, os.PathLike]) -> bool:
        """
        Return whether this path points to the same file

        :param other_path: Path to compare.
        :return: True if both represent the same file.
        """
        if not isinstance(other_path, SmartPath):
            other_path = self.from_uri(other_path)
        if self.filesystem.protocol != other_path.filesystem.protocol:
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
        return self.filesystem.protocol + "://"

    @cached_property
    def anchor(self) -> str:
        return self.root

    async def joinpath(self, *other_paths: T.Union[str, os.PathLike]) -> "SmartPath":
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
            return self.parents[0]  # pytype: disable=bad-return-type
        return self.from_uri(self.filesystem.generate_uri(""))

    async def is_dir(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a directory, otherwise False.
        """
        return await self.filesystem.is_dir(self._path, followlinks=followlinks)

    async def is_file(self, followlinks: bool = False) -> bool:
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

    async def exists(self, *, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path exists, otherwise False.
        """
        return await self.filesystem.exists(self._path, followlinks=followlinks)

    async def stat(self, *, follow_symlinks=True) -> StatResult:
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
        if not await self.filesystem.is_file(self._path):
            raise IsADirectoryError(f"Is a directory: {fspath(self)}")
        return await self.filesystem.unlink(self._path, missing_ok=missing_ok)

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
        """Remove (delete) the directory.

        :raises NotADirectoryError: If the target is not a directory.
        """
        if not await self.filesystem.is_dir(self._path):
            raise NotADirectoryError(f"Not a directory: {fspath(self)}")
        return await self.filesystem.rmdir(self._path)

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
        :param encoding: Text encoding in text mode.
        :param errors: Error handling strategy.
        :param newline: Newline handling policy in text mode.
        """
        return self.filesystem.open(
            self._path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    async def walk(
        self, follow_symlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param follow_symlinks: Whether to traverse symbolic links to directories.
        :return: Async iterator of (root, dirs, files).
        """
        async for item in self.filesystem.walk(self._path, followlinks=follow_symlinks):
            yield item

    async def iglob(self, pattern: str) -> T.AsyncIterator["SmartPath"]:
        """Return an iterator of files whose paths match the glob pattern.

        :param pattern: Glob pattern to match relative to this path.
        :return: Async iterator of matching SmartPath objects.
        """
        # TODO: implement iglob
        raise NotImplementedError("iglob is not implemented")
        yield  # to make it an async generator

    async def glob(self, pattern: str) -> T.List["SmartPath"]:
        """Return files whose paths match the glob pattern.

        :param pattern: Glob pattern to match relative to this path.
        :return: List of matching SmartPath instances.
        """
        result = []
        async for item in self.iglob(pattern=pattern):
            result.append(item)
        return result

    async def rglob(self, pattern: str) -> T.List["SmartPath"]:
        """
        This is like calling ``Path.glob()`` with ``**/`` added in front of
        the given relative pattern

        :param pattern: Glob pattern to match recursively.
        :return: List of matching SmartPath instances.
        """
        if not pattern:
            pattern = ""
        pattern = "**/" + pattern.lstrip("/")
        return await self.glob(pattern=pattern)

    async def _copy_file(self, target: T.Union[str, os.PathLike]) -> "SmartPath":
        """
        copy file only

        :param target: Given destination path
        :return: Target SmartPath.
        """
        target_path = self.from_uri(target)

        if target_path.filesystem.same_endpoint(self.filesystem):
            await self.filesystem.copy(
                src_path=self._path,
                dst_path=target_path._path,
            )
            return target_path

        if self.filesystem.protocol == "file":
            try:
                await target_path.filesystem.upload(
                    src_path=self._path,
                    dst_path=target_path._path,
                )
                return target_path
            except NotImplementedError:
                pass

        if target_path.filesystem.protocol == "file":
            try:
                await self.filesystem.download(
                    src_path=self._path,
                    dst_path=target_path._path,
                )
                return target_path
            except NotImplementedError:
                pass

        async with self.open("rb") as src_file:
            async with target_path.open("wb") as dst_file:
                while True:
                    chunk = await src_file.read(16 * 1024)  # 16KB, same as shutil
                    if not chunk:
                        break
                    await dst_file.write(chunk)

        return target_path

    async def copy(
        self,
        target: T.Union[str, os.PathLike],
        *,
        follow_symlinks: bool = False,
    ) -> "SmartPath":
        """
        copy file, if self is directory, copy directory

        :param target: Given destination path
        :param follow_symlinks: whether or not follow symbolic link
        :return: Target SmartPath.
        """

        if follow_symlinks:
            src_path = await self.resolve()
            return await src_path.copy(target=target, follow_symlinks=False)

        target_path = self.from_uri(target)

        if await self.is_dir():
            await target_path.mkdir(parents=True, exist_ok=True)
            async for root, _, files in self.walk(follow_symlinks=follow_symlinks):
                for filename in files:
                    current_src = os.path.join(root, filename)
                    current_src_path = self.from_uri(
                        self.filesystem.generate_uri(current_src)
                    )
                    relative_path = await current_src_path.relative_to(self)
                    current_target_path = await target_path.joinpath(relative_path)
                    await current_target_path.parent.mkdir(parents=True, exist_ok=True)
                    await current_src_path._copy_file(target=current_target_path)
            return target_path

        await self._copy_file(target=target_path)
        return target_path

    async def copy_into(
        self,
        target_dir: T.Union[str, os.PathLike],
        *,
        follow_symlinks: bool = False,
    ) -> "SmartPath":
        """
        copy file or directory into dst directory

        :param target_dir: Given destination path
        :param follow_symlinks: whether or not follow symbolic link
        :return: Target SmartPath.
        """
        target = await self.from_uri(target_dir).joinpath(self.name)
        await target.parent.mkdir(parents=True, exist_ok=True)
        await self.copy(target=target, follow_symlinks=follow_symlinks)
        return target

    async def rename(self, target: T.Union[str, os.PathLike]) -> "SmartPath":
        """
        rename file

        :param target: Given destination path
        :return: Target SmartPath after rename.
        :raises FileExistsError: If destination exists.
        """
        result = await self.filesystem.move(
            self._path, dst_path=fspath(target), overwrite=False
        )
        return self.from_uri(result)

    async def replace(self, target: T.Union[str, os.PathLike]) -> "SmartPath":
        """
        move file

        :param target: Given destination path
        :return: Destination SmartPath after replace.
        """
        result = await self.filesystem.move(
            self._path, dst_path=fspath(target), overwrite=True
        )
        return self.from_uri(result)

    async def move(
        self,
        target: T.Union[str, os.PathLike],
    ) -> "SmartPath":
        """
        move file

        :param target: Given destination path
        :return: Destination SmartPath after move.
        """
        return await self.replace(target=target)

    async def move_into(
        self,
        target_dir: T.Union[str, os.PathLike],
    ) -> "SmartPath":
        """
        move file or directory into dst directory

        :param target_dir: Given destination path
        :return: Destination SmartPath inside the target directory.
        """
        target = await self.from_uri(target_dir).joinpath(self.name)
        await target.parent.mkdir(parents=True, exist_ok=True)
        await self.move(target=target)
        return target

    async def symlink_to(self, target: T.Union[str, os.PathLike]) -> None:
        """
        Make this path a symbolic link to target.
        symlink_to's arguments is the reverse of symlink's.

        :param target: Destination the new link should point to.
        """
        return await self.filesystem.symlink(
            src_path=self.from_uri(target)._path,
            dst_path=self._path,
        )

    async def readlink(self) -> "SmartPath":
        """
        Return a new path representing the symbolic link's target.
        """
        result = await self.filesystem.readlink(self._path)
        return self.from_uri(result)

    async def hardlink_to(self, target: T.Union[str, os.PathLike]) -> None:
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
        async for path_str in self.filesystem.iterdir(self._path):
            yield self.from_uri(path_str)

    async def absolute(self) -> "SmartPath":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object

        :return: Absolute SmartPath without symlink resolution.
        """
        result = await self.filesystem.absolute(self._path)
        return self.from_uri(result)

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
