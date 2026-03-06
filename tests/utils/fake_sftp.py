import os
import posixpath
import stat
import typing as T
from dataclasses import dataclass

FILE_TYPE_REGULAR = 1
FILE_TYPE_DIRECTORY = 2
FILE_TYPE_SYMLINK = 3


@dataclass
class FakeSFTPAttrs:
    """Simple SFTP attrs model for tests.

    :param type: File type marker.
    :param size: File size.
    :param permissions: POSIX mode bits.
    :param mtime: Modification timestamp.
    :param ctime: Change timestamp.
    """

    type: int
    size: int = 0
    permissions: int = 0
    mtime: float = 0.0
    ctime: float = 0.0


@dataclass
class FakeSFTPName:
    """Simple scandir entry model for tests.

    :param filename: Entry name.
    :param attrs: Entry attrs.
    """

    filename: str
    attrs: FakeSFTPAttrs


class FakeSFTPFile:
    """Fake async SFTP file object used by tests.

    :param client: Fake SFTP client instance.
    :param path: Absolute file path.
    :param mode: Open mode.
    :param encoding: Optional text encoding.
    :param errors: Text decode/encode policy.
    """

    def __init__(
        self,
        client: "FakeSFTPClient",
        path: str,
        mode: str,
        encoding: T.Optional[str],
        errors: str,
    ) -> None:
        """Initialize fake file object.

        :param client: Fake SFTP client.
        :param path: Absolute file path.
        :param mode: Open mode.
        :param encoding: Optional text encoding.
        :param errors: Text decode/encode policy.
        """
        self._client = client
        self._path = path
        self._mode = mode
        self._encoding = encoding or "utf-8"
        self._errors = errors
        self._offset = 0

        if "r" in mode and "w" not in mode and "a" not in mode and "x" not in mode:
            if path not in client.files:
                raise FileNotFoundError(path)
        elif "x" in mode:
            if path in client.files or path in client.symlinks or path in client.dirs:
                raise FileExistsError(path)
            client.files[path] = b""
            client._ensure_parent_dirs(path)
        elif "w" in mode:
            client.files[path] = b""
            client._ensure_parent_dirs(path)
        elif "a" in mode:
            client.files.setdefault(path, b"")
            client._ensure_parent_dirs(path)
            self._offset = len(client.files[path])

    async def read(self, size: int = -1, offset: T.Optional[int] = None):
        """Read file content.

        :param size: Maximum bytes/chars to read.
        :param offset: Optional explicit offset.
        :return: Bytes in binary mode, str in text mode.
        """
        content = self._client.files.get(self._path, b"")
        start = self._offset if offset is None else int(offset)

        if size is None or size < 0:
            chunk = content[start:]
        else:
            chunk = content[start : start + int(size)]

        if offset is None:
            self._offset = start + len(chunk)

        if "b" in self._mode:
            return chunk
        return chunk.decode(self._encoding, errors=self._errors)

    async def write(self, data, offset: T.Optional[int] = None) -> int:
        """Write file content.

        :param data: Bytes or str based on mode.
        :param offset: Optional explicit offset.
        :return: Number of written bytes/chars.
        :rtype: int
        """
        if "b" in self._mode:
            if isinstance(data, bytearray):
                raw = bytes(data)
            elif isinstance(data, bytes):
                raw = data
            else:
                raise TypeError("binary mode requires bytes-like data")
            result_size = len(raw)
        else:
            if not isinstance(data, str):
                raise TypeError("text mode requires string data")
            raw = data.encode(self._encoding, errors=self._errors)
            result_size = len(data)

        current = self._client.files.get(self._path, b"")
        if offset is None:
            if "a" in self._mode:
                start = len(current)
            else:
                start = self._offset
        else:
            start = int(offset)

        if start > len(current):
            current += b"\x00" * (start - len(current))

        end = start + len(raw)
        tail = current[end:] if end < len(current) else b""
        self._client.files[self._path] = current[:start] + raw + tail

        if offset is None:
            self._offset = start + len(raw)

        return result_size

    async def seek(self, offset: int, from_what: int = 0) -> int:
        """Seek stream offset.

        :param offset: Offset value.
        :param from_what: Seek origin.
        :return: New absolute offset.
        :rtype: int
        """
        if from_what == 0:
            target = offset
        elif from_what == 1:
            target = self._offset + offset
        elif from_what == 2:
            target = len(self._client.files.get(self._path, b"")) + offset
        else:
            raise ValueError(f"invalid whence: {from_what!r}")

        if target < 0:
            raise ValueError("negative seek position")

        self._offset = int(target)
        return self._offset

    async def tell(self) -> int:
        """Return current file offset.

        :return: Current offset.
        :rtype: int
        """
        return self._offset

    async def close(self) -> None:
        """Close fake file handle."""
        return None

    def __await__(self) -> T.Generator[T.Any, None, "FakeSFTPFile"]:
        """Support awaiting the opened file object.

        :return: Awaitable yielding the file object itself.
        :rtype: typing.Generator[typing.Any, None, FakeSFTPFile]
        """

        async def _identity() -> "FakeSFTPFile":
            return self

        return _identity().__await__()

    async def __aenter__(self) -> "FakeSFTPFile":
        """Enter async context and return file object.

        :return: Current file object.
        :rtype: FakeSFTPFile
        """
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        """Exit async context and close the file.

        :param exc_type: Exception type.
        :param exc: Exception instance.
        :param tb: Exception traceback.
        :return: False to propagate exceptions.
        :rtype: bool
        """
        _ = exc_type, exc, tb
        await self.close()
        return False


class FakeSFTPClient:
    """Fake asyncssh SFTP client for tests.

    :param home_dir: Home directory path.
    """

    def __init__(self, home_dir: str = "/home/test") -> None:
        """Initialize fake SFTP client storage.

        :param home_dir: Home directory path.
        """
        self.home_dir = posixpath.normpath(home_dir)
        home_parent = posixpath.dirname(self.home_dir)

        self.files: T.Dict[str, bytes] = {}
        self.symlinks: T.Dict[str, str] = {}
        self.dirs = {"/", home_parent, self.home_dir}
        self.copy_calls = 0
        self.put_calls = 0
        self.get_calls = 0

    def _normalize(self, path: T.Union[str, bytes]) -> str:
        """Normalize a path to absolute POSIX path.

        :param path: Input path.
        :return: Absolute normalized path.
        :rtype: str
        """
        text_path = path.decode() if isinstance(path, bytes) else path

        if text_path in ("", "."):
            text_path = self.home_dir
        elif not text_path.startswith("/"):
            text_path = posixpath.join(self.home_dir, text_path)

        normalized = posixpath.normpath(text_path)
        return normalized if normalized != "." else "/"

    def _ensure_parent_dirs(self, path: str) -> None:
        """Ensure parent directories exist in fake store.

        :param path: Target file path.
        """
        parent = posixpath.dirname(path)
        while parent and parent not in self.dirs:
            self.dirs.add(parent)
            if parent == "/":
                break
            parent = posixpath.dirname(parent)

    def _attrs_for_path(self, path: str, follow_symlinks: bool) -> FakeSFTPAttrs:
        """Return attrs for an existing path.

        :param path: Absolute path.
        :param follow_symlinks: Whether to resolve symlink targets.
        :return: Fake attrs.
        :rtype: FakeSFTPAttrs
        """
        if path in self.symlinks:
            if follow_symlinks:
                target = self._normalize(self.symlinks[path])
                return self._attrs_for_path(target, follow_symlinks=True)
            return FakeSFTPAttrs(
                type=FILE_TYPE_SYMLINK,
                size=len(self.symlinks[path]),
                permissions=stat.S_IFLNK | 0o777,
                mtime=1.0,
                ctime=1.0,
            )

        if path in self.dirs:
            return FakeSFTPAttrs(
                type=FILE_TYPE_DIRECTORY,
                size=0,
                permissions=stat.S_IFDIR | 0o755,
                mtime=1.0,
                ctime=1.0,
            )

        if path in self.files:
            return FakeSFTPAttrs(
                type=FILE_TYPE_REGULAR,
                size=len(self.files[path]),
                permissions=stat.S_IFREG | 0o644,
                mtime=1.0,
                ctime=1.0,
            )

        raise FileNotFoundError(path)

    async def realpath(
        self,
        path: T.Union[str, bytes],
        *compose_paths: T.Union[str, bytes],
        check: int = 1,
    ) -> str:
        """Return normalized absolute path.

        :param path: Input path.
        :param compose_paths: Additional path fragments.
        :param check: Unused compatibility argument.
        :return: Normalized absolute path.
        :rtype: str
        """
        _ = check
        current = self._normalize(path)
        for item in compose_paths:
            current = self._normalize(posixpath.join(current, self._normalize(item)))
        if current in self.symlinks:
            return self._normalize(self.symlinks[current])
        return current

    async def stat(self, path, flags=0, *, follow_symlinks: bool = True):
        """Return attrs for a path.

        :param path: Input path.
        :param flags: Unused compatibility argument.
        :param follow_symlinks: Whether to resolve symlink targets.
        :return: Fake attrs.
        :rtype: FakeSFTPAttrs
        """
        _ = flags
        normalized = self._normalize(path)
        return self._attrs_for_path(normalized, follow_symlinks=follow_symlinks)

    async def lstat(self, path, flags=0):
        """Return attrs for a path without following symlinks.

        :param path: Input path.
        :param flags: Unused compatibility argument.
        :return: Fake attrs.
        :rtype: FakeSFTPAttrs
        """
        _ = flags
        normalized = self._normalize(path)
        return self._attrs_for_path(normalized, follow_symlinks=False)

    async def exists(self, path) -> bool:
        """Return whether a path exists.

        :param path: Input path.
        :return: True if path exists.
        :rtype: bool
        """
        normalized = self._normalize(path)
        return (
            normalized in self.files
            or normalized in self.dirs
            or normalized in self.symlinks
        )

    async def isdir(self, path) -> bool:
        """Return whether path is a directory.

        :param path: Input path.
        :return: True for directories.
        :rtype: bool
        """
        normalized = self._normalize(path)
        try:
            attrs = self._attrs_for_path(normalized, follow_symlinks=True)
        except FileNotFoundError:
            return False
        return attrs.type == FILE_TYPE_DIRECTORY

    async def isfile(self, path) -> bool:
        """Return whether path is a regular file.

        :param path: Input path.
        :return: True for files.
        :rtype: bool
        """
        normalized = self._normalize(path)
        try:
            attrs = self._attrs_for_path(normalized, follow_symlinks=True)
        except FileNotFoundError:
            return False
        return attrs.type == FILE_TYPE_REGULAR

    async def islink(self, path) -> bool:
        """Return whether path is a symbolic link.

        :param path: Input path.
        :return: True for symlinks.
        :rtype: bool
        """
        normalized = self._normalize(path)
        return normalized in self.symlinks

    async def mkdir(self, path, attrs=None) -> None:
        """Create a directory.

        :param path: Directory path.
        :param attrs: Unused compatibility argument.
        """
        _ = attrs
        normalized = self._normalize(path)
        if (
            normalized in self.files
            or normalized in self.symlinks
            or normalized in self.dirs
        ):
            raise FileExistsError(normalized)

        parent = posixpath.dirname(normalized)
        if parent not in self.dirs:
            raise FileNotFoundError(parent)

        self.dirs.add(normalized)

    async def makedirs(self, path, attrs=None, exist_ok: bool = False) -> None:
        """Create directories recursively.

        :param path: Directory path.
        :param attrs: Unused compatibility argument.
        :param exist_ok: Ignore existing directory.
        """
        _ = attrs
        normalized = self._normalize(path)
        if normalized in self.files or normalized in self.symlinks:
            raise FileExistsError(normalized)
        if normalized in self.dirs and not exist_ok:
            raise FileExistsError(normalized)

        segments = normalized.strip("/").split("/") if normalized != "/" else []
        current = "/"
        for segment in segments:
            current = posixpath.join(current, segment)
            self.dirs.add(current)

    async def remove(self, path) -> None:
        """Remove a non-directory path.

        :param path: Target path.
        """
        normalized = self._normalize(path)
        if normalized in self.files:
            del self.files[normalized]
            return
        if normalized in self.symlinks:
            del self.symlinks[normalized]
            return
        if normalized in self.dirs:
            raise IsADirectoryError(normalized)
        raise FileNotFoundError(normalized)

    async def unlink(self, path) -> None:
        """Alias for remove.

        :param path: Target path.
        """
        await self.remove(path)

    async def rmdir(self, path) -> None:
        """Remove an empty directory.

        :param path: Target directory.
        """
        normalized = self._normalize(path)
        if normalized not in self.dirs:
            raise FileNotFoundError(normalized)

        for entry in list(self.files) + list(self.symlinks) + list(self.dirs):
            if entry != normalized and entry.startswith(normalized.rstrip("/") + "/"):
                raise OSError("directory not empty")

        if normalized != "/":
            self.dirs.remove(normalized)

    async def rmtree(self, path, ignore_errors: bool = False, onerror=None) -> None:
        """Remove directory tree recursively.

        :param path: Root directory.
        :param ignore_errors: Ignore missing root errors.
        :param onerror: Unused compatibility callback.
        """
        _ = onerror
        normalized = self._normalize(path)
        if normalized not in self.dirs:
            if ignore_errors:
                return
            raise FileNotFoundError(normalized)

        prefix = normalized.rstrip("/") + "/"

        for key in list(self.files):
            if key.startswith(prefix):
                del self.files[key]
        for key in list(self.symlinks):
            if key.startswith(prefix):
                del self.symlinks[key]
        for key in sorted(list(self.dirs), key=len, reverse=True):
            if key != normalized and key.startswith(prefix):
                self.dirs.remove(key)

        if normalized != "/":
            self.dirs.remove(normalized)

    async def scandir(self, path="."):
        """Yield direct children for a directory.

        :param path: Directory path.
        :return: Async iterator of directory entries.
        """
        normalized = self._normalize(path)
        if normalized in self.symlinks:
            normalized = self._normalize(self.symlinks[normalized])
        if normalized not in self.dirs:
            raise NotADirectoryError(normalized)

        prefix = normalized.rstrip("/") + "/"
        child_names: T.Dict[str, FakeSFTPAttrs] = {}

        for directory in self.dirs:
            if not directory.startswith(prefix):
                continue
            remaining = directory[len(prefix) :]
            if not remaining or "/" in remaining:
                continue
            child_names[remaining] = self._attrs_for_path(
                directory,
                follow_symlinks=False,
            )

        for file_path in self.files:
            if not file_path.startswith(prefix):
                continue
            remaining = file_path[len(prefix) :]
            if "/" in remaining:
                continue
            child_names[remaining] = self._attrs_for_path(
                file_path,
                follow_symlinks=False,
            )

        for link_path in self.symlinks:
            if not link_path.startswith(prefix):
                continue
            remaining = link_path[len(prefix) :]
            if "/" in remaining:
                continue
            child_names[remaining] = self._attrs_for_path(
                link_path,
                follow_symlinks=False,
            )

        for name in sorted(child_names):
            yield FakeSFTPName(filename=name, attrs=child_names[name])

    async def rename(self, oldpath, newpath, flags: int = 0) -> None:
        """Rename file, symlink, or directory.

        :param oldpath: Source path.
        :param newpath: Destination path.
        :param flags: Unused compatibility argument.
        """
        _ = flags
        old_norm = self._normalize(oldpath)
        new_norm = self._normalize(newpath)

        if not await self.exists(old_norm):
            raise FileNotFoundError(old_norm)
        if await self.exists(new_norm):
            raise FileExistsError(new_norm)

        self._ensure_parent_dirs(new_norm)

        if old_norm in self.files:
            self.files[new_norm] = self.files.pop(old_norm)
            return

        if old_norm in self.symlinks:
            self.symlinks[new_norm] = self.symlinks.pop(old_norm)
            return

        prefix_old = old_norm.rstrip("/") + "/"
        prefix_new = new_norm.rstrip("/") + "/"

        moved_dirs = [
            path
            for path in self.dirs
            if path == old_norm or path.startswith(prefix_old)
        ]
        moved_files = {
            path: self.files[path] for path in self.files if path.startswith(prefix_old)
        }
        moved_links = {
            path: self.symlinks[path]
            for path in self.symlinks
            if path.startswith(prefix_old)
        }

        for path in moved_dirs:
            self.dirs.remove(path)
        for path in moved_files:
            del self.files[path]
        for path in moved_links:
            del self.symlinks[path]

        for path in moved_dirs:
            suffix = path[len(old_norm) :]
            self.dirs.add(new_norm + suffix)
        for path, data in moved_files.items():
            suffix = path[len(prefix_old) :]
            self.files[prefix_new + suffix] = data
        for path, target in moved_links.items():
            suffix = path[len(prefix_old) :]
            self.symlinks[prefix_new + suffix] = target

    async def copy(
        self,
        srcpaths,
        dstpath=None,
        *,
        preserve: bool = False,
        recurse: bool = False,
        follow_symlinks: bool = False,
        sparse: bool = True,
        block_size: int = -1,
        max_requests: int = -1,
        progress_handler=None,
        error_handler=None,
        remote_only: bool = False,
    ) -> None:
        """Copy remote file to remote file.

        :param srcpaths: Source remote path.
        :param dstpath: Destination remote path.
        :param preserve: Unused compatibility argument.
        :param recurse: Unused compatibility argument.
        :param follow_symlinks: Whether to follow symbolic links.
        :param sparse: Unused compatibility argument.
        :param block_size: Unused compatibility argument.
        :param max_requests: Unused compatibility argument.
        :param progress_handler: Optional progress callback.
        :param error_handler: Optional error callback.
        :param remote_only: Unused compatibility argument.
        """
        _ = (
            preserve,
            recurse,
            sparse,
            block_size,
            max_requests,
            error_handler,
            remote_only,
        )
        if dstpath is None:
            raise ValueError("dstpath is required")

        src = self._normalize(srcpaths)
        dst = self._normalize(dstpath)

        if src in self.symlinks and follow_symlinks:
            src = self._normalize(self.symlinks[src])

        if src in self.dirs:
            raise IsADirectoryError(src)
        if src not in self.files:
            raise FileNotFoundError(src)

        self._ensure_parent_dirs(dst)
        data = self.files[src]
        self.files[dst] = data
        self.copy_calls += 1

        if progress_handler is not None:
            progress_handler(src.encode(), dst.encode(), len(data), len(data))

    async def symlink(self, oldpath, newpath) -> None:
        """Create symbolic link.

        :param oldpath: Link target path.
        :param newpath: Link path.
        """
        new_norm = self._normalize(newpath)
        if await self.exists(new_norm):
            raise FileExistsError(new_norm)

        self._ensure_parent_dirs(new_norm)
        self.symlinks[new_norm] = self._normalize(oldpath)

    async def put(
        self,
        localpaths,
        remotepath=None,
        *,
        preserve: bool = False,
        recurse: bool = False,
        follow_symlinks: bool = False,
        sparse: bool = True,
        block_size: int = -1,
        max_requests: int = -1,
        progress_handler=None,
        error_handler=None,
    ) -> None:
        """Upload local file to remote path.

        :param localpaths: Local source path.
        :param remotepath: Remote destination path.
        :param preserve: Unused compatibility argument.
        :param recurse: Unused compatibility argument.
        :param follow_symlinks: Unused compatibility argument.
        :param sparse: Unused compatibility argument.
        :param block_size: Unused compatibility argument.
        :param max_requests: Unused compatibility argument.
        :param progress_handler: Optional progress callback.
        :param error_handler: Optional error callback.
        """
        _ = (
            preserve,
            recurse,
            follow_symlinks,
            sparse,
            block_size,
            max_requests,
            error_handler,
        )
        if remotepath is None:
            raise ValueError("remotepath is required")

        src_path = str(localpaths)
        dst = self._normalize(remotepath)
        with open(src_path, "rb") as src_file:
            data = src_file.read()

        self._ensure_parent_dirs(dst)
        self.files[dst] = data
        self.put_calls += 1

        if progress_handler is not None:
            progress_handler(src_path.encode(), dst.encode(), len(data), len(data))

    async def get(
        self,
        remotepaths,
        localpath=None,
        *,
        preserve: bool = False,
        recurse: bool = False,
        follow_symlinks: bool = False,
        sparse: bool = True,
        block_size: int = -1,
        max_requests: int = -1,
        progress_handler=None,
        error_handler=None,
    ) -> None:
        """Download remote file to local path.

        :param remotepaths: Remote source path.
        :param localpath: Local destination path.
        :param preserve: Unused compatibility argument.
        :param recurse: Unused compatibility argument.
        :param follow_symlinks: Whether to follow symbolic links.
        :param sparse: Unused compatibility argument.
        :param block_size: Unused compatibility argument.
        :param max_requests: Unused compatibility argument.
        :param progress_handler: Optional progress callback.
        :param error_handler: Optional error callback.
        """
        _ = (
            preserve,
            recurse,
            sparse,
            block_size,
            max_requests,
            error_handler,
        )
        if localpath is None:
            raise ValueError("localpath is required")

        src = self._normalize(remotepaths)
        if src in self.symlinks and follow_symlinks:
            src = self._normalize(self.symlinks[src])
        if src not in self.files:
            raise FileNotFoundError(src)

        data = self.files[src]
        dst_path = str(localpath)
        parent = os.path.dirname(dst_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(dst_path, "wb") as dst_file:
            dst_file.write(data)

        self.get_calls += 1

        if progress_handler is not None:
            progress_handler(src.encode(), dst_path.encode(), len(data), len(data))

    async def readlink(self, path):
        """Return symbolic link target.

        :param path: Link path.
        :return: Target path.
        :rtype: str
        """
        normalized = self._normalize(path)
        if normalized not in self.symlinks:
            raise FileNotFoundError(normalized)
        return self.symlinks[normalized]

    def open(
        self,
        path,
        pflags_or_mode="r",
        attrs=None,
        encoding: T.Optional[str] = "utf-8",
        errors: str = "strict",
        block_size: int = -1,
        max_requests: int = -1,
    ) -> FakeSFTPFile:
        """Open fake file object.

        :param path: File path.
        :param pflags_or_mode: Open mode.
        :param attrs: Unused compatibility argument.
        :param encoding: Text encoding.
        :param errors: Text error mode.
        :param block_size: Unused compatibility argument.
        :param max_requests: Unused compatibility argument.
        :return: Fake file object.
        :rtype: FakeSFTPFile
        """
        _ = attrs, block_size, max_requests
        normalized = self._normalize(path)
        return FakeSFTPFile(
            self,
            normalized,
            pflags_or_mode,
            encoding=encoding,
            errors=errors,
        )

    def exit(self) -> None:
        """Close fake client."""
        return None


class FakeSSHConnection:
    """Fake SSH connection object for tests."""

    def __init__(self) -> None:
        """Initialize connection state."""
        self.closed = False

    def close(self) -> None:
        """Close connection state."""
        self.closed = True

    async def wait_closed(self) -> None:
        """Await close completion."""
        return None
