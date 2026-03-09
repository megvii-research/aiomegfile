"""Fake async WebDAV client used by WebDAV unit tests."""

from __future__ import annotations

import inspect
import posixpath
import typing as T
import urllib.parse
from datetime import datetime, timezone


class FakeAiodavResponse:
    """Minimal aiohttp-like response used by fake WebDAV client.

    :param body: Response payload bytes.
    :param status: HTTP status code.
    :param headers: Response headers.
    """

    def __init__(self, body: bytes, status: int, headers: dict[str, str]) -> None:
        """Initialize fake response object.

        :param body: Response payload bytes.
        :param status: HTTP status code.
        :param headers: Response headers.
        """
        self._body = body
        self.status = status
        self.headers = headers
        self.released = False

    async def read(self) -> bytes:
        """Return body bytes.

        :return: Payload bytes.
        :rtype: bytes
        """
        return self._body

    def release(self) -> None:
        """Release response resources."""
        self.released = True


class FakeWebdavClient:
    """Fake aiodav client implementing methods used by aiomegfile tests.

    :param chunk_size: Transfer chunk size for upload/download.
    """

    def __init__(self, chunk_size: int = 8) -> None:
        """Initialize in-memory WebDAV storage.

        :param chunk_size: Transfer chunk size for upload/download.
        """
        self.chunk_size = chunk_size
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = {"/"}
        self.closed = False

    def _normalize(self, path: str) -> str:
        """Normalize path to absolute POSIX style path.

        :param path: Input path.
        :return: Absolute normalized path.
        :rtype: str
        """
        decoded = urllib.parse.unquote(path or "/")
        if not decoded.startswith("/"):
            decoded = "/" + decoded
        normalized = posixpath.normpath(decoded)
        return "/" if normalized in ("", ".") else normalized

    def _ensure_parent_dirs(self, path: str) -> None:
        """Ensure parent directories exist.

        :param path: File or directory path.
        """
        parent = posixpath.dirname(path)
        while parent and parent not in self.dirs:
            self.dirs.add(parent)
            if parent == "/":
                break
            parent = posixpath.dirname(parent)

    def _build_modified_time(self) -> str:
        """Return deterministic RFC1123 timestamp string.

        :return: RFC1123 datetime text.
        :rtype: str
        """
        return datetime(2024, 1, 1, tzinfo=timezone.utc).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )

    def _entry_info(self, path: str) -> dict[str, T.Any]:
        """Build metadata dictionary for a path.

        :param path: Absolute normalized path.
        :return: Metadata dictionary.
        :rtype: dict[str, T.Any]
        :raises FileNotFoundError: If path does not exist.
        """
        if path in self.files:
            return {
                "path": path,
                "name": posixpath.basename(path),
                "size": str(len(self.files[path])),
                "modified": self._build_modified_time(),
                "isdir": False,
            }
        if path in self.dirs:
            name = posixpath.basename(path.rstrip("/")) if path != "/" else ""
            return {
                "path": path,
                "name": name,
                "size": "0",
                "modified": self._build_modified_time(),
                "isdir": True,
            }
        raise FileNotFoundError(path)

    async def close(self) -> None:
        """Close fake client."""
        self.closed = True

    async def exists(self, path: str) -> bool:
        """Return whether path exists.

        :param path: Remote path.
        :return: True if path exists.
        :rtype: bool
        """
        normalized = self._normalize(path)
        return normalized in self.files or normalized in self.dirs

    async def is_directory(self, path: str) -> bool:
        """Return whether path is a directory.

        :param path: Remote path.
        :return: True when path is a directory.
        :rtype: bool
        """
        normalized = self._normalize(path)
        if normalized not in self.files and normalized not in self.dirs:
            raise FileNotFoundError(path)
        return normalized in self.dirs

    async def info(self, path: str) -> dict[str, T.Any]:
        """Return metadata info for a path.

        :param path: Remote path.
        :return: Metadata dictionary.
        :rtype: dict[str, T.Any]
        """
        normalized = self._normalize(path)
        return self._entry_info(normalized)

    async def list(
        self,
        path: str = "/",
        get_info: bool = False,
    ) -> list[T.Any]:
        """Return direct children for a directory.

        :param path: Directory path.
        :param get_info: Return metadata dictionaries when True.
        :return: List of names or metadata dictionaries.
        :rtype: list[T.Any]
        """
        normalized = self._normalize(path)
        if normalized not in self.dirs:
            raise FileNotFoundError(path)

        prefix = "/" if normalized == "/" else normalized + "/"
        names: list[str] = []
        infos: list[dict[str, T.Any]] = []

        for child in sorted(self.dirs.union(self.files.keys())):
            if child == normalized:
                continue
            if not child.startswith(prefix):
                continue
            relative = child[len(prefix) :]
            if "/" in relative.rstrip("/"):
                continue
            if not relative:
                continue
            info = self._entry_info(child)
            names.append(relative.rstrip("/"))
            infos.append(info)

        if get_info:
            return infos
        return names

    async def create_directory(self, path: str) -> bool:
        """Create a directory.

        :param path: Directory path.
        :return: Always True on success.
        :rtype: bool
        """
        normalized = self._normalize(path)
        parent = posixpath.dirname(normalized)
        if parent not in self.dirs:
            raise FileNotFoundError(parent)
        if normalized in self.files:
            raise FileExistsError(normalized)
        self.dirs.add(normalized)
        return True

    async def delete(self, path: str) -> None:
        """Delete file or directory recursively.

        :param path: Target path.
        """
        normalized = self._normalize(path)
        if normalized in self.files:
            del self.files[normalized]
            return
        if normalized not in self.dirs:
            raise FileNotFoundError(path)

        dir_prefix = "/" if normalized == "/" else normalized + "/"
        for file_path in list(self.files):
            if file_path.startswith(dir_prefix):
                del self.files[file_path]
        for dir_path in sorted(list(self.dirs), reverse=True):
            if dir_path == "/":
                continue
            if dir_path == normalized or dir_path.startswith(dir_prefix):
                self.dirs.remove(dir_path)

    async def move(
        self,
        source: str,
        destination: str,
        overwrite: bool = False,
    ) -> None:
        """Move file or directory.

        :param source: Source path.
        :param destination: Destination path.
        :param overwrite: Whether to overwrite destination.
        """
        src = self._normalize(source)
        dst = self._normalize(destination)
        if src not in self.files and src not in self.dirs:
            raise FileNotFoundError(source)

        if await self.exists(dst):
            if not overwrite:
                raise FileExistsError(destination)
            await self.delete(dst)

        self._ensure_parent_dirs(dst)
        if src in self.files:
            self.files[dst] = self.files.pop(src)
            return

        src_prefix = "/" if src == "/" else src + "/"
        dst_prefix = "/" if dst == "/" else dst + "/"

        moved_dirs = []
        moved_files: dict[str, bytes] = {}
        for current_dir in list(self.dirs):
            if current_dir == src or current_dir.startswith(src_prefix):
                suffix = current_dir[len(src) :].lstrip("/")
                target_dir = dst if not suffix else f"{dst_prefix}{suffix}"
                moved_dirs.append((current_dir, self._normalize(target_dir)))
        for current_file, data in list(self.files.items()):
            if current_file.startswith(src_prefix):
                suffix = current_file[len(src_prefix) :]
                target_file = f"{dst_prefix}{suffix}"
                moved_files[current_file] = data
                self.files[self._normalize(target_file)] = data

        for old_file in moved_files:
            del self.files[old_file]
        for old_dir, _ in moved_dirs:
            if old_dir != "/":
                self.dirs.discard(old_dir)
        for _, new_dir in moved_dirs:
            self.dirs.add(new_dir)

    async def copy(self, source: str, destination: str, depth: int = 1) -> None:
        """Copy file.

        :param source: Source file path.
        :param destination: Destination file path.
        :param depth: Unused compatibility argument.
        """
        _ = depth
        src = self._normalize(source)
        dst = self._normalize(destination)
        if src not in self.files:
            if src in self.dirs:
                raise IsADirectoryError(source)
            raise FileNotFoundError(source)
        self._ensure_parent_dirs(dst)
        self.files[dst] = self.files[src]

    async def _buffer_read(self, buffer: T.Any, size: int) -> bytes:
        """Read a chunk from sync/async buffer.

        :param buffer: Buffer object with ``read`` method.
        :param size: Max bytes to read.
        :return: Read bytes.
        :rtype: bytes
        """
        chunk = buffer.read(size)
        if inspect.isawaitable(chunk):
            chunk = await chunk
        if chunk is None:
            return b""
        if isinstance(chunk, str):
            return chunk.encode("utf-8")
        return bytes(chunk)

    async def _buffer_write(self, buffer: T.Any, data: bytes) -> None:
        """Write a chunk into sync/async buffer.

        :param buffer: Buffer object with ``write`` method.
        :param data: Bytes to write.
        """
        result = buffer.write(data)
        if inspect.isawaitable(result):
            await result

    async def upload_to(
        self,
        path: str,
        buffer: T.Any,
        buffer_size: T.Optional[int] = None,
        overwrite: bool = True,
        progress: T.Optional[T.Callable[..., T.Any]] = None,
        progress_args: tuple = (),
    ) -> None:
        """Upload buffer data to remote file.

        :param path: Destination file path.
        :param buffer: Source buffer.
        :param buffer_size: Total source size when available.
        :param overwrite: Whether to overwrite existing file.
        :param progress: Optional progress callback.
        :param progress_args: Additional callback arguments.
        """
        remote_path = self._normalize(path)
        parent = posixpath.dirname(remote_path)
        if parent not in self.dirs:
            raise FileNotFoundError(parent)
        if not overwrite and remote_path in self.files:
            return

        if hasattr(buffer, "seek"):
            seek_result = buffer.seek(0)
            if inspect.isawaitable(seek_result):
                await seek_result

        payload = bytearray()
        current = 0
        if callable(progress):
            progress(current, buffer_size or 0, *progress_args)

        while True:
            chunk = await self._buffer_read(buffer, self.chunk_size)
            if not chunk:
                break
            payload.extend(chunk)
            current += len(chunk)
            if callable(progress):
                progress(current, buffer_size or current, *progress_args)

        self.files[remote_path] = bytes(payload)

    async def download_to(
        self,
        path: str,
        buffer: T.Any,
        progress: T.Optional[T.Callable[..., T.Any]] = None,
        progress_args: tuple = (),
    ) -> dict[str, T.Any]:
        """Download remote file into buffer.

        :param path: Source file path.
        :param buffer: Destination buffer.
        :param progress: Optional progress callback.
        :param progress_args: Additional callback arguments.
        :return: Metadata dictionary.
        :rtype: dict[str, T.Any]
        """
        remote_path = self._normalize(path)
        if remote_path not in self.files:
            raise FileNotFoundError(path)
        data = self.files[remote_path]
        total = len(data)
        current = 0

        if callable(progress):
            progress(current, total, *progress_args)

        while current < total:
            chunk = data[current : current + self.chunk_size]
            await self._buffer_write(buffer, chunk)
            current += len(chunk)
            if callable(progress):
                progress(current, total, *progress_args)

        return self._entry_info(remote_path)

    async def _execute_request(
        self,
        action: str,
        path: str,
        data: T.Optional[T.Any] = None,
        headers_ext: T.Optional[dict[str, str]] = None,
    ) -> FakeAiodavResponse:
        """Execute low-level request used by prefetch reader.

        :param action: Request action name.
        :param path: Target path.
        :param data: Unused request payload.
        :param headers_ext: Optional HTTP headers.
        :return: Fake response object.
        :rtype: FakeAiodavResponse
        """
        _ = data
        if action != "download":
            raise NotImplementedError(f"Unsupported action: {action!r}")

        remote_path = self._normalize(path)
        if remote_path not in self.files:
            raise FileNotFoundError(path)
        content = self.files[remote_path]
        headers = {"Accept-Ranges": "bytes"}

        range_header = (headers_ext or {}).get("Range")
        if not range_header:
            headers["Content-Length"] = str(len(content))
            return FakeAiodavResponse(bytes(content), 200, headers)

        prefix = "bytes="
        if not range_header.startswith(prefix):
            return FakeAiodavResponse(b"", 416, headers)

        try:
            start_text, end_text = range_header[len(prefix) :].split("-", 1)
            start = int(start_text)
            end = int(end_text)
        except Exception:
            return FakeAiodavResponse(b"", 416, headers)

        if start < 0 or start >= len(content) or end < start:
            return FakeAiodavResponse(b"", 416, headers)

        end = min(end, len(content) - 1)
        chunk = bytes(content[start : end + 1])
        headers["Content-Length"] = str(len(chunk))
        headers["Content-Range"] = f"bytes {start}-{end}/{len(content)}"
        return FakeAiodavResponse(chunk, 206, headers)
