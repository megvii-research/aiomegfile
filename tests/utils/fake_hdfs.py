"""Fake sync HDFS client used by tests."""

from __future__ import annotations

import hashlib
import io
import posixpath
import typing as T


class FakeHdfsError(Exception):
    """Minimal HDFS-like error carrying ``message`` and ``status_code``."""

    def __init__(
        self,
        message: str = "",
        status_code: T.Optional[int] = None,
    ) -> None:
        """Initialize the fake HDFS error.

        :param message: Error message.
        :param status_code: Optional HTTP-like status code.
        """
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class FakeHdfsReadContext:
    """Context manager returning an in-memory readable object."""

    def __init__(self, data: bytes) -> None:
        """Initialize the read context.

        :param data: Bytes to expose.
        """
        self._buffer = io.BytesIO(data)

    def __enter__(self) -> io.BytesIO:
        """Enter the context and return the buffer.

        :return: Readable in-memory buffer.
        :rtype: io.BytesIO
        """
        return self._buffer

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Close the buffer on context exit."""
        self._buffer.close()


class FakeHdfsWriteContext:
    """Context manager collecting writes before committing to fake storage."""

    def __init__(
        self,
        client: "FakeHdfsClient",
        path: str,
        *,
        append: bool,
        encoding: T.Optional[str],
    ) -> None:
        """Initialize the write context.

        :param client: Owning fake client.
        :param path: Target HDFS path without protocol.
        :param append: Whether to append to existing content.
        :param encoding: Optional text encoding.
        """
        self._client = client
        self._path = path
        self._append = append
        self._encoding = encoding
        self._buffer: T.Union[io.BytesIO, io.StringIO, None] = None

    def __enter__(self) -> T.Union[io.BytesIO, io.StringIO]:
        """Enter the context and return a writable buffer.

        :return: Writable in-memory buffer.
        :rtype: typing.Union[io.BytesIO, io.StringIO]
        """
        absolute_path = self._client._to_absolute(self._path)
        existing = self._client.files.get(absolute_path, b"") if self._append else b""
        if self._encoding is None:
            self._buffer = io.BytesIO(existing)
            if self._append:
                self._buffer.seek(0, io.SEEK_END)
        else:
            text = existing.decode(self._encoding)
            self._buffer = io.StringIO(text)
            if self._append:
                self._buffer.seek(0, io.SEEK_END)
        return self._buffer

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Commit buffered content into fake storage when no exception occurred."""
        if exc_type is not None or self._buffer is None:
            return

        if isinstance(self._buffer, io.StringIO):
            data = self._buffer.getvalue().encode(self._encoding or "utf-8")
        else:
            data = self._buffer.getvalue()
        self._client._store_file(self._path, data)
        self._buffer.close()


class FakeHdfsClient:
    """Minimal fake HDFS client implementing the methods used by tests."""

    def __init__(self, root: str = "/") -> None:
        """Initialize the fake client.

        :param root: Client root directory.
        """
        normalized_root = posixpath.normpath(root)
        if normalized_root == ".":
            normalized_root = "/"
        self.root = "/" + normalized_root.lstrip("/")
        self.files: T.Dict[str, bytes] = {}
        self.dirs: T.Set[str] = {"/", self.root}
        self.mtime = 1_700_000_000_000

    def _to_absolute(self, path: str) -> str:
        """Resolve client-relative path into an absolute fake HDFS path.

        :param path: HDFS path without protocol.
        :return: Absolute fake HDFS path.
        :rtype: str
        """
        if path in {"", "."}:
            return self.root
        if path.startswith("/"):
            normalized = posixpath.normpath(path)
        else:
            normalized = posixpath.normpath(posixpath.join(self.root, path))
        if normalized == ".":
            normalized = self.root
        return "/" + normalized.lstrip("/")

    def _ensure_parent_dirs(self, path: str) -> None:
        """Ensure parent directories for the given absolute path exist.

        :param path: Absolute fake HDFS path.
        """
        current = posixpath.dirname(path)
        while current and current not in self.dirs:
            self.dirs.add(current)
            if current == "/":
                break
            current = posixpath.dirname(current)
        self.dirs.add("/")

    def _store_file(self, path: str, data: bytes) -> None:
        """Store file content in fake storage.

        :param path: HDFS path without protocol.
        :param data: File content.
        """
        absolute_path = self._to_absolute(path)
        self._ensure_parent_dirs(absolute_path)
        self.files[absolute_path] = data
        self.mtime += 1

    def _status_for_absolute(self, absolute_path: str) -> T.Mapping[str, T.Any]:
        """Build status mapping for a known absolute path.

        :param absolute_path: Absolute fake HDFS path.
        :return: Status mapping.
        :rtype: typing.Mapping[str, typing.Any]
        """
        if absolute_path in self.files:
            return {
                "length": len(self.files[absolute_path]),
                "modificationTime": self.mtime,
                "type": "FILE",
            }
        return {
            "length": 0,
            "modificationTime": self.mtime,
            "type": "DIRECTORY",
        }

    def status(
        self,
        path: str,
        strict: bool = True,
    ) -> T.Optional[T.Mapping[str, T.Any]]:
        """Return file or directory status.

        :param path: HDFS path without protocol.
        :param strict: Whether to raise when the path is missing.
        :return: Status mapping or ``None``.
        :rtype: typing.Optional[typing.Mapping[str, typing.Any]]
        """
        absolute_path = self._to_absolute(path)
        if absolute_path in self.files or absolute_path in self.dirs:
            return self._status_for_absolute(absolute_path)
        if strict:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)
        return None

    def list(
        self,
        path: str,
        status: bool = False,
    ) -> T.Union[T.List[str], T.List[T.Tuple[str, T.Mapping[str, T.Any]]]]:
        """List direct children of a directory.

        :param path: HDFS directory path without protocol.
        :param status: Whether to include status information.
        :return: Child names or ``(name, status)`` pairs.
        """
        absolute_path = self._to_absolute(path)
        if absolute_path in self.files:
            raise FakeHdfsError("Path is not a directory", status_code=400)
        if absolute_path not in self.dirs:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)

        child_names: T.Set[str] = set()
        prefix = "/" if absolute_path == "/" else absolute_path.rstrip("/") + "/"
        for directory in self.dirs:
            if directory == absolute_path or not directory.startswith(prefix):
                continue
            relative = directory[len(prefix) :]
            if relative and "/" not in relative:
                child_names.add(relative)
        for file_path in self.files:
            if not file_path.startswith(prefix):
                continue
            relative = file_path[len(prefix) :]
            if relative and "/" not in relative:
                child_names.add(relative)

        names = sorted(child_names)
        if not status:
            return names
        return [
            (name, self._status_for_absolute(posixpath.join(absolute_path, name)))
            for name in names
        ]

    def walk(
        self,
        path: str,
        status: bool = False,
        ignore_missing: bool = False,
        allow_dir_changes: bool = False,
    ):
        """Walk the fake HDFS directory tree.

        :param path: HDFS path without protocol.
        :param status: Whether to include status information.
        :param ignore_missing: Whether to ignore missing paths.
        :param allow_dir_changes: Unused compatibility flag.
        :return: Iterator of walk tuples.
        """
        _ = allow_dir_changes
        absolute_path = self._to_absolute(path)
        if absolute_path in self.files:
            return iter(())
        if absolute_path not in self.dirs:
            if ignore_missing:
                return iter(())
            raise FakeHdfsError(f"No such file: {path}", status_code=404)

        directories = sorted(
            directory
            for directory in self.dirs
            if directory == absolute_path
            or directory.startswith(absolute_path.rstrip("/") + "/")
        )

        def _iterator():
            """Yield walk entries lazily.

            :return: Iterator of walk tuples.
            """
            for directory in directories:
                dir_infos: T.List[T.Any] = []
                file_infos: T.List[T.Any] = []
                prefix = "/" if directory == "/" else directory.rstrip("/") + "/"
                for child_dir in self.dirs:
                    if child_dir == directory or not child_dir.startswith(prefix):
                        continue
                    relative = child_dir[len(prefix) :]
                    if relative and "/" not in relative:
                        if status:
                            dir_infos.append(
                                (relative, self._status_for_absolute(child_dir))
                            )
                        else:
                            dir_infos.append(relative)
                for file_path in self.files:
                    if not file_path.startswith(prefix):
                        continue
                    relative = file_path[len(prefix) :]
                    if relative and "/" not in relative:
                        if status:
                            file_infos.append(
                                (relative, self._status_for_absolute(file_path))
                            )
                        else:
                            file_infos.append(relative)
                dir_infos.sort(key=lambda item: item[0] if status else item)
                file_infos.sort(key=lambda item: item[0] if status else item)
                if status:
                    yield (
                        (directory, self._status_for_absolute(directory)),
                        dir_infos,
                        file_infos,
                    )
                else:
                    yield (directory, dir_infos, file_infos)

        return _iterator()

    def read(
        self,
        path: str,
        offset: int = 0,
        length: T.Optional[int] = None,
    ) -> FakeHdfsReadContext:
        """Open a readable context for a file range.

        :param path: HDFS file path without protocol.
        :param offset: Start offset.
        :param length: Optional read length.
        :return: Read context manager.
        :rtype: FakeHdfsReadContext
        """
        absolute_path = self._to_absolute(path)
        if absolute_path in self.dirs:
            raise FakeHdfsError("Path is not a file", status_code=400)
        if absolute_path not in self.files:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)
        data = self.files[absolute_path]
        if length is None:
            chunk = data[offset:]
        else:
            chunk = data[offset : offset + length]
        return FakeHdfsReadContext(chunk)

    def write(
        self,
        path: str,
        overwrite: bool = False,
        append: bool = False,
        buffersize: T.Optional[int] = None,
        encoding: T.Optional[str] = None,
        data: T.Optional[T.IO[T.Any]] = None,
    ) -> T.Optional[FakeHdfsWriteContext]:
        """Open or perform a write operation.

        :param path: HDFS file path without protocol.
        :param overwrite: Whether to overwrite existing content.
        :param append: Whether to append to existing content.
        :param buffersize: Unused compatibility option.
        :param encoding: Optional text encoding.
        :param data: Optional already-open file object to copy from.
        :return: Write context when ``data`` is not provided.
        """
        _ = buffersize
        absolute_path = self._to_absolute(path)
        if absolute_path in self.dirs:
            raise FakeHdfsError("Path is not a file", status_code=400)
        if data is not None:
            content = data.read()
            if isinstance(content, str):
                content = content.encode(encoding or "utf-8")
            if absolute_path in self.files and not overwrite and not append:
                raise FakeHdfsError("File already exists", status_code=409)
            if append and absolute_path in self.files:
                content = self.files[absolute_path] + bytes(content)
            self._store_file(path, bytes(content))
            return None
        return FakeHdfsWriteContext(self, path, append=append, encoding=encoding)

    def makedirs(self, path: str, permission: int = 0o777) -> None:
        """Create directories recursively.

        :param path: HDFS directory path without protocol.
        :param permission: Unused compatibility option.
        """
        _ = permission
        absolute_path = self._to_absolute(path)
        current = absolute_path
        while current and current not in self.dirs:
            self.dirs.add(current)
            if current == "/":
                break
            current = posixpath.dirname(current)
        self.dirs.add("/")

    def rename(self, src_path: str, dst_path: str) -> None:
        """Rename a file or directory.

        :param src_path: Source HDFS path without protocol.
        :param dst_path: Destination HDFS path without protocol.
        """
        src_absolute = self._to_absolute(src_path)
        dst_absolute = self._to_absolute(dst_path)
        if src_absolute in self.files:
            data = self.files.pop(src_absolute)
            self._store_file(dst_path, data)
            return
        if src_absolute not in self.dirs:
            raise FakeHdfsError(f"No such file: {src_path}", status_code=404)

        affected_dirs = sorted(
            directory
            for directory in self.dirs
            if directory == src_absolute
            or directory.startswith(src_absolute.rstrip("/") + "/")
        )
        affected_files = sorted(
            file_path
            for file_path in self.files
            if file_path.startswith(src_absolute.rstrip("/") + "/")
        )
        moved_files = {
            (
                dst_absolute
                if not file_path[len(src_absolute) :].lstrip("/")
                else posixpath.join(
                    dst_absolute,
                    file_path[len(src_absolute) :].lstrip("/"),
                )
            ): self.files[file_path]
            for file_path in affected_files
        }

        for directory in affected_dirs:
            self.dirs.discard(directory)
        for file_path in affected_files:
            self.files.pop(file_path)

        for directory in affected_dirs:
            suffix = directory[len(src_absolute) :].lstrip("/")
            target_dir = (
                dst_absolute if not suffix else posixpath.join(dst_absolute, suffix)
            )
            self.dirs.add(target_dir)
        self.files.update(moved_files)

        self._ensure_parent_dirs(dst_absolute)

    def delete(self, path: str, recursive: bool = True) -> None:
        """Delete a file or directory.

        :param path: HDFS path without protocol.
        :param recursive: Whether to delete directories recursively.
        """
        absolute_path = self._to_absolute(path)
        if absolute_path in self.files:
            del self.files[absolute_path]
            return
        if absolute_path not in self.dirs:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)
        if not recursive:
            raise FakeHdfsError(
                "Directory deletion requires recursive=True",
                status_code=400,
            )
        for file_path in list(self.files):
            if file_path.startswith(absolute_path.rstrip("/") + "/"):
                del self.files[file_path]
        for directory in sorted(self.dirs, reverse=True):
            if directory == "/":
                continue
            if directory == absolute_path or directory.startswith(
                absolute_path.rstrip("/") + "/"
            ):
                self.dirs.discard(directory)

    def checksum(self, path: str) -> T.Mapping[str, str]:
        """Return MD5 checksum mapping for a file.

        :param path: HDFS file path without protocol.
        :return: Mapping with the ``bytes`` checksum entry.
        :rtype: typing.Mapping[str, str]
        """
        absolute_path = self._to_absolute(path)
        if absolute_path not in self.files:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)
        return {"bytes": hashlib.md5(self.files[absolute_path]).hexdigest()}  # nosec

    def resolve(self, path: str) -> str:
        """Resolve a client-relative HDFS path into an absolute path.

        :param path: HDFS path without protocol.
        :return: Absolute HDFS path.
        :rtype: str
        """
        absolute_path = self._to_absolute(path)
        if absolute_path not in self.files and absolute_path not in self.dirs:
            raise FakeHdfsError(f"No such file: {path}", status_code=404)
        return absolute_path
