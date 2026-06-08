"""HDFS filesystem implementation backed by the sync ``hdfs`` client."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import posixpath
import typing as T
from contextlib import suppress
from functools import lru_cache

import aiofiles
import aiofiles.ospath

from aiomegfile.config import (
    HDFS_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    CaseSensitiveConfigParser,
)
from aiomegfile.errors.hdfs import (
    HdfsConfigError,
    HdfsFileExistsError,
    HdfsFileNotFoundError,
    HdfsInvalidError,
    HdfsIsADirectoryError,
    HdfsSameFileError,
    HdfsUnsupportedError,
    translate_hdfs_error,
)
from aiomegfile.interfaces import (
    Access,
    AioScannableManager,
    AioWritable,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.lib.hdfs_tools import hdfs_api
from aiomegfile.lib.prefetch_reader.hdfs_prefetch_reader import AioHdfsPrefetchReader
from aiomegfile.utils.path import PathLike, copyfileobj, fspath, split_uri

logger = logging.getLogger(__name__)

__all__ = [
    "HdfsFileSystem",
    "get_hdfs_client",
    "get_hdfs_config",
    "is_hdfs",
]

HDFS_USER = "HDFS_USER"
HDFS_URL = "HDFS_URL"
HDFS_ROOT = "HDFS_ROOT"
HDFS_TIMEOUT = "HDFS_TIMEOUT"
HDFS_TOKEN = "HDFS_TOKEN"  # nosec B105
HDFS_CONFIG_PATH = "HDFS_CONFIG_PATH"
HDFS_DEFAULT_TIMEOUT = 10


def _build_hdfs_error(message: str) -> Exception:
    """Create an HDFS-flavored configuration error.

    :param message: Error message.
    :return: Instantiated exception.
    :rtype: Exception
    """
    return HdfsConfigError(message)


def _coerce_timeout(value: T.Any) -> int:
    """Return timeout value coerced to integer seconds.

    :param value: Raw timeout value.
    :return: Parsed timeout.
    :rtype: int
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return HDFS_DEFAULT_TIMEOUT


def _join_hdfs_path(base_path: str, name: str) -> str:
    """Join an HDFS path and a child name.

    :param base_path: Base HDFS path without protocol.
    :param name: Child entry name.
    :return: Joined HDFS path without protocol.
    :rtype: str
    """
    if not base_path:
        return name
    if base_path == "/":
        return f"/{name}"
    return f"{base_path.rstrip('/')}/{name}"


def _make_stat_result(stat_data: T.Mapping[str, T.Any]) -> StatResult:
    """Convert HDFS status mapping into ``StatResult``.

    :param stat_data: Raw HDFS status mapping.
    :return: Converted stat result.
    :rtype: StatResult
    """
    mtime = float(stat_data.get("modificationTime", 0) or 0) / 1000
    return StatResult(
        st_size=int(stat_data.get("length", 0) or 0),
        st_ctime=mtime,
        st_mtime=mtime,
        isdir=str(stat_data.get("type", "")).upper() == "DIRECTORY",
        islnk=False,
        extra=dict(stat_data),
    )


def is_hdfs(path: PathLike) -> bool:
    """Return whether the given path is an HDFS URI.

    :param path: Path to be tested.
    :return: True if path is an HDFS URI.
    :rtype: bool
    """
    protocol, _, _ = split_uri(fspath(path))
    return protocol == "hdfs"


def get_hdfs_config(profile_name: T.Optional[str] = None) -> T.Dict[str, T.Any]:
    """Load HDFS configuration from environment variables and config file.

    :param profile_name: Optional HDFS profile name.
    :return: HDFS client configuration dictionary.
    :rtype: dict[str, typing.Any]
    :raises Exception: If required HDFS config is missing.
    """
    env_profile = f"{profile_name.upper()}__" if profile_name else ""
    config: T.Dict[str, T.Any] = {
        "user": os.getenv(f"{env_profile}{HDFS_USER}"),
        "url": os.getenv(f"{env_profile}{HDFS_URL}"),
        "root": os.getenv(f"{env_profile}{HDFS_ROOT}"),
        "timeout": os.getenv(f"{env_profile}{HDFS_TIMEOUT}"),
        "token": os.getenv(f"{env_profile}{HDFS_TOKEN}"),
    }

    config_path = os.path.expanduser(os.getenv(HDFS_CONFIG_PATH) or "~/.hdfscli.cfg")
    parser = CaseSensitiveConfigParser()
    if os.path.exists(config_path):
        parser.read(config_path)
        if (
            not profile_name
            and parser.has_section("global")
            and parser.has_option(
                "global",
                "default.alias",
            )
        ):
            profile_name = parser.get("global", "default.alias")
        if profile_name:
            for suffix in (".alias", "_alias"):
                section = f"{profile_name}{suffix}"
                if not parser.has_section(section):
                    continue
                for key, current_value in list(config.items()):
                    if current_value not in (None, ""):
                        continue
                    if parser.has_option(section, key):
                        config[key] = parser.get(section, key)
                break

    config["timeout"] = _coerce_timeout(config.get("timeout"))

    if config.get("url"):
        return config

    raise _build_hdfs_error(
        'Config error, please set environments or use "amf config hdfs ..."'
    )


@lru_cache()
def get_hdfs_client(profile_name: T.Optional[str] = None) -> T.Any:
    """Create or reuse a cached sync HDFS client.

    :param profile_name: Optional HDFS profile name.
    :return: HDFS client instance.
    :rtype: typing.Any
    :raises ImportError: If the optional ``hdfs`` dependency is unavailable.
    """
    if hdfs_api is None or not hasattr(hdfs_api, "InsecureClient"):
        raise HdfsConfigError("hdfs not found, please `pip install 'aiomegfile[hdfs]'`")

    config = {
        key: value
        for key, value in get_hdfs_config(profile_name).items()
        if value not in (None, "")
    }
    if config.get("token"):
        config.pop("user", None)
        return hdfs_api.TokenClient(**config)
    config.pop("token", None)
    return hdfs_api.InsecureClient(**config)


class AioHdfsWritableFile(AioWritable[T.AnyStr]):
    """Async writable HDFS file wrapper around the sync client.

    :param filesystem: Owning HDFS filesystem instance.
    :param path: HDFS path without protocol.
    :param mode: File mode.
    :param buffering: Writer buffer size hint.
    :param encoding: Text encoding in text mode.
    :param errors: Text error handling mode.
    """

    def __init__(
        self,
        filesystem: "HdfsFileSystem",
        path: str,
        mode: str,
        buffering: int,
        encoding: T.Optional[str],
        errors: T.Optional[str],
    ) -> None:
        """Initialize writable HDFS file adapter.

        :param filesystem: Owning filesystem instance.
        :param path: HDFS path without protocol.
        :param mode: File mode.
        :param buffering: Writer buffer size hint.
        :param encoding: Text encoding in text mode.
        :param errors: Text error handling strategy.
        """
        self._filesystem = filesystem
        self._path = path
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding or "utf-8"
        self._errors = errors or "strict"

        self._writer_context = None
        self._file = None
        self._offset = 0

    @property
    def name(self) -> str:
        """Return full URI of the opened file.

        :return: Full HDFS URI.
        :rtype: str
        """
        return self._filesystem.build_uri(self._path)

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: File mode.
        :rtype: str
        """
        return self._mode

    async def __aenter__(self):
        """Enter async context and open the HDFS writer.

        :return: Opened writable HDFS file.
        :rtype: AioHdfsWritableFile
        """

        def _open_writer():
            """Open the underlying sync HDFS writer.

            :return: Tuple of context manager and opened file object.
            :rtype: tuple[typing.Any, typing.Any]
            """
            append_mode = "a" in self._mode
            path_exists = bool(
                self._filesystem._client.status(self._path, strict=False)
            )
            writer_context = self._filesystem._client.write(
                self._path,
                overwrite=not append_mode or not path_exists,
                append=append_mode and path_exists,
                buffersize=None if self._buffering in (-1, None) else self._buffering,
                encoding=(None if "b" in self._mode else self._encoding),
            )
            file_obj = writer_context.__enter__()
            return writer_context, file_obj

        try:
            self._writer_context, self._file = await asyncio.to_thread(_open_writer)
        except Exception as error:
            translated = translate_hdfs_error(error, self.name)
            raise translated from error

        return self

    def _ensure_open(self) -> None:
        """Validate the HDFS file is still open.

        :raises IOError: If file has been closed.
        """
        if self.closed:
            raise IOError(f"file already closed: {self.name!r}")
        if self._file is None:
            raise IOError(f"file not open: {self.name!r}")

    async def write(self, data: T.AnyStr) -> int:
        """Write data to the HDFS file.

        :param data: Data to write.
        :return: Number of written bytes or characters.
        :rtype: int
        """
        self._ensure_open()

        if "b" in self._mode and not isinstance(data, (bytes, bytearray)):
            raise HdfsInvalidError("a bytes-like object is required, not 'str'")
        if "b" not in self._mode and not isinstance(data, str):
            raise HdfsInvalidError("write() argument must be str in text mode")

        try:
            written = await asyncio.to_thread(self._file.write, data)
        except Exception as error:
            translated = translate_hdfs_error(error, self.name)
            raise translated from error

        if written is None:
            written = len(data)
        self._offset += int(written)
        return int(written)

    async def flush(self) -> None:
        """Flush pending writes when the writer exposes ``flush``."""
        self._ensure_open()
        flush_func = getattr(self._file, "flush", None)
        if flush_func is None:
            return None
        try:
            await asyncio.to_thread(flush_func)
        except Exception as error:
            translated = translate_hdfs_error(error, self.name)
            raise translated from error
        return None

    async def tell(self) -> int:
        """Return the current stream position.

        :return: Current stream position.
        :rtype: int
        """
        self._ensure_open()
        tell_func = getattr(self._file, "tell", None)
        if tell_func is None:
            return self._offset
        try:
            return int(await asyncio.to_thread(tell_func))
        except Exception:
            return self._offset

    async def close(self) -> None:
        """Close the writer and finalize the HDFS upload context."""
        if self._writer_context is not None:
            try:
                await asyncio.to_thread(self._writer_context.__exit__, None, None, None)
            except Exception as error:
                translated = translate_hdfs_error(error, self.name)
                raise translated from error
            finally:
                self._writer_context = None
                self._file = None


class HdfsFileSystem(BaseFileSystem):
    """Filesystem implementation for HDFS URIs."""

    protocol = "hdfs"

    def __init__(self, profile_name: T.Optional[str] = None) -> None:
        """Create an HDFS filesystem instance.

        :param profile_name: Optional HDFS profile name.
        """
        self._profile_name = profile_name
        if profile_name:
            self._protocol_with_profile = f"{self.protocol}+{profile_name}"
        else:
            self._protocol_with_profile = self.protocol

    @property
    def _client(self) -> T.Any:
        """Return cached sync HDFS client for the current profile.

        :return: HDFS client instance.
        :rtype: typing.Any
        """
        return get_hdfs_client(self._profile_name)

    async def exists(self, path: str, followlinks: bool = True) -> bool:
        """Return whether the path points to an existing file or directory.

        :param path: HDFS path without protocol.
        :param followlinks: Ignored because HDFS symlinks are unsupported.
        :return: True if the path exists, otherwise False.
        :rtype: bool
        """
        _ = followlinks

        def _status() -> T.Optional[T.Mapping[str, T.Any]]:
            """Fetch status using non-strict mode.

            :return: HDFS status mapping or ``None`` when missing.
            :rtype: typing.Optional[typing.Mapping[str, typing.Any]]
            """
            return self._client.status(path, strict=False)

        try:
            return bool(await asyncio.to_thread(_status))
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            raise translated from error

    async def stat(self, path: str, followlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param path: HDFS path without protocol.
        :param followlinks: Ignored because HDFS symlinks are unsupported.
        :return: Populated stat result.
        :rtype: StatResult
        """
        _ = followlinks

        def _status() -> T.Mapping[str, T.Any]:
            """Fetch strict status mapping from HDFS.

            :return: HDFS status mapping.
            :rtype: typing.Mapping[str, typing.Any]
            """
            return self._client.status(path)

        try:
            stat_data = await asyncio.to_thread(_status)
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            raise translated from error
        return _make_stat_result(stat_data)

    async def is_dir(self, path: str, followlinks: bool = True) -> bool:
        """Return True if the path points to a directory.

        :param path: HDFS path without protocol.
        :param followlinks: Ignored because HDFS symlinks are unsupported.
        :return: True if the path is a directory, otherwise False.
        :rtype: bool
        """
        _ = followlinks
        try:
            return (await self.stat(path)).is_dir()
        except FileNotFoundError:
            return False

    async def is_file(self, path: str, followlinks: bool = True) -> bool:
        """Return True if the path points to a regular file.

        :param path: HDFS path without protocol.
        :param followlinks: Ignored because HDFS symlinks are unsupported.
        :return: True if the path is a file, otherwise False.
        :rtype: bool
        """
        _ = followlinks
        try:
            return (await self.stat(path)).is_file()
        except FileNotFoundError:
            return False

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove a file or directory recursively.

        :param path: HDFS path without protocol.
        :param missing_ok: Whether to ignore missing targets.
        """

        def _delete() -> None:
            """Delete HDFS path synchronously."""
            self._client.delete(path, recursive=True)

        try:
            await asyncio.to_thread(_delete)
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            if missing_ok and isinstance(translated, FileNotFoundError):
                return
            raise translated from error

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param path: HDFS path without protocol.
        :param mode: Permission bits for the new directory.
        :param parents: Ignored for compatibility with pathlib semantics.
        :param exist_ok: Whether to ignore if the directory exists.
        """
        _ = parents
        if not exist_ok and await self.exists(path):
            raise HdfsFileExistsError(f"File exists: {self.build_uri(path)!r}")

        def _makedirs() -> None:
            """Create HDFS directory synchronously."""
            self._client.makedirs(path, permission=mode)

        try:
            await asyncio.to_thread(_makedirs)
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            raise translated from error

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        **kwargs: T.Any,
    ) -> T.AsyncContextManager:
        """Open an HDFS file with the requested mode.

        :param path: HDFS path without protocol.
        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding in text mode.
        :param errors: Error handling strategy.
        :param newline: Newline handling policy.
        :param kwargs: Extra open options for compatibility with megfile.
        :return: Async file context manager.
        :rtype: typing.AsyncContextManager
        :raises HdfsInvalidError: If an unacceptable mode is provided.
        """
        if "+" in mode:
            raise HdfsInvalidError(f"unacceptable mode: {mode!r}")

        atomic = bool(kwargs.pop("atomic", False))
        max_buffer_size = int(kwargs.pop("max_buffer_size", READER_MAX_BUFFER_SIZE))
        block_forward = kwargs.pop("block_forward", None)
        block_size = int(kwargs.pop("block_size", READER_BLOCK_SIZE))
        max_retries = int(kwargs.pop("max_retries", HDFS_MAX_RETRY_TIMES))
        _ = kwargs

        normalized_mode = mode.replace("t", "")
        if normalized_mode not in {"r", "rb", "w", "wb", "a", "ab"}:
            raise HdfsInvalidError(f"unacceptable mode: {mode!r}")

        if "b" in normalized_mode:
            encoding = None
        elif encoding is None:
            encoding = "utf-8"

        if atomic:
            logger.warning(
                "`atomic` parameter in HdfsFileSystem.open is not supported yet. "
                "The parameter will be ignored."
            )

        if normalized_mode in {"r", "rb"}:
            return AioHdfsPrefetchReader(
                path,
                filesystem=self,
                mode="rb" if "b" in normalized_mode else "r",
                encoding=encoding,
                errors=errors,
                newline=newline,
                block_size=block_size,
                max_buffer_size=max_buffer_size,
                block_forward=block_forward,
                max_retries=max_retries,
            )

        return AioHdfsWritableFile(
            self,
            path,
            normalized_mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
        )

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an async context manager for iterating directory entries.

        :param path: HDFS directory path without protocol.
        :return: Async context manager producing ``FileEntry`` items.
        :rtype: typing.AsyncContextManager[typing.AsyncIterator[FileEntry]]
        """
        uri = self.build_uri(path)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            """Yield directory entries from HDFS.

            :return: Async iterator of ``FileEntry`` objects.
            :rtype: typing.AsyncIterator[FileEntry]
            """

            def _list_entries() -> T.List[T.Tuple[str, T.Mapping[str, T.Any]]]:
                """List HDFS directory entries synchronously.

                :return: Sorted directory entries with status.
                :rtype: list[tuple[str, typing.Mapping[str, typing.Any]]]
                """
                entries = self._client.list(path, status=True)
                return sorted(entries, key=lambda item: item[0])

            try:
                entries = await asyncio.to_thread(_list_entries)
            except Exception as error:
                translated = translate_hdfs_error(error, uri)
                raise translated from error

            for name, stat_data in entries:
                yield FileEntry(
                    name=name,
                    path=_join_hdfs_path(path, name),
                    stat=_make_stat_result(stat_data),
                )

        return AioScannableManager(aiterator())

    def scanfile(
        self,
        path: str,
        sort: bool = False,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Iteratively traverse only files under the given path.

        :param path: HDFS path without protocol.
        :param sort: Compatibility flag for protocol-aligned scanfile APIs.
        :return: Async context manager yielding file entries.
        :rtype: typing.AsyncContextManager[typing.AsyncIterator[FileEntry]]
        """
        _ = sort
        uri = self.build_uri(path)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            """Yield file entries under the HDFS path.

            :return: Async iterator of ``FileEntry`` objects.
            :rtype: typing.AsyncIterator[FileEntry]
            """
            if await self.is_file(path):
                stat_result = await self.stat(path)
                yield FileEntry(
                    name=posixpath.basename(path.rstrip("/")) or path,
                    path=path,
                    stat=stat_result,
                )
                return

            def _walk_files() -> T.List[T.Tuple[str, T.Mapping[str, T.Any]]]:
                """Walk HDFS tree synchronously and collect file entries.

                :return: Sorted list of file paths and stats.
                :rtype: list[tuple[str, typing.Mapping[str, typing.Any]]]
                """
                results: T.List[T.Tuple[str, T.Mapping[str, T.Any]]] = []
                for root_info, _dir_infos, file_infos in self._client.walk(
                    path,
                    status=True,
                    ignore_missing=True,
                ):
                    root_path = (
                        root_info[0] if isinstance(root_info, tuple) else root_info
                    )
                    for filename, stat_data in sorted(
                        file_infos,
                        key=lambda item: item[0],
                    ):
                        results.append(
                            (_join_hdfs_path(root_path, filename), stat_data)
                        )
                return results

            try:
                entries = await asyncio.to_thread(_walk_files)
            except Exception as error:
                translated = translate_hdfs_error(error, uri)
                raise translated from error

            for file_path, stat_data in entries:
                yield FileEntry(
                    name=posixpath.basename(file_path.rstrip("/")),
                    path=file_path,
                    stat=_make_stat_result(stat_data),
                )

        iterator = aiterator()

        async def aexit(exc_type, exc_value, traceback) -> None:
            """Close the async iterator if supported."""
            with suppress(Exception):
                await iterator.aclose()

        return AioScannableManager(iterator, aexit)

    async def upload(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Upload a local file into HDFS.

        :param src_path: Local source file path.
        :param dst_path: Destination HDFS path without protocol.
        :param callback: Optional callback receiving transferred byte counts.
        """
        if await aiofiles.ospath.isdir(src_path):
            raise IsADirectoryError(f"Is a directory: {src_path!r}")

        async with aiofiles.open(src_path, "rb") as src_file:
            async with self.open(dst_path, "wb") as dst_file:
                await copyfileobj(src_file, dst_file, callback=callback)

    async def download(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Download an HDFS file into the local filesystem.

        :param src_path: Source HDFS path without protocol.
        :param dst_path: Local destination file path.
        :param callback: Optional callback receiving transferred byte counts.
        """
        if await self.is_dir(src_path):
            raise HdfsIsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        dir_path = os.path.dirname(dst_path)
        if dir_path and dir_path != ".":
            os.makedirs(dir_path, exist_ok=True)

        async with self.open(src_path, "rb") as src_file:
            async with aiofiles.open(dst_path, "wb") as dst_file:
                await copyfileobj(src_file, dst_file, callback=callback)

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> str:
        """Copy a single file inside HDFS.

        :param src_path: Source HDFS path without protocol.
        :param dst_path: Destination HDFS path without protocol.
        :param callback: Optional callback receiving transferred byte counts.
        :return: Destination path.
        :rtype: str
        """
        if await self.samefile(src_path, dst_path):
            raise HdfsSameFileError(
                f"src and dst are the same file: {self.build_uri(src_path)!r}"
            )
        if not await self.exists(src_path):
            raise HdfsFileNotFoundError(f"No such file: {self.build_uri(src_path)!r}")
        if await self.is_dir(src_path):
            raise HdfsIsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")
        if await self.is_dir(dst_path):
            raise HdfsIsADirectoryError(f"Is a directory: {self.build_uri(dst_path)!r}")

        parent_path = posixpath.dirname(dst_path)
        if parent_path and parent_path not in {"", "."}:
            await self.mkdir(parent_path, parents=True, exist_ok=True)

        await self.remove(dst_path, missing_ok=True)
        async with self.open(src_path, "rb") as src_file:
            async with self.open(dst_path, "wb") as dst_file:
                await copyfileobj(src_file, dst_file, callback=callback)
        return dst_path

    async def move(self, src_path: str, dst_path: str) -> str:
        """Move a file or directory to another HDFS path.

        :param src_path: Source HDFS path without protocol.
        :param dst_path: Destination HDFS path without protocol.
        :return: Destination path.
        :rtype: str
        """
        if await self.exists(dst_path):
            await self.remove(dst_path, missing_ok=True)

        parent_path = posixpath.dirname(dst_path)
        if parent_path and parent_path not in {"", "."}:
            await self.mkdir(parent_path, parents=True, exist_ok=True)

        def _rename() -> None:
            """Rename HDFS path synchronously."""
            self._client.rename(src_path, dst_path)

        try:
            await asyncio.to_thread(_rename)
        except Exception as error:
            translated = translate_hdfs_error(
                error,
                f"{self.build_uri(src_path)!r} or {self.build_uri(dst_path)!r}",
            )
            raise translated from error
        return dst_path

    async def md5(
        self,
        path: str,
        recalculate: bool = False,
        followlinks: bool = False,
    ) -> str:
        """Return MD5 checksum for a file or directory.

        :param path: HDFS path without protocol.
        :param recalculate: Ignored for compatibility.
        :param followlinks: Ignored because HDFS symlinks are unsupported.
        :return: MD5 hex digest.
        :rtype: str
        """
        _ = recalculate
        _ = followlinks

        if await self.is_dir(path):
            hash_md5 = hashlib.md5()  # nosec
            async with self.scandir(path) as iterator:
                async for entry in iterator:
                    child_md5 = await self.md5(entry.path)
                    hash_md5.update(child_md5.encode())
            return hash_md5.hexdigest()

        def _checksum() -> str:
            """Read checksum from HDFS synchronously.

            :return: File checksum.
            :rtype: str
            """
            return str(self._client.checksum(path)["bytes"])

        try:
            return await asyncio.to_thread(_checksum)
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            raise translated from error

    async def access(self, path: str, mode: Access = Access.READ) -> bool:
        """Check read/write access heuristically for an HDFS path.

        :param path: HDFS path without protocol.
        :param mode: Access mode enum.
        :return: Whether access is likely available.
        :rtype: bool
        """
        if not isinstance(mode, Access):
            raise HdfsInvalidError(f"unsupported mode type: {type(mode)!r}")
        return await self.exists(path)

    async def absolute(self, path: str) -> str:
        """Make the path absolute without resolving symlinks.

        :param path: HDFS path without protocol.
        :return: Absolute HDFS path without protocol.
        :rtype: str
        """

        def _resolve() -> str:
            """Resolve an HDFS path synchronously.

            :return: Absolute HDFS path.
            :rtype: str
            """
            return str(self._client.resolve(path))

        try:
            resolved = await asyncio.to_thread(_resolve)
        except Exception as error:
            translated = translate_hdfs_error(error, self.build_uri(path))
            raise translated from error
        return "/" + resolved.lstrip("/")

    async def is_absolute(self, path: str) -> bool:
        """Return whether an HDFS path is absolute.

        :param path: HDFS path without protocol.
        :return: True if the path is absolute.
        :rtype: bool
        """
        return path.startswith("/")

    async def samefile(self, path: str, other_path: str) -> bool:
        """Return whether two HDFS paths point to the same file.

        :param path: First HDFS path without protocol.
        :param other_path: Second HDFS path without protocol.
        :return: True if both point to the same file.
        :rtype: bool
        """
        try:
            return await self.absolute(path) == await self.absolute(other_path)
        except FileNotFoundError:
            return False

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Raise because HDFS symlinks are unsupported.

        :param src_path: Source path.
        :param dst_path: Destination path.
        :raises NotImplementedError: Always.
        """
        _ = src_path
        _ = dst_path
        raise HdfsUnsupportedError("'symlink' is unsupported on 'hdfs' protocol")

    async def readlink(self, path: str) -> str:
        """Raise because HDFS symlinks are unsupported.

        :param path: Symlink path.
        :return: Never returns.
        :rtype: str
        :raises NotImplementedError: Always.
        """
        _ = path
        raise HdfsUnsupportedError("'readlink' is unsupported on 'hdfs' protocol")

    async def is_symlink(self, path: str) -> bool:
        """Return False because HDFS symlinks are unsupported.

        :param path: Path to check.
        :return: Always False.
        :rtype: bool
        """
        _ = path
        return False

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """Return whether another filesystem points to the same HDFS endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True when two filesystems share the same HDFS profile/config.
        :rtype: bool
        """
        if not isinstance(other_filesystem, HdfsFileSystem):
            return False
        if self._profile_name == other_filesystem._profile_name:
            return True
        with suppress(Exception):
            return get_hdfs_config(self._profile_name) == get_hdfs_config(
                other_filesystem._profile_name
            )
        return False

    def parse_uri(self, uri: str) -> str:
        """Parse URI into path part without protocol.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        protocol, path, _profile_name = split_uri(uri)
        if protocol == self.protocol:
            return path
        if "://" not in uri:
            return uri
        raise HdfsInvalidError(f"unsupported scheme for hdfs filesystem: {uri!r}")

    def build_uri(self, path: str) -> str:
        """Build URI from path part.

        :param path: Path without protocol.
        :return: Full HDFS URI.
        :rtype: str
        """
        return f"{self._protocol_with_profile}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "HdfsFileSystem":
        """Create filesystem instance from URI.

        :param uri: URI string.
        :return: HdfsFileSystem instance.
        :rtype: HdfsFileSystem
        """
        protocol, _path, profile_name = split_uri(uri)
        if protocol != cls.protocol:
            raise HdfsInvalidError(f"unsupported scheme for hdfs filesystem: {uri!r}")
        return cls(profile_name=profile_name)
