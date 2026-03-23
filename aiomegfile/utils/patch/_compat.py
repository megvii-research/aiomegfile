"""Megfile-compatible synchronous wrappers built on top of aiomegfile."""

from __future__ import annotations

import os
import typing as T

import asyncssh

from aiomegfile.filesystem.hdfs import is_hdfs as _aio_is_hdfs
from aiomegfile.filesystem.http import is_http as _aio_is_http
from aiomegfile.filesystem.s3 import is_s3 as _aio_is_s3
from aiomegfile.filesystem.sftp import is_sftp as _aio_is_sftp
from aiomegfile.filesystem.webdav import is_webdav as _aio_is_webdav
from aiomegfile.smart import (
    smart_access as _aio_smart_access,
)
from aiomegfile.smart import smart_concat as _aio_smart_concat
from aiomegfile.smart import smart_copy as _aio_smart_copy
from aiomegfile.smart import smart_load_content as _aio_smart_load_content
from aiomegfile.smart import (
    smart_move as _aio_smart_move,
)
from aiomegfile.smart import smart_open as _aio_smart_open
from aiomegfile.smart import (
    smart_rename as _aio_smart_rename,
)
from aiomegfile.smart import smart_scandir as _aio_smart_scandir
from aiomegfile.stdio import is_stdio as _aio_is_stdio
from aiomegfile.stdio import stdio_open as _aio_stdio_open
from aiomegfile.utils.path import fspath, split_uri

from ._sync import (
    _normalize_access_mode,
    _run_coroutine,
    _SyncAsyncFile,
    _SyncAsyncScandir,
    _SyncSmartPath,
)

PathLike = T.Union[str, os.PathLike]

_CopyFunc = T.Callable[
    [PathLike, PathLike, T.Optional[T.Callable[[int], None]], bool, bool],
    T.Any,
]
_COPY_FUNCS: T.Dict[str, T.Dict[str, T.Optional[_CopyFunc]]] = {}


class SmartPath(_SyncSmartPath):
    """Megfile-compatible sync ``SmartPath`` wrapper."""


class FSPath(_SyncSmartPath):
    """Megfile-compatible local filesystem path wrapper."""

    protocol = "file"
    _accepted_protocols = ("file",)
    _default_protocol = "file"
    _path_attr_includes_protocol = False


class HdfsPath(_SyncSmartPath):
    """Megfile-compatible HDFS path wrapper."""

    protocol = "hdfs"
    _accepted_protocols = ("hdfs",)
    _default_protocol = "hdfs"
    _path_attr_includes_protocol = False


class HttpPath(_SyncSmartPath):
    """Megfile-compatible HTTP path wrapper."""

    protocol = "http"
    _accepted_protocols = ("http",)
    _default_protocol = "http"
    _path_attr_includes_protocol = False


class HttpsPath(_SyncSmartPath):
    """Megfile-compatible HTTPS path wrapper."""

    protocol = "https"
    _accepted_protocols = ("https",)
    _default_protocol = "https"
    _path_attr_includes_protocol = False


class S3Path(_SyncSmartPath):
    """Megfile-compatible S3 path wrapper."""

    protocol = "s3"
    _accepted_protocols = ("s3",)
    _default_protocol = "s3"
    _path_attr_includes_protocol = False


class SftpPath(_SyncSmartPath):
    """Megfile-compatible SFTP path wrapper."""

    protocol = "sftp"
    _accepted_protocols = ("sftp",)
    _default_protocol = "sftp"
    _path_attr_includes_protocol = False


class StdioPath(_SyncSmartPath):
    """Megfile-compatible stdio path wrapper."""

    protocol = "stdio"
    _accepted_protocols = ("stdio",)
    _default_protocol = "stdio"
    _path_attr_includes_protocol = False


class WebdavPath(_SyncSmartPath):
    """Megfile-compatible WebDAV path wrapper."""

    protocol = "webdav"
    _accepted_protocols = ("webdav", "webdavs")
    _default_protocol = "webdav"
    _path_attr_includes_protocol = False


def is_fs(path: PathLike) -> bool:
    """Return whether the given path uses the local filesystem protocol.

    :param path: Path to inspect.
    :return: True if the path is a local filesystem path.
    :rtype: bool
    """
    protocol, _, _ = split_uri(fspath(path))
    return protocol == "file"


def is_hdfs(path: PathLike) -> bool:
    """Return whether the given path is an HDFS URI.

    :param path: Path to inspect.
    :return: True when the path is an HDFS URI.
    :rtype: bool
    """
    return _aio_is_hdfs(path)


def is_http(path: PathLike) -> bool:
    """Return whether the given path is an HTTP(S) URL.

    :param path: Path to inspect.
    :return: True when the path is HTTP or HTTPS.
    :rtype: bool
    """
    return _aio_is_http(path)


def is_s3(path: PathLike) -> bool:
    """Return whether the given path is an S3 URI.

    :param path: Path to inspect.
    :return: True when the path is an S3 URI.
    :rtype: bool
    """
    return _aio_is_s3(path)


def is_sftp(path: PathLike) -> bool:
    """Return whether the given path is an SFTP URI.

    :param path: Path to inspect.
    :return: True when the path is an SFTP URI.
    :rtype: bool
    """
    return _aio_is_sftp(path)


def is_stdio(path: PathLike) -> bool:
    """Return whether the given path is a stdio URI.

    :param path: Path to inspect.
    :return: True when the path is a stdio URI.
    :rtype: bool
    """
    return _aio_is_stdio(path)


def is_webdav(path: PathLike) -> bool:
    """Return whether the given path is a WebDAV URI.

    :param path: Path to inspect.
    :return: True when the path is a WebDAV URI.
    :rtype: bool
    """
    return _aio_is_webdav(path)


def get_traditional_path(path: PathLike) -> str:
    """Return the path string without protocol prefix.

    :param path: Input path.
    :return: Traditional path string without protocol.
    :rtype: str
    """
    return SmartPath(path).path_without_protocol


def register_copy_func(
    src_protocol: str,
    dst_protocol: str,
    copy_func: T.Optional[_CopyFunc] = None,
) -> None:
    """Register a custom copy function for a protocol pair.

    :param src_protocol: Source protocol name.
    :param dst_protocol: Destination protocol name.
    :param copy_func: Copy implementation.
    :raises ValueError: If a mapping already exists.
    """
    if src_protocol not in _COPY_FUNCS:
        _COPY_FUNCS[src_protocol] = {}
    if dst_protocol in _COPY_FUNCS[src_protocol]:
        raise ValueError(
            "Copy Function has already existed: {}->{}".format(
                src_protocol, dst_protocol
            )
        )
    _COPY_FUNCS[src_protocol][dst_protocol] = copy_func


def smart_access(path: PathLike, mode) -> bool:
    """Return whether the path supports the requested access mode.

    :param path: Path to inspect.
    :param mode: Access enum value.
    :return: True when access is allowed.
    :rtype: bool
    """
    return T.cast(
        bool,
        _run_coroutine(_aio_smart_access(path, _normalize_access_mode(mode))),
    )


def smart_open(
    path: PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    newline: T.Optional[str] = None,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open a file synchronously through aiomegfile.

    :param path: File path to open.
    :param mode: File mode.
    :param buffering: Buffering policy.
    :param encoding: Text encoding.
    :param errors: Error handling strategy.
    :param newline: Newline handling strategy.
    :param kwargs: Extra backend-specific options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _SyncAsyncFile(
        _aio_smart_open(
            path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )
    )


def smart_scandir(path: PathLike) -> _SyncAsyncScandir:
    """Return a synchronous scandir adapter.

    :param path: Directory path to scan.
    :return: Sync scandir adapter.
    :rtype: _SyncAsyncScandir
    """
    return _SyncAsyncScandir(_aio_smart_scandir(path))


def smart_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Copy a path while preserving megfile's ``None`` return contract.

    :param src_path: Source path.
    :param dst_path: Destination path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    copy_func = _get_registered_copy_func(src_path, dst_path)
    if copy_func is not None:
        copy_func(src_path, dst_path, callback, followlinks, overwrite)
        return
    _run_coroutine(
        _aio_smart_copy(
            src_path,
            dst_path,
            callback=callback,
            followlinks=followlinks,
            overwrite=overwrite,
        )
    )


def smart_move(src_path: PathLike, dst_path: PathLike, overwrite: bool = True) -> None:
    """Move a path while preserving megfile's ``None`` return contract.

    :param src_path: Source path.
    :param dst_path: Destination path.
    :param overwrite: Whether to overwrite destination.
    """
    _run_coroutine(_aio_smart_move(src_path, dst_path, overwrite=overwrite))


def smart_rename(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> None:
    """Rename a path while preserving megfile's ``None`` return contract.

    :param src_path: Source path.
    :param dst_path: Destination path.
    :param overwrite: Whether to overwrite destination.
    """
    _run_coroutine(_aio_smart_rename(src_path, dst_path, overwrite=overwrite))


def smart_ismount(path: PathLike) -> bool:
    """Return whether the path is a mount point.

    :param path: Path to inspect.
    :return: True when the path is a mount point.
    :rtype: bool
    """
    return SmartPath(path).is_mount()


def fs_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Copy between local filesystem paths.

    :param src_path: Source path.
    :param dst_path: Destination path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_path, ("file",), "fs_copy source")
    _require_path_protocol(dst_path, ("file",), "fs_copy destination")
    smart_copy(
        src_path,
        dst_path,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def s3_buffered_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the buffered-open compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open(
        "s3_buffered_open",
        s3_url,
        ("s3",),
        mode,
        followlinks,
        kwargs,
    )


def s3_cached_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the cached-open compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open("s3_cached_open", s3_url, ("s3",), mode, followlinks, kwargs)


def s3_concat(src_paths: T.List[PathLike], dst_path: PathLike, **kwargs: T.Any) -> None:
    """Concatenate S3 objects into a destination S3 object.

    :param src_paths: Source S3 paths.
    :param dst_path: Destination S3 path.
    :param kwargs: Extra concat options.
    """
    _require_path_list_protocol(src_paths, ("s3",), "s3_concat sources")
    _require_path_protocol(dst_path, ("s3",), "s3_concat destination")
    _run_coroutine(_aio_smart_concat(src_paths, dst_path, **kwargs))


def s3_copy(
    src_url: PathLike,
    dst_url: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Copy an S3 object to another S3 object.

    :param src_url: Source S3 URI.
    :param dst_url: Destination S3 URI.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_url, ("s3",), "s3_copy source")
    _require_path_protocol(dst_url, ("s3",), "s3_copy destination")
    smart_copy(
        src_url,
        dst_url,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def s3_download(
    src_url: PathLike,
    dst_url: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Download an S3 object to a local path.

    :param src_url: Source S3 URI.
    :param dst_url: Destination local path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_url, ("s3",), "s3_download source")
    _require_path_protocol(dst_url, ("file",), "s3_download destination")
    smart_copy(
        src_url,
        dst_url,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def s3_load_content(
    s3_url: PathLike,
    start: T.Optional[int] = None,
    stop: T.Optional[int] = None,
) -> bytes:
    """Load content from an S3 object.

    :param s3_url: Source S3 URI.
    :param start: Optional start offset.
    :param stop: Optional stop offset.
    :return: Loaded bytes.
    :rtype: bytes
    """
    _require_path_protocol(s3_url, ("s3",), "s3_load_content path")
    return T.cast(bytes, _run_coroutine(_aio_smart_load_content(s3_url, start, stop)))


def s3_memory_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the memory-open compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open("s3_memory_open", s3_url, ("s3",), mode, followlinks, kwargs)


def s3_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open("s3_open", s3_url, ("s3",), mode, followlinks, kwargs)


def s3_pipe_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the pipe-open compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open("s3_pipe_open", s3_url, ("s3",), mode, followlinks, kwargs)


def s3_prefetch_open(
    s3_url: PathLike,
    mode: str = "rb",
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the prefetch-open compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open(
        "s3_prefetch_open",
        s3_url,
        ("s3",),
        mode,
        followlinks,
        kwargs,
    )


def s3_share_cache_open(
    s3_url: PathLike,
    mode: str = "rb",
    followlinks: bool = False,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open an S3 object using the shared-cache compatibility wrapper.

    :param s3_url: S3 URI.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra backend options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _protocol_open(
        "s3_share_cache_open",
        s3_url,
        ("s3",),
        mode,
        followlinks,
        kwargs,
    )


def s3_upload(
    src_url: PathLike,
    dst_url: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Upload a local file to S3.

    :param src_url: Source local path.
    :param dst_url: Destination S3 URI.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_url, ("file",), "s3_upload source")
    _require_path_protocol(dst_url, ("s3",), "s3_upload destination")
    smart_copy(
        src_url,
        dst_url,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def sftp_add_host_key(
    hostname: str,
    port: int = 22,
    prompt: bool = False,
    host_key_path: T.Optional[str] = None,
) -> None:
    """Add an SFTP host key to ``known_hosts`` using AsyncSSH.

    :param hostname: SSH host name.
    :param port: SSH port.
    :param prompt: Whether to ask before writing.
    :param host_key_path: Path to ``known_hosts``.
    """
    if not host_key_path:
        host_key_path = os.path.expanduser("~/.ssh/known_hosts")
    _ensure_known_hosts_file(host_key_path)
    host_pattern = _format_known_host_pattern(hostname, port)

    with open(host_key_path, "r", encoding="utf-8") as handle:
        existing_data = handle.read()
    if host_pattern in existing_data:
        return

    key = _run_coroutine(asyncssh.get_server_host_key(host=hostname, port=port))
    if key is None:
        raise RuntimeError(f"Unable to fetch server host key for {hostname}:{port}")

    public_key = key.export_public_key().decode().strip()
    if prompt and not _prompt_add_to_known_hosts(host_pattern, public_key):
        return

    with open(host_key_path, "a", encoding="utf-8") as handle:
        if existing_data and not existing_data.endswith("\n"):
            handle.write("\n")
        handle.write(f"{host_pattern} {public_key}\n")


def sftp_concat(src_paths: T.List[PathLike], dst_path: PathLike) -> None:
    """Concatenate SFTP files into a destination SFTP file.

    :param src_paths: Source SFTP paths.
    :param dst_path: Destination SFTP path.
    """
    _require_path_list_protocol(src_paths, ("sftp",), "sftp_concat sources")
    _require_path_protocol(dst_path, ("sftp",), "sftp_concat destination")
    _run_coroutine(_aio_smart_concat(src_paths, dst_path))


def sftp_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Copy an SFTP path to another SFTP path.

    :param src_path: Source SFTP path.
    :param dst_path: Destination SFTP path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_path, ("sftp",), "sftp_copy source")
    _require_path_protocol(dst_path, ("sftp",), "sftp_copy destination")
    smart_copy(
        src_path,
        dst_path,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def sftp_download(
    src_url: PathLike,
    dst_url: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Download an SFTP file to a local path.

    :param src_url: Source SFTP path.
    :param dst_url: Destination local path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_url, ("sftp",), "sftp_download source")
    _require_path_protocol(dst_url, ("file",), "sftp_download destination")
    smart_copy(
        src_url,
        dst_url,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def sftp_upload(
    src_url: PathLike,
    dst_url: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """Upload a local file to an SFTP path.

    :param src_url: Source local path.
    :param dst_url: Destination SFTP path.
    :param callback: Progress callback.
    :param followlinks: Whether to follow symlinks.
    :param overwrite: Whether to overwrite destination.
    """
    _require_path_protocol(src_url, ("file",), "sftp_upload source")
    _require_path_protocol(dst_url, ("sftp",), "sftp_upload destination")
    smart_copy(
        src_url,
        dst_url,
        callback=callback,
        followlinks=followlinks,
        overwrite=overwrite,
    )


def stdio_open(
    path: PathLike,
    mode: str = "rb",
    *,
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    **kwargs: T.Any,
) -> _SyncAsyncFile:
    """Open a stdio path synchronously.

    :param path: Stdio path.
    :param mode: File mode.
    :param encoding: Optional text encoding.
    :param errors: Optional text error handler.
    :param kwargs: Extra options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    return _SyncAsyncFile(
        _aio_stdio_open(
            path,
            mode=mode,
            encoding=encoding,
            errors=errors,
            **kwargs,
        )
    )


def _get_registered_copy_func(
    src_path: PathLike, dst_path: PathLike
) -> T.Optional[_CopyFunc]:
    """Return a registered copy function for the given protocol pair.

    :param src_path: Source path.
    :param dst_path: Destination path.
    :return: Registered copy function if present.
    :rtype: Optional[_CopyFunc]
    """
    src_protocol = split_uri(fspath(src_path))[0]
    dst_protocol = split_uri(fspath(dst_path))[0]
    return _COPY_FUNCS.get(src_protocol, {}).get(dst_protocol)


def _require_path_protocol(
    path: PathLike,
    protocols: T.Tuple[str, ...],
    label: str,
) -> None:
    """Validate that a path uses one of the required protocols.

    :param path: Path to validate.
    :param protocols: Accepted protocols.
    :param label: Label used in error messages.
    :raises ValueError: If the path protocol is unsupported.
    """
    protocol = split_uri(fspath(path))[0]
    if protocol not in protocols:
        raise ValueError(
            f"{label} requires protocol in {protocols!r}, got {protocol!r}"
        )


def _require_path_list_protocol(
    paths: T.Iterable[PathLike],
    protocols: T.Tuple[str, ...],
    label: str,
) -> None:
    """Validate protocols for every path in an iterable.

    :param paths: Paths to validate.
    :param protocols: Accepted protocols.
    :param label: Label used in error messages.
    """
    for path in paths:
        _require_path_protocol(path, protocols, label)


def _protocol_open(
    name: str,
    path: PathLike,
    protocols: T.Tuple[str, ...],
    mode: str,
    followlinks: bool,
    kwargs: T.Mapping[str, T.Any],
) -> _SyncAsyncFile:
    """Open a protocol-scoped path using the generic smart open wrapper.

    :param name: Wrapper name for error messages.
    :param path: Input path.
    :param protocols: Accepted protocols.
    :param mode: File mode.
    :param followlinks: Whether to follow symlinks.
    :param kwargs: Extra options.
    :return: Sync file wrapper.
    :rtype: _SyncAsyncFile
    """
    _require_path_protocol(path, protocols, f"{name} path")
    return smart_open(path, mode, followlinks=followlinks, **dict(kwargs))


def _ensure_known_hosts_file(path: str) -> None:
    """Ensure the target ``known_hosts`` file exists with safe permissions.

    :param path: Target file path.
    """
    if os.path.exists(path):
        return
    parent = os.path.dirname(path)
    if parent and parent != ".":
        os.makedirs(parent, exist_ok=True, mode=0o700)
    with open(path, "w", encoding="utf-8"):
        pass
    os.chmod(path, 0o600)


def _format_known_host_pattern(hostname: str, port: int) -> str:
    """Format the host pattern used in ``known_hosts``.

    :param hostname: Host name.
    :param port: SSH port.
    :return: Known-hosts pattern.
    :rtype: str
    """
    if port == 22:
        return hostname
    return f"[{hostname}]:{port}"


def _prompt_add_to_known_hosts(host_pattern: str, public_key: str) -> bool:
    """Ask the user whether to write a host key entry.

    :param host_pattern: Host pattern for the entry.
    :param public_key: Public key text.
    :return: True when the entry should be written.
    :rtype: bool
    """
    response = input(
        f"Add host key for {host_pattern} to known_hosts? "
        f"[public key: {public_key}] (yes/no): "
    )
    return response.strip().lower() == "yes"
