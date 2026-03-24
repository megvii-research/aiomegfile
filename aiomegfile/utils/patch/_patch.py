"""Patch installed ``megfile`` modules to use aiomegfile compatibility layers."""

from __future__ import annotations

import importlib
import inspect
import typing as T

from aiomegfile import smart as aio_smart

from . import _compat
from ._sync import _AsyncToSync


def patch_megfile() -> T.Dict[str, T.Any]:
    """Patch ``megfile`` top-level, ``smart``, and ``smart_path`` surfaces.

    :return: Mapping of patched exported names to patched objects.
    :rtype: dict[str, typing.Any]
    """
    megfile = importlib.import_module("megfile")
    meg_smart = importlib.import_module("megfile.smart")
    meg_smart_path = importlib.import_module("megfile.smart_path")

    patched: T.Dict[str, T.Any] = {}

    smart_exports = _build_smart_exports(meg_smart)
    _patch_module(meg_smart, smart_exports, patched)
    _patch_module(meg_smart_path, _build_smart_path_exports(), patched)

    _patch_optional_module("megfile.fs_path", _build_fs_exports(), patched)
    _patch_optional_module("megfile.hdfs_path", _build_hdfs_exports(), patched)
    _patch_optional_module("megfile.http_path", _build_http_exports(), patched)
    _patch_optional_module("megfile.s3_path", _build_s3_exports(), patched)
    _patch_optional_module("megfile.sftp_path", _build_sftp_exports(), patched)
    _patch_optional_module("megfile.stdio_path", _build_stdio_exports(), patched)
    _patch_optional_module("megfile.webdav_path", _build_webdav_exports(), patched)

    top_level_exports: T.Dict[str, T.Any] = {}
    top_level_exports.update(_build_smart_top_level_exports(megfile, smart_exports))
    top_level_exports.update(_build_smart_path_top_level_exports())
    top_level_exports.update(_build_fs_exports())
    top_level_exports.update(_build_hdfs_exports())
    top_level_exports.update(_build_http_exports())
    top_level_exports.update(_build_s3_exports())
    top_level_exports.update(_build_sftp_exports())
    top_level_exports.update(_build_stdio_exports())
    top_level_exports.update(_build_webdav_exports())
    _patch_module(megfile, top_level_exports, patched)
    return patched


def _patch_optional_module(
    module_name: str,
    exports: T.Mapping[str, T.Any],
    patched: T.MutableMapping[str, T.Any],
) -> None:
    """Patch a module when it can be imported.

    :param module_name: Fully qualified module name.
    :param exports: Attributes to set on the module.
    :param patched: Shared patched-name mapping.
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return
    _patch_module(module, exports, patched)


def _patch_module(
    module,
    exports: T.Mapping[str, T.Any],
    patched: T.MutableMapping[str, T.Any],
) -> None:
    """Set exported objects on a target module.

    :param module: Module object to patch.
    :param exports: Attributes to set.
    :param patched: Shared patched-name mapping.
    """
    for name, value in exports.items():
        setattr(module, name, value)
        patched[name] = value


def _build_smart_exports(meg_smart) -> T.Dict[str, T.Any]:
    """Build the export mapping for ``megfile.smart``.

    :param meg_smart: Imported ``megfile.smart`` module.
    :return: Export mapping.
    :rtype: dict[str, typing.Any]
    """
    exports: T.Dict[str, T.Any] = {
        "SmartPath": _compat.SmartPath,
        "get_traditional_path": _compat.get_traditional_path,
        "register_copy_func": _compat.register_copy_func,
        "smart_access": _compat.smart_access,
        "smart_copy": _compat.smart_copy,
        "smart_ismount": _compat.smart_ismount,
        "smart_move": _compat.smart_move,
        "smart_open": _compat.smart_open,
        "smart_rename": _compat.smart_rename,
        "smart_scandir": _compat.smart_scandir,
    }

    for name, func in aio_smart.__dict__.items():
        if name.startswith("_"):
            continue
        if name in exports:
            continue
        if not inspect.isfunction(func) and not inspect.iscoroutinefunction(func):
            continue
        if not hasattr(meg_smart, name):
            continue
        exports[name] = _AsyncToSync(func)
    return exports


def _build_smart_top_level_exports(
    megfile, smart_exports: T.Mapping[str, T.Any]
) -> T.Dict[str, T.Any]:
    """Return smart exports that should also be mirrored to ``megfile``.

    :param megfile: Imported ``megfile`` top-level module.
    :param smart_exports: Export mapping for ``megfile.smart``.
    :return: Mirrored export mapping.
    :rtype: dict[str, typing.Any]
    """
    top_level_exports: T.Dict[str, T.Any] = {}
    export_names = set(getattr(megfile, "__all__", []))
    for name, value in smart_exports.items():
        if hasattr(megfile, name) or name in export_names:
            top_level_exports[name] = value
    return top_level_exports


def _build_smart_path_exports() -> T.Dict[str, T.Any]:
    """Build the export mapping for ``megfile.smart_path``.

    :return: Export mapping.
    :rtype: dict[str, typing.Any]
    """
    return {
        "SmartPath": _compat.SmartPath,
        "get_traditional_path": _compat.get_traditional_path,
    }


def _build_smart_path_top_level_exports() -> T.Dict[str, T.Any]:
    """Build top-level exports derived from ``smart_path``.

    :return: Export mapping.
    :rtype: dict[str, typing.Any]
    """
    return {
        "SmartPath": _compat.SmartPath,
    }


def _build_fs_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for filesystem-local helpers."""
    return {
        "FSPath": _compat.FSPath,
        "fs_copy": _compat.fs_copy,
        "is_fs": _compat.is_fs,
    }


def _build_hdfs_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for HDFS helpers."""
    return {
        "HdfsPath": _compat.HdfsPath,
        "is_hdfs": _compat.is_hdfs,
    }


def _build_http_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for HTTP helpers."""
    return {
        "HttpPath": _compat.HttpPath,
        "HttpsPath": _compat.HttpsPath,
        "is_http": _compat.is_http,
    }


def _build_s3_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for S3 helpers."""
    return {
        "S3Path": _compat.S3Path,
        "is_s3": _compat.is_s3,
        "s3_buffered_open": _compat.s3_buffered_open,
        "s3_cached_open": _compat.s3_cached_open,
        "s3_concat": _compat.s3_concat,
        "s3_copy": _compat.s3_copy,
        "s3_download": _compat.s3_download,
        "s3_load_content": _compat.s3_load_content,
        "s3_memory_open": _compat.s3_memory_open,
        "s3_open": _compat.s3_open,
        "s3_pipe_open": _compat.s3_pipe_open,
        "s3_prefetch_open": _compat.s3_prefetch_open,
        "s3_share_cache_open": _compat.s3_share_cache_open,
        "s3_upload": _compat.s3_upload,
    }


def _build_sftp_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for SFTP helpers."""
    return {
        "SftpPath": _compat.SftpPath,
        "is_sftp": _compat.is_sftp,
        "sftp_add_host_key": _compat.sftp_add_host_key,
        "sftp_concat": _compat.sftp_concat,
        "sftp_copy": _compat.sftp_copy,
        "sftp_download": _compat.sftp_download,
        "sftp_upload": _compat.sftp_upload,
    }


def _build_stdio_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for stdio helpers."""
    return {
        "StdioPath": _compat.StdioPath,
        "is_stdio": _compat.is_stdio,
        "stdio_open": _compat.stdio_open,
    }


def _build_webdav_exports() -> T.Dict[str, T.Any]:
    """Build export mapping for WebDAV helpers."""
    return {
        "WebdavPath": _compat.WebdavPath,
        "is_webdav": _compat.is_webdav,
    }
