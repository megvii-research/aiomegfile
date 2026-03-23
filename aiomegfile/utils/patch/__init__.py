"""Compatibility patch helpers for bridging ``megfile`` to ``aiomegfile``."""

from ._patch import patch_megfile
from ._sync import (
    _AsyncToSync,
    _run_coroutine,
    _SyncAsyncFile,
    _SyncAsyncProxy,
    _wrap_async_result,
)

__all__ = [
    "_AsyncToSync",
    "_run_coroutine",
    "_SyncAsyncFile",
    "_SyncAsyncProxy",
    "_wrap_async_result",
    "patch_megfile",
]
