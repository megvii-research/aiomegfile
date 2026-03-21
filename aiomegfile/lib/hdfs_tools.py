"""Optional HDFS dependency helpers."""

from __future__ import annotations

import typing as T

try:
    import hdfs as _hdfs_api
except ImportError:  # pragma: no cover - depends on optional extra
    _hdfs_api = None

hdfs_api = T.cast(T.Any, _hdfs_api)

__all__ = ["hdfs_api"]
