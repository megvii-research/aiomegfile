"""HDFS protocol retry and error translation helpers."""

from __future__ import annotations

import asyncio
import typing as T

from aiomegfile.config import HDFS_MAX_RETRY_TIMES
from aiomegfile.errors.core import UnknownError, aioretry

__all__ = [
    "HdfsConfigError",
    "HdfsException",
    "HdfsFileExistsError",
    "HdfsFileNotFoundError",
    "HdfsInvalidError",
    "HdfsIsADirectoryError",
    "HdfsNotADirectoryError",
    "HdfsPermissionError",
    "HdfsSameFileError",
    "HdfsTimeoutError",
    "HdfsUnsupportedError",
    "HdfsUnknownError",
    "hdfs_retry",
    "hdfs_should_retry",
    "translate_hdfs_error",
]


class HdfsException(Exception):
    """Base type for HDFS-specific errors."""


class HdfsConfigError(HdfsException, EnvironmentError):
    """Raised when HDFS configuration or dependency is invalid."""


class HdfsFileNotFoundError(HdfsException, FileNotFoundError):
    """Raised when an HDFS resource does not exist."""


class HdfsFileExistsError(HdfsException, FileExistsError):
    """Raised when creating an HDFS resource that already exists."""


class HdfsNotADirectoryError(HdfsException, NotADirectoryError):
    """Raised when an HDFS resource is not a directory."""


class HdfsIsADirectoryError(HdfsException, IsADirectoryError):
    """Raised when an HDFS resource is a directory."""


class HdfsSameFileError(HdfsException, OSError):
    """Raised when HDFS source and destination are the same file."""


class HdfsPermissionError(HdfsException, PermissionError):
    """Raised when HDFS access is denied."""


class HdfsInvalidError(HdfsException, ValueError):
    """Raised when an HDFS operation uses invalid arguments or state."""


class HdfsUnsupportedError(HdfsException, NotImplementedError):
    """Raised when an HDFS operation is unsupported."""


class HdfsTimeoutError(HdfsException, TimeoutError):
    """Raised when an HDFS operation times out."""


class HdfsUnknownError(HdfsException, UnknownError):
    """Raised for unmapped HDFS failures."""


def _error_message(error: Exception) -> str:
    """Return a normalized error message string.

    :param error: Original exception.
    :return: Lower-level message extracted from the exception.
    :rtype: str
    """
    message = getattr(error, "message", None)
    if message is None:
        message = str(error)
    return str(message)


def hdfs_should_retry(error: Exception) -> bool:
    """Return whether an HDFS exception should trigger retry.

    :param error: Exception raised by an HDFS operation.
    :return: True if operation should be retried.
    :rtype: bool
    """
    if isinstance(error, (asyncio.TimeoutError, ConnectionError, TimeoutError)):
        return True

    status_code = getattr(error, "status_code", None)
    if status_code in {408, 425, 429, 500, 502, 503, 504}:
        return True

    message = _error_message(error).lower()
    retry_markers = (
        "timed out",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "broken pipe",
        "max retries exceeded",
    )
    return any(marker in message for marker in retry_markers)


def translate_hdfs_error(error: Exception, uri: str) -> Exception:
    """Translate HDFS client errors to filesystem-like exceptions.

    :param error: Original exception raised by an HDFS operation.
    :param uri: URI used in the failed operation.
    :return: Translated exception.
    :rtype: Exception
    """
    if isinstance(error, HdfsException):
        return error

    if isinstance(error, FileNotFoundError):
        return HdfsFileNotFoundError(str(error) or f"No such file: {uri!r}")
    if isinstance(error, FileExistsError):
        return HdfsFileExistsError(str(error) or f"File exists: {uri!r}")
    if isinstance(error, PermissionError):
        return HdfsPermissionError(str(error) or f"Permission denied: {uri!r}")
    if isinstance(error, IsADirectoryError):
        return HdfsIsADirectoryError(str(error) or f"Is a directory: {uri!r}")
    if isinstance(error, NotADirectoryError):
        return HdfsNotADirectoryError(str(error) or f"Not a directory: {uri!r}")
    if isinstance(error, TimeoutError):
        return HdfsTimeoutError(str(error) or f"Operation timed out: {uri!r}")
    if isinstance(error, ValueError):
        return HdfsInvalidError(str(error) or f"Invalid operation: {uri!r}")

    message = _error_message(error)
    message_lower = message.lower()
    status_code = getattr(error, "status_code", None)

    if "path is not a file" in message_lower:
        return HdfsIsADirectoryError(f"Is a directory: {uri!r}")
    if "path is not a directory" in message_lower:
        return HdfsNotADirectoryError(f"Not a directory: {uri!r}")
    if status_code in {401, 403}:
        return HdfsPermissionError(f"Permission denied: {uri!r}")
    if status_code == 400:
        return HdfsInvalidError(f"{message}, path: {uri}")
    if status_code == 404 or any(
        marker in message_lower
        for marker in ("not found", "no such file", "does not exist", "missing")
    ):
        return HdfsFileNotFoundError(f"No such file or directory: {uri!r}")
    if status_code == 409 or "already exists" in message_lower:
        return HdfsFileExistsError(f"File exists: {uri!r}")
    if isinstance(error, OSError):
        return HdfsUnknownError(error, uri, extra="HDFS OS-level operation failed")

    return HdfsUnknownError(error, uri, extra="HDFS operation failed")


def hdfs_retry(
    max_retries: int = HDFS_MAX_RETRY_TIMES,
    before_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
    after_callback: T.Optional[T.Callable[..., T.Awaitable[T.Any]]] = None,
    retry_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
):
    """Return retry decorator configured for HDFS operations.

    :param max_retries: Maximum retry attempts.
    :param before_callback: Optional callback before the first attempt.
    :param after_callback: Optional callback after a successful attempt.
    :param retry_callback: Optional callback before each retry.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=hdfs_should_retry,
        max_retries=max_retries,
        before_callback=before_callback,
        after_callback=after_callback,
        retry_callback=retry_callback,
    )
