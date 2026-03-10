"""WebDAV protocol retry and error translation helpers."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import typing as T

import aiohttp

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.errors.core import aioretry
from aiomegfile.errors.http import http_should_retry

WEBDAV_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
WEBDAV_NOT_FOUND_STATUS_CODES = {404}

__all__ = [
    "WebdavException",
    "WebdavFileNotFoundError",
    "WebdavPermissionError",
    "WebdavTimeoutError",
    "WebdavUnknownError",
    "translate_webdav_error",
    "webdav_retry",
    "webdav_should_retry",
]


def _ensure_aiodav() -> None:
    """Ensure the optional ``aiodav`` dependency is importable.

    :return: None
    :rtype: None
    :raises ModuleNotFoundError: If WebDAV optional dependency is unavailable.
    """
    try:
        importlib.import_module("aiodav")
    except ImportError as error:
        raise ImportError(
            inspect.cleandoc(
                """
                Failed to import aiodav, the following steps show you how to install it:

                    pip3 install 'aiomegfile[webdav]'
                """
            )
        ) from error


class WebdavException(Exception):
    """Base type for WebDAV-specific errors."""


class WebdavFileNotFoundError(WebdavException, FileNotFoundError):
    """Raised when WebDAV resource does not exist."""


class WebdavFileExistsError(WebdavException, FileExistsError):
    """Raised when WebDAV resource already exists."""


class WebdavNotADirectoryError(WebdavException, NotADirectoryError):
    """Raised when WebDAV resource is not a directory."""


class WebdavIsADirectoryError(WebdavException, IsADirectoryError):
    """Raised when WebDAV resource is a directory."""


class WebdavSameFileError(WebdavException, OSError):
    """Raised when WebDAV source and destination are the same."""


class WebdavPermissionError(WebdavException, PermissionError):
    """Raised when WebDAV access is denied."""


class WebdavTimeoutError(WebdavException, TimeoutError):
    """Raised when WebDAV request times out."""


class WebdavUnknownError(WebdavException, OSError):
    """Raised for unmapped WebDAV failures."""


def webdav_should_retry(error: Exception) -> bool:
    """Return whether a WebDAV exception should trigger retry.

    :param error: Exception raised by WebDAV operation.
    :return: True if operation should be retried.
    :rtype: bool
    """
    _ensure_aiodav()
    from aiodav.exceptions import (
        ConnectionException,
        NoConnection,
        ResponseErrorCode,
    )

    if http_should_retry(error):
        return True

    if isinstance(error, ResponseErrorCode):
        status = int(getattr(error, "code", 0))
        return status in WEBDAV_RETRYABLE_STATUS_CODES
    if isinstance(error, asyncio.TimeoutError):
        return True
    if isinstance(error, (NoConnection, ConnectionException)):
        return True
    if isinstance(error, aiohttp.ClientConnectionError):
        return True
    if isinstance(error, aiohttp.ServerDisconnectedError):
        return True
    if isinstance(error, aiohttp.ClientPayloadError):
        return True
    return False


def translate_webdav_error(error: Exception, uri: str) -> Exception:
    """Translate aiodav errors to filesystem-like exceptions.

    :param error: Original WebDAV error.
    :param uri: Target URI.
    :return: Translated exception.
    :rtype: Exception
    """
    _ensure_aiodav()
    from aiodav.exceptions import (
        ConnectionException,
        NoConnection,
        RemoteParentNotFound,
        RemoteResourceNotFound,
        ResponseErrorCode,
        WebDavException,
    )

    if isinstance(error, WebdavException):
        return error

    if isinstance(error, (FileNotFoundError, PermissionError)):
        return error

    if isinstance(error, (RemoteResourceNotFound, RemoteParentNotFound)):
        return WebdavFileNotFoundError(f"No such file: {uri!r}")

    if isinstance(error, ResponseErrorCode):
        status = int(getattr(error, "code", 0))
        if status in WEBDAV_NOT_FOUND_STATUS_CODES:
            return WebdavFileNotFoundError(f"No such file: {uri!r}")
        if status == 401:
            return True
        elif status == 403:
            return WebdavPermissionError(f"Permission denied: {uri!r}")
        return WebdavUnknownError(f"WebDAV error {status}: {uri!r}")

    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return WebdavTimeoutError(f"Request timeout: {uri!r}")

    if isinstance(error, (NoConnection, ConnectionException)):
        return WebdavUnknownError(f"Unable to access {uri!r}: {error}")

    if isinstance(error, aiohttp.ClientError):
        return WebdavUnknownError(f"Unable to access {uri!r}: {error}")

    if isinstance(error, WebDavException):
        return WebdavUnknownError(f"WebDAV operation failed on {uri!r}: {error}")

    if isinstance(error, OSError):
        return error

    return WebdavUnknownError(f"WebDAV operation failed on {uri!r}: {error}")


def webdav_retry(
    max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    before_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
    after_callback: T.Optional[T.Callable[..., T.Awaitable[T.Any]]] = None,
    retry_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
):
    """Return retry decorator configured for WebDAV operations.

    :param max_retries: Maximum retry attempts.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=webdav_should_retry,
        max_retries=max_retries,
        before_callback=before_callback,
        after_callback=after_callback,
        retry_callback=retry_callback,
    )
