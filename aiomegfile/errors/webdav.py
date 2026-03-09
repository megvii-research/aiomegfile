"""WebDAV protocol retry and error translation helpers."""

from __future__ import annotations

import asyncio
import inspect
from functools import lru_cache

import aiohttp

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.errors.core import aioretry

WEBDAV_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
WEBDAV_NOT_FOUND_STATUS_CODES = {404}
WEBDAV_PERMISSION_STATUS_CODES = {401, 403}
WEBDAV_INSTALL_HINT = (
    "WebDAV support requires optional dependency 'aiodav'. "
    "Install it with: pip install 'aiomegfile[webdav]'"
)

__all__ = [
    "WEBDAV_INSTALL_HINT",
    "WEBDAV_NOT_FOUND_STATUS_CODES",
    "WEBDAV_PERMISSION_STATUS_CODES",
    "WEBDAV_RETRYABLE_STATUS_CODES",
    "WebdavException",
    "WebdavFileNotFoundError",
    "WebdavPermissionError",
    "WebdavTimeoutError",
    "WebdavUnknownError",
    "translate_webdav_error",
    "webdav_retry",
    "webdav_should_retry",
]


def _import_aiodav_module(module_name: str):
    """Import a WebDAV optional module with install hints.

    :param module_name: Target module name under ``aiodav`` package.
    :return: Imported module object.
    :rtype: module
    :raises ImportError: If optional dependency is unavailable.
    """
    try:
        return __import__(module_name, fromlist=["*"])
    except ImportError as error:
        raise ImportError(
            inspect.cleandoc(
                """
                Failed to import aiodav, the following steps show you how to install it:

                    pip3 install 'aiomegfile[webdav]' --user
                """
            )
        ) from error


@lru_cache(maxsize=1)
def _import_aiodav_exceptions():
    """Import ``aiodav.exceptions`` with install hint.

    :return: Imported ``aiodav.exceptions`` module.
    :rtype: module
    :raises ImportError: If optional dependency is unavailable.
    """
    return _import_aiodav_module("aiodav.exceptions")


def _get_aiodav_exception_class(name: str):
    """Return exception class by name from ``aiodav.exceptions``.

    :param name: Exception class name.
    :return: Exception class, or ``None`` if missing.
    """
    try:
        module = _import_aiodav_exceptions()
    except ImportError:
        return None
    cls = getattr(module, name, None)
    if isinstance(cls, type) and issubclass(cls, Exception):
        return cls
    return None


def _is_aiodav_exception(error: Exception, *names: str) -> bool:
    """Return whether error is one of named ``aiodav`` exception types.

    :param error: Exception instance.
    :param names: Candidate exception class names.
    :return: True when matched.
    :rtype: bool
    """
    for name in names:
        cls = _get_aiodav_exception_class(name)
        if cls is not None and isinstance(error, cls):
            return True
    return False


class WebdavException(Exception):
    """Base type for WebDAV-specific errors."""


class WebdavFileNotFoundError(WebdavException, FileNotFoundError):
    """Raised when WebDAV resource does not exist."""


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
    if _is_aiodav_exception(error, "ResponseErrorCode"):
        status = int(getattr(error, "code", 0))
        return status in WEBDAV_RETRYABLE_STATUS_CODES
    if isinstance(error, asyncio.TimeoutError):
        return True
    if _is_aiodav_exception(error, "NoConnection", "ConnectionException"):
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
    if isinstance(error, ImportError) and "aiodav" in str(error):
        return ModuleNotFoundError(WEBDAV_INSTALL_HINT)

    if isinstance(error, WebdavException):
        return error

    if isinstance(error, (FileNotFoundError, PermissionError)):
        return error

    if _is_aiodav_exception(error, "RemoteResourceNotFound", "RemoteParentNotFound"):
        return WebdavFileNotFoundError(f"No such file: {uri!r}")

    if _is_aiodav_exception(error, "ResponseErrorCode"):
        status = int(getattr(error, "code", 0))
        if status in WEBDAV_NOT_FOUND_STATUS_CODES:
            return WebdavFileNotFoundError(f"No such file: {uri!r}")
        if status in WEBDAV_PERMISSION_STATUS_CODES:
            return WebdavPermissionError(f"Permission denied: {uri!r}")
        return WebdavUnknownError(f"WebDAV error {status}: {uri!r}")

    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return WebdavTimeoutError(f"Request timeout: {uri!r}")

    if _is_aiodav_exception(error, "NoConnection", "ConnectionException"):
        return WebdavUnknownError(f"Unable to access {uri!r}: {error}")

    if isinstance(error, aiohttp.ClientError):
        return WebdavUnknownError(f"Unable to access {uri!r}: {error}")

    if _is_aiodav_exception(error, "WebDavException"):
        return WebdavUnknownError(f"WebDAV operation failed on {uri!r}: {error}")

    if isinstance(error, OSError):
        return error

    return WebdavUnknownError(f"WebDAV operation failed on {uri!r}: {error}")


def webdav_retry(max_retries: int = DEFAULT_MAX_RETRY_TIMES):
    """Return retry decorator configured for WebDAV operations.

    :param max_retries: Maximum retry attempts.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=webdav_should_retry,
        max_retries=max_retries,
    )
