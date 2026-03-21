"""HTTP protocol retry and error translation helpers."""

from __future__ import annotations

import asyncio
import typing as T

import aiohttp

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.errors.core import UnknownError, aioretry

HTTP_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
HTTP_NOT_FOUND_STATUS_CODES = {404}
HTTP_PERMISSION_STATUS_CODES = {401, 403}

__all__ = [
    "HTTP_NOT_FOUND_STATUS_CODES",
    "HTTP_PERMISSION_STATUS_CODES",
    "HTTP_RETRYABLE_STATUS_CODES",
    "HttpException",
    "HttpFileNotFoundError",
    "HttpPermissionError",
    "HttpTimeoutError",
    "HttpUnknownError",
    "http_retry",
    "http_should_retry",
    "translate_http_error",
]


class HttpException(Exception):
    """Base type for HTTP-specific errors."""


class HttpFileNotFoundError(HttpException, FileNotFoundError):
    """Raised when HTTP resource does not exist."""


class HttpPermissionError(HttpException, PermissionError):
    """Raised when HTTP access is denied."""


class HttpTimeoutError(HttpException, TimeoutError):
    """Raised when HTTP request times out."""


class HttpUnknownError(HttpException, UnknownError):
    """Raised for unmapped HTTP failures."""


def http_should_retry(error: Exception) -> bool:
    """Return whether an HTTP exception should trigger retry.

    :param error: Exception raised by request.
    :return: True if request should be retried.
    :rtype: bool
    """
    if isinstance(error, aiohttp.ClientResponseError):
        status = error.status  # pytype: disable=attribute-error
        return status in HTTP_RETRYABLE_STATUS_CODES
    if isinstance(error, asyncio.TimeoutError):
        return True
    if isinstance(error, aiohttp.ClientConnectionError):
        return True
    if isinstance(error, aiohttp.ClientPayloadError):
        return True
    if isinstance(error, aiohttp.ServerDisconnectedError):
        return True
    return False


def translate_http_error(error: Exception, url: str) -> Exception:
    """Translate aiohttp errors to filesystem-like exceptions.

    :param error: Original HTTP error.
    :param url: Target URL.
    :return: Translated exception.
    :rtype: Exception
    """
    if isinstance(error, HttpException):
        return error

    if isinstance(error, aiohttp.ClientResponseError):
        status = error.status  # pytype: disable=attribute-error
        if status in HTTP_NOT_FOUND_STATUS_CODES:
            return HttpFileNotFoundError(f"No such file: {url!r}")
        if status in HTTP_PERMISSION_STATUS_CODES:
            return HttpPermissionError(f"Permission denied: {url!r}")
        return HttpUnknownError(error, url, extra=f"HTTP error {status}")

    if isinstance(error, asyncio.TimeoutError):
        return HttpTimeoutError(f"Request timeout: {url!r}")

    if isinstance(error, aiohttp.ClientError):
        return HttpUnknownError(error, url, extra="Unable to access resource")

    return error


def http_retry(
    max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    before_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
    after_callback: T.Optional[T.Callable[..., T.Awaitable[T.Any]]] = None,
    retry_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
):
    """Return retry decorator configured for HTTP requests.

    :param max_retries: Maximum retry attempts.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=http_should_retry,
        max_retries=max_retries,
        before_callback=before_callback,
        after_callback=after_callback,
        retry_callback=retry_callback,
    )
