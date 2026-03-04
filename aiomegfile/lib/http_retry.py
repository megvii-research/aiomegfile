import asyncio

import aiohttp

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.errors import aioretry

HTTP_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
HTTP_NOT_FOUND_STATUS_CODES = {404}
HTTP_PERMISSION_STATUS_CODES = {401, 403}

__all__ = [
    "HTTP_NOT_FOUND_STATUS_CODES",
    "HTTP_PERMISSION_STATUS_CODES",
    "HTTP_RETRYABLE_STATUS_CODES",
    "http_retry",
    "http_should_retry",
    "translate_http_error",
]


def http_should_retry(error: Exception) -> bool:
    """Return whether an HTTP exception should trigger retry.

    :param error: Exception raised by request.
    :return: True if request should be retried.
    :rtype: bool
    """
    if isinstance(error, aiohttp.ClientResponseError):
        return error.status in HTTP_RETRYABLE_STATUS_CODES
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
    if isinstance(error, aiohttp.ClientResponseError):
        if error.status in HTTP_NOT_FOUND_STATUS_CODES:
            return FileNotFoundError(f"No such file: {url!r}")
        if error.status in HTTP_PERMISSION_STATUS_CODES:
            return PermissionError(f"Permission denied: {url!r}")
        return OSError(f"HTTP error {error.status}: {url!r}")

    if isinstance(error, asyncio.TimeoutError):
        return TimeoutError(f"Request timeout: {url!r}")

    if isinstance(error, aiohttp.ClientError):
        return OSError(f"Unable to access {url!r}: {error}")

    return error


def http_retry(max_retries: int = DEFAULT_MAX_RETRY_TIMES):
    """Return retry decorator configured for HTTP requests.

    :param max_retries: Maximum retry attempts.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=http_should_retry,
        max_retries=max_retries,
    )
