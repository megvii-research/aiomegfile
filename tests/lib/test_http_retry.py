"""Tests for HTTP retry helpers."""

import asyncio

import aiohttp

from aiomegfile.lib.http_retry import (
    http_retry,
    http_should_retry,
    translate_http_error,
)


def _response_error(status: int) -> aiohttp.ClientResponseError:
    """Build a ``ClientResponseError`` with the given status.

    :param status: HTTP status code.
    :return: ClientResponseError instance.
    :rtype: aiohttp.ClientResponseError
    """
    return aiohttp.ClientResponseError(
        request_info=None,
        history=(),
        status=status,
        message="",
        headers=None,
    )


def test_http_should_retry_on_retryable_status_codes():
    """Test retry policy for response status errors."""
    assert http_should_retry(_response_error(500)) is True
    assert http_should_retry(_response_error(503)) is True
    assert http_should_retry(_response_error(404)) is False


def test_http_should_retry_on_transport_errors():
    """Test retry policy for transport-level failures."""
    assert http_should_retry(aiohttp.ClientConnectionError()) is True
    assert http_should_retry(asyncio.TimeoutError()) is True
    assert http_should_retry(ValueError("boom")) is False


def test_translate_http_error_response_status():
    """Test response status translation to filesystem-like exceptions."""
    not_found = translate_http_error(_response_error(404), "http://example.com/missing")
    assert isinstance(not_found, FileNotFoundError)

    permission = translate_http_error(_response_error(403), "http://example.com/denied")
    assert isinstance(permission, PermissionError)

    unknown = translate_http_error(_response_error(500), "http://example.com/error")
    assert isinstance(unknown, OSError)


def test_translate_http_error_transport_error():
    """Test transport error translation to ``OSError``."""
    translated = translate_http_error(
        aiohttp.ClientConnectionError(), "http://example.com"
    )
    assert isinstance(translated, OSError)


async def test_http_retry_decorator_retries_transient_error():
    """Test retry decorator retries transient HTTP exceptions."""
    attempts = {"count": 0}

    @http_retry(max_retries=3)
    async def unstable_call() -> str:
        """Raise once then succeed.

        :return: Static response string.
        :rtype: str
        """
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise aiohttp.ClientConnectionError()
        return "ok"

    result = await unstable_call()

    assert result == "ok"
    assert attempts["count"] == 2
