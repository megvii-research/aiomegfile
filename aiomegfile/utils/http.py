import re
import typing as T

import aiohttp

from aiomegfile.config import HTTP_MAX_RETRY_TIMES
from aiomegfile.errors.http import http_retry

_CONTENT_RANGE_TOTAL_RE = re.compile(r"/(\d+)\s*$")

__all__ = [
    "is_byte_range_supported",
    "parse_content_length",
    "parse_total_size_from_headers",
    "request_headers",
]


def parse_content_length(content_length: T.Optional[str]) -> T.Optional[int]:
    """Parse ``Content-Length`` header value.

    :param content_length: Raw ``Content-Length`` header value.
    :return: Parsed content length, or ``None`` if unavailable/invalid.
    :rtype: T.Optional[int]
    """
    if content_length is None:
        return None

    try:
        value = int(content_length)
    except (TypeError, ValueError):
        return None

    if value < 0:
        return None
    return value


def parse_total_size_from_headers(headers: T.Mapping[str, str]) -> T.Optional[int]:
    """Parse total object size from HTTP headers.

    ``Content-Range`` is preferred because it exposes the full object size even when
    the response body is a single-byte range (for example, ``bytes=0-0``).

    :param headers: HTTP response headers.
    :return: Total object size in bytes, or ``None`` if unavailable.
    :rtype: T.Optional[int]
    """
    content_range = headers.get("Content-Range")
    if content_range:
        match = _CONTENT_RANGE_TOTAL_RE.search(content_range)
        if match is not None:
            total_size = parse_content_length(match.group(1))
            if total_size is not None:
                return total_size

    return parse_content_length(headers.get("Content-Length"))


def is_byte_range_supported(headers: T.Mapping[str, str], status_code: int) -> bool:
    """Return whether response supports byte-range requests.

    :param headers: HTTP response headers.
    :param status_code: HTTP status code.
    :return: True if the server supports byte-range access.
    :rtype: bool
    """
    accept_ranges = (headers.get("Accept-Ranges") or "").lower()
    if accept_ranges == "bytes":
        return True
    if "Content-Range" in headers:
        return True
    return status_code == 206


async def request_headers(
    url: str,
    timeout: float,
    max_retries: int = HTTP_MAX_RETRY_TIMES,
    session: T.Optional[aiohttp.ClientSession] = None,
) -> tuple[dict[str, str], int]:
    """Fetch response headers for an HTTP resource.

    The function first sends ``HEAD``. If server returns ``405 Method Not Allowed``,
    it falls back to ``GET`` with ``Range: bytes=0-0``.

    :param url: Full HTTP(S) URL.
    :param timeout: Request timeout in seconds.
    :param max_retries: Maximum retry attempts for transient failures.
    :param session: Optional existing aiohttp session.
    :return: Tuple of response headers and status code.
    :rtype: tuple[dict[str, str], int]
    :raises RuntimeError: If the provided session is closed.
    """
    request_session = session
    owns_session = False
    if request_session is None:
        request_timeout = aiohttp.ClientTimeout(total=timeout)
        request_session = aiohttp.ClientSession(timeout=request_timeout)
        owns_session = True
    elif request_session.closed:
        raise RuntimeError("HTTP session is closed")

    @http_retry(max_retries=max_retries)
    async def _request_once() -> tuple[dict[str, str], int]:
        if request_session is None:
            raise RuntimeError("HTTP session is not initialized")
        try:
            async with request_session.head(url) as response:
                headers = dict(response.headers.items())
                response.raise_for_status()
                return headers, response.status
        except aiohttp.ClientResponseError as error:
            if error.status != 405:
                raise

        async with request_session.get(url) as response:
            headers = dict(response.headers.items())
            response.raise_for_status()
            return headers, response.status

    try:
        return await _request_once()
    finally:
        if owns_session and request_session is not None:
            await request_session.close()
