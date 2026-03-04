import typing as T
from io import BytesIO

import aiohttp

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.lib.base_prefetch_reader import AioBasePrefetchReader
from aiomegfile.lib.http_retry import http_retry, translate_http_error

__all__ = [
    "DEFAULT_TIMEOUT",
    "AioHttpPrefetchReader",
]

DEFAULT_TIMEOUT = 60.0


def _parse_total_size_from_headers(headers: T.Mapping[str, str]) -> T.Optional[int]:
    """Parse total size from HTTP headers.

    :param headers: HTTP response headers.
    :return: Total content size, or ``None`` if unavailable.
    :rtype: T.Optional[int]
    """
    content_range = headers.get("Content-Range")
    if content_range and "/" in content_range:
        total_size = content_range.rsplit("/", 1)[-1]
        if total_size != "*":
            try:
                return int(total_size)
            except ValueError:
                pass

    content_length = headers.get("Content-Length")
    if content_length:
        try:
            return int(content_length)
        except ValueError:
            pass
    return None


def _parse_content_length(headers: T.Mapping[str, str]) -> T.Optional[int]:
    """Parse content length from headers.

    :param headers: HTTP response headers.
    :return: Content length, or ``None`` when unavailable.
    :rtype: T.Optional[int]
    """
    content_length = headers.get("Content-Length")
    if content_length is None:
        return None
    try:
        return int(content_length)
    except ValueError:
        return None


def _supports_byte_range(headers: T.Mapping[str, str], status_code: int) -> bool:
    """Return whether the response supports byte-range reading.

    :param headers: HTTP response headers.
    :param status_code: HTTP status code.
    :return: True if byte-range request is supported.
    :rtype: bool
    """
    accept_ranges = (headers.get("Accept-Ranges") or "").lower()
    if accept_ranges == "bytes":
        return True
    if "Content-Range" in headers:
        return True
    return status_code == 206


class AioHttpPrefetchReader(AioBasePrefetchReader):
    """Async prefetch reader for HTTP(S) content.

    This reader only supports byte-range capable servers.

    :param url: HTTP(S) URL.
    :param mode: File mode, either ``r``/``rt`` or ``rb``.
    :param encoding: Text encoding for text mode.
    :param errors: Error handling for text decoding.
    :param newline: Newline handling for text mode.
    :param timeout: Request timeout in seconds.
    :param content_size: Optional known content size.
    :param session: Optional external ``aiohttp.ClientSession``.
    :param block_size: Prefetch block size in bytes.
    :param max_buffer_size: Maximum prefetch buffer in bytes.
    :param block_forward: Number of blocks to prefetch ahead.
    :param max_retries: Maximum retry times for HTTP requests.
    """

    def __init__(
        self,
        url: str,
        *,
        mode: str = "rb",
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        content_size: T.Optional[int] = None,
        session: T.Optional[aiohttp.ClientSession] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: T.Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    ) -> None:
        """Initialize the HTTP prefetch reader.

        :param url: HTTP(S) URL.
        :param mode: File mode, either ``r``/``rt`` or ``rb``.
        :param encoding: Text encoding for text mode.
        :param errors: Error handling for text decoding.
        :param newline: Newline handling for text mode.
        :param timeout: Request timeout in seconds.
        :param content_size: Optional known content size.
        :param session: Optional external ``aiohttp.ClientSession``.
        :param block_size: Prefetch block size in bytes.
        :param max_buffer_size: Maximum prefetch buffer in bytes.
        :param block_forward: Number of blocks to prefetch ahead.
        :param max_retries: Maximum retry times for HTTP requests.
        """
        self._url = url
        self._timeout = timeout
        self._explicit_content_size = content_size
        self._response_headers: dict[str, str] = {}
        self._supports_range = False

        self._session = session
        self._owns_session = session is None

        super().__init__(
            mode=mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
            block_size=block_size,
            max_buffer_size=max_buffer_size,
            block_forward=block_forward,
            max_retries=max_retries,
        )

    async def __aenter__(self):
        """Enter async context and initialize content metadata.

        :return: Initialized reader instance.
        :rtype: AioHttpPrefetchReader
        """
        await self._ensure_session()
        return await super().__aenter__()

    @property
    def name(self) -> str:
        """Return the URL as file name.

        :return: URL string.
        :rtype: str
        """
        return self._url

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure a usable aiohttp session exists.

        :return: Active ``aiohttp.ClientSession``.
        :rtype: aiohttp.ClientSession
        :raises RuntimeError: If provided external session is already closed.
        """
        if self._session is not None:
            if self._session.closed:
                raise RuntimeError("HTTP session is closed")
            return self._session

        timeout = aiohttp.ClientTimeout(total=self._timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._owns_session = True
        return self._session

    async def _fetch_headers_once(self) -> tuple[dict[str, str], int]:
        """Fetch response headers once.

        This tries ``HEAD`` first and falls back to ``GET`` with
        ``Range: bytes=0-0`` when the endpoint returns ``405``.

        :return: Tuple of response headers and status code.
        :rtype: tuple[dict[str, str], int]
        """
        session = await self._ensure_session()

        try:
            async with session.head(self._url) as response:
                headers = dict(response.headers.items())
                response.raise_for_status()
                return headers, response.status
        except aiohttp.ClientResponseError as error:
            if error.status != 405:
                raise

        async with session.get(self._url, headers={"Range": "bytes=0-0"}) as response:
            headers = dict(response.headers.items())
            await response.read()
            response.raise_for_status()
            return headers, response.status

    async def _download_bytes_once(
        self,
        headers: T.Optional[dict[str, str]] = None,
    ) -> tuple[bytes, dict[str, str], int]:
        """Download response bytes once.

        :param headers: Optional request headers.
        :return: Tuple of body bytes, response headers, and status code.
        :rtype: tuple[bytes, dict[str, str], int]
        """
        session = await self._ensure_session()

        async with session.get(self._url, headers=headers) as response:
            body = await response.read()
            response_headers = dict(response.headers.items())
            response.raise_for_status()
            return body, response_headers, response.status

    async def _fetch_headers_with_retry(self) -> tuple[dict[str, str], int]:
        """Fetch response headers with retry behavior.

        :return: Tuple of response headers and status code.
        :rtype: tuple[dict[str, str], int]
        """

        @http_retry(max_retries=self._max_retries)
        async def _fetch() -> tuple[dict[str, str], int]:
            return await self._fetch_headers_once()

        try:
            return await _fetch()
        except Exception as error:
            raise translate_http_error(error, self._url) from error

    async def _download_with_retry(
        self,
        headers: T.Optional[dict[str, str]] = None,
    ) -> tuple[bytes, dict[str, str], int]:
        """Download response bytes with retry behavior.

        :param headers: Optional request headers.
        :return: Tuple of body bytes, response headers, and status code.
        :rtype: tuple[bytes, dict[str, str], int]
        """

        @http_retry(max_retries=self._max_retries)
        async def _download() -> tuple[bytes, dict[str, str], int]:
            return await self._download_bytes_once(headers=headers)

        try:
            return await _download()
        except Exception as error:
            raise translate_http_error(error, self._url) from error

    async def _get_content_size(self) -> int:
        """Get content size from response headers.

        :return: Content size in bytes.
        :rtype: int
        :raises OSError: If server does not support byte-range requests.
        """
        if self._explicit_content_size is not None:
            self._supports_range = True
            return int(self._explicit_content_size)

        headers, status_code = await self._fetch_headers_with_retry()
        self._response_headers = headers

        if not _supports_byte_range(headers, status_code):
            raise OSError(
                "Unsupported server, server must support byte-range request: "
                f"{self._url!r}"
            )
        self._supports_range = True

        content_size = _parse_total_size_from_headers(headers)
        if content_size is not None:
            return content_size

        body, headers, status_code = await self._download_with_retry(
            headers={"Range": "bytes=0-0"}
        )
        self._response_headers = headers
        if status_code != 206 or not _supports_byte_range(headers, status_code):
            raise OSError(
                "Unsupported server, server must support byte-range request: "
                f"{self._url!r}"
            )

        content_size = _parse_total_size_from_headers(headers)
        if content_size is not None:
            return content_size

        if len(body) == 0:
            return 0

        raise OSError(
            f"Cannot determine content size from response headers: {self._url!r}"
        )

    async def _fetch_response(
        self,
        start: T.Optional[int] = None,
        end: T.Optional[int] = None,
    ) -> dict:
        """Fetch HTTP response body by byte range.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with ``Body`` and metadata.
        :rtype: dict
        :raises OSError: If server returns non-range response.
        """
        if start is None or end is None:
            headers, status_code = await self._fetch_headers_with_retry()
            self._response_headers = headers
            return {
                "Headers": headers,
                "StatusCode": status_code,
            }

        if not self._supports_range:
            raise OSError(
                "Unsupported server, server must support byte-range request: "
                f"{self._url!r}"
            )

        range_end = min(end, self._content_size - 1)
        body, headers, status_code = await self._download_with_retry(
            headers={"Range": f"bytes={start}-{range_end}"}
        )
        self._response_headers = headers

        if status_code != 206:
            raise OSError(
                "Unsupported server, expected HTTP 206 for byte-range request: "
                f"{self._url!r}"
            )

        expected_size = _parse_content_length(headers)
        if expected_size is not None and expected_size != len(body):
            raise OSError(
                "The downloaded content is incomplete, "
                f"expected size: {expected_size}, actual size: {len(body)}"
            )

        return {
            "Body": BytesIO(body),
            "Headers": headers,
            "StatusCode": status_code,
        }

    async def close(self) -> None:
        """Close reader and release HTTP session."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        await super().close()
