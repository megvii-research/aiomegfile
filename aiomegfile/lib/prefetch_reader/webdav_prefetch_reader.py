"""Async prefetch reader for WebDAV resources."""

from __future__ import annotations

import typing as T
from io import BytesIO

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.errors.webdav import (
    WEBDAV_INSTALL_HINT,
    translate_webdav_error,
    webdav_retry,
)
from aiomegfile.lib.prefetch_reader.base_prefetch_reader import AioBasePrefetchReader
from aiomegfile.utils.http import parse_content_length

if T.TYPE_CHECKING:
    from aiodav.client import Client as AiodavClient

    from aiomegfile.filesystem.webdav import WebdavFileSystem
else:
    AiodavClient = T.Any

__all__ = [
    "AioWebdavPrefetchReader",
]


def _quote_webdav_urn(path: str) -> str:
    """Quote a remote path using ``aiodav.urn.Urn``.

    :param path: Remote path.
    :return: Quoted path for WebDAV request.
    :rtype: str
    :raises ModuleNotFoundError: If WebDAV optional dependency is unavailable.
    """
    try:
        from aiodav.urn import Urn
    except Exception as error:
        raise ModuleNotFoundError(WEBDAV_INSTALL_HINT) from error
    return Urn(path).quote()


class AioWebdavPrefetchReader(AioBasePrefetchReader):
    """Async prefetch reader for WebDAV content.

    :param path: WebDAV path without protocol for current filesystem.
    :param filesystem: WebDAV filesystem instance used to build client when needed.
    :param client: Optional existing aiodav client.
    :param mode: File mode, either ``r``/``rt`` or ``rb``.
    :param encoding: Text encoding for text mode.
    :param errors: Error handling for text decoding.
    :param newline: Newline handling for text mode.
    :param block_size: Prefetch block size in bytes.
    :param max_buffer_size: Maximum prefetch buffer in bytes.
    :param block_forward: Number of blocks to prefetch ahead.
    :param max_retries: Maximum retry times for WebDAV requests.
    """

    def __init__(
        self,
        path: str,
        *,
        filesystem: "WebdavFileSystem",
        client: T.Optional[AiodavClient] = None,
        mode: str = "rb",
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: T.Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    ) -> None:
        """Initialize the WebDAV prefetch reader.

        :param path: WebDAV path without protocol for current filesystem.
        :param filesystem: WebDAV filesystem instance.
        :param client: Optional existing aiodav client.
        :param mode: File mode, either ``r``/``rt`` or ``rb``.
        :param encoding: Text encoding for text mode.
        :param errors: Error handling for text decoding.
        :param newline: Newline handling for text mode.
        :param block_size: Prefetch block size in bytes.
        :param max_buffer_size: Maximum prefetch buffer in bytes.
        :param block_forward: Number of blocks to prefetch ahead.
        :param max_retries: Maximum retry times for WebDAV requests.
        """
        self._path = path
        self._filesystem = filesystem
        self._client = client
        self._owns_client = client is None
        self._supports_range = False

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

    @property
    def name(self) -> str:
        """Return full URI of the target file.

        :return: Full WebDAV URI.
        :rtype: str
        """
        return self._filesystem.build_uri(self._path)

    async def __aenter__(self):
        """Enter async context and initialize metadata.

        :return: Initialized reader.
        :rtype: AioWebdavPrefetchReader
        """
        if self._client is None:
            self._client = self._filesystem._create_client()
            self._owns_client = False
        return await super().__aenter__()

    async def _execute_download(
        self,
        headers: T.Optional[dict[str, str]] = None,
    ):
        """Execute a WebDAV download request with retry behavior.

        :param headers: Optional HTTP headers.
        :return: aiodav HTTP response object.
        """
        if self._client is None:
            raise RuntimeError("WebDAV reader is not initialized")

        remote_path = self._filesystem._normalize_remote_path(self._path)
        uri = self.name

        @webdav_retry(max_retries=self._max_retries)
        async def _request():
            return await self._client._execute_request(  # pyre-ignore[16]
                action="download",
                path=_quote_webdav_urn(remote_path),
                headers_ext=headers,
            )

        try:
            return await _request()
        except Exception as error:
            translated = translate_webdav_error(error, uri)
            raise translated from error

    async def _get_content_size(self) -> int:
        """Get content size from WebDAV metadata.

        :return: Content size in bytes.
        :rtype: int
        :raises OSError: If server does not support byte-range requests.
        """
        if self._client is None:
            raise RuntimeError("WebDAV reader is not initialized")

        remote_path = self._filesystem._normalize_remote_path(self._path)
        uri = self.name

        @webdav_retry(max_retries=self._max_retries)
        async def _fetch_info() -> dict[str, T.Any]:
            return await self._client.info(remote_path)  # pyre-ignore[16]

        try:
            info = await _fetch_info()
        except Exception as error:
            translated = translate_webdav_error(error, uri)
            raise translated from error

        size = int(info.get("size") or 0)

        if size <= 0:
            self._supports_range = True
            return 0

        response = await self._execute_download(headers={"Range": "bytes=0-0"})
        try:
            body = await response.read()
            status_code = int(response.status)
            if status_code != 206:
                raise OSError(
                    "Unsupported server, server must support byte-range request: "
                    f"{uri!r}"
                )

            headers = dict(response.headers.items())
            expected_size = parse_content_length(headers.get("Content-Length"))
            if (
                expected_size is not None
                and expected_size != len(body)
                and not headers.get("Content-Encoding")
            ):
                raise OSError(
                    "The downloaded content is incomplete, "
                    f"expected size: {expected_size}, actual size: {len(body)}"
                )
        finally:
            response.release()

        self._supports_range = True
        return size

    async def _fetch_response(
        self,
        start: T.Optional[int] = None,
        end: T.Optional[int] = None,
    ) -> dict:
        """Fetch response bytes from WebDAV by range.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with ``Body`` and metadata.
        :rtype: dict
        """
        if start is None or end is None:
            response = await self._execute_download()
            try:
                return {
                    "Headers": dict(response.headers.items()),
                    "StatusCode": int(response.status),
                }
            finally:
                response.release()

        if not self._supports_range and self._content_size > 0:
            raise OSError(
                "Unsupported server, server must support byte-range request: "
                f"{self.name!r}"
            )

        range_end = min(end, self._content_size - 1)
        response = await self._execute_download(
            headers={"Range": f"bytes={start}-{range_end}"}
        )
        try:
            body = await response.read()
            headers = dict(response.headers.items())
            status_code = int(response.status)

            if status_code != 206:
                raise OSError(
                    "Unsupported server, expected HTTP 206 for byte-range request: "
                    f"{self.name!r}"
                )

            expected_size = parse_content_length(headers.get("Content-Length"))
            if (
                expected_size is not None
                and expected_size != len(body)
                and not headers.get("Content-Encoding")
            ):
                raise OSError(
                    "The downloaded content is incomplete, "
                    f"expected size: {expected_size}, actual size: {len(body)}"
                )

            return {
                "Body": BytesIO(body),
                "Headers": headers,
                "StatusCode": status_code,
            }
        finally:
            response.release()

    async def close(self) -> None:
        """Close reader and release WebDAV client when owned."""
        if self._owns_client and self._client is not None:
            await self._client.close()
        self._client = None
        await super().close()
