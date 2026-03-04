import codecs
import contextlib
import io
import sys
import typing as T
import urllib.parse
from datetime import timezone
from email.utils import parsedate_to_datetime

import aiohttp

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.interfaces import (
    AioReadable,
    AioSeekable,
    BaseFileSystem,
    StatResult,
)
from aiomegfile.utils.retry.http_retry import (
    HTTP_NOT_FOUND_STATUS_CODES,
    http_retry,
    translate_http_error,
)
from aiomegfile.lib.prefetch_reader.http_prefetch_reader import AioHttpPrefetchReader
from aiomegfile.utils.http import (
    is_byte_range_supported,
    parse_total_size_from_headers,
    request_headers,
)
from aiomegfile.utils.path import PathLike, fspath, split_uri

DEFAULT_HTTP_TIMEOUT = 60.0


def is_http(path: PathLike) -> bool:
    """Return whether the given path is an HTTP or HTTPS URL.

    :param path: Path to be tested.
    :return: True if path is an HTTP(S) URL, otherwise False.
    :rtype: bool
    """
    path = fspath(path)
    parsed = urllib.parse.urlparse(path)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _parse_http_timestamp(last_modified: T.Optional[str]) -> float:
    """Parse HTTP ``Last-Modified`` header into unix timestamp.

    :param last_modified: Header value.
    :return: Parsed unix timestamp, or 0.0 when unavailable.
    :rtype: float
    """
    if not last_modified:
        return 0.0
    parsed = parsedate_to_datetime(last_modified)
    if parsed is None:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class AioHttpContentReader(AioReadable[T.AnyStr]):
    """Async reader for streaming HTTP response content."""

    def __init__(
        self,
        uri: str,
        mode: str,
        timeout: float,
        max_retries: int,
        encoding: str,
        errors: str,
        newline: T.Optional[str],
    ) -> None:
        """Initialize the content reader.

        :param uri: Full HTTP(S) URL.
        :param mode: File mode.
        :param timeout: Request timeout in seconds.
        :param max_retries: Maximum retry attempts.
        :param encoding: Text encoding for text mode.
        :param errors: Text decoding error strategy.
        :param newline: Newline handling for text mode.
        """
        self._uri = uri
        self._mode = mode
        self._timeout = timeout
        self._max_retries = max_retries
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._session: T.Optional[aiohttp.ClientSession] = None
        self._owns_session = True
        self._response_ctx: T.Any = None
        self._response: T.Optional[aiohttp.ClientResponse] = None
        self._stream: T.Optional[aiohttp.StreamReader] = None
        self._pending = bytearray()
        self._offset = 0

        self._decoder = None
        self._decoder_finalized = False
        if "b" not in mode:
            decoder_factory = codecs.getincrementaldecoder(self._encoding)
            self._decoder = decoder_factory(errors=self._errors)

    @property
    def name(self) -> str:
        """Return source URI.

        :return: URI string.
        :rtype: str
        """
        return self._uri

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: File mode string.
        :rtype: str
        """
        return self._mode

    async def __aenter__(self):
        """Enter async context and open HTTP streaming response.

        :return: Reader instance.
        :rtype: AioHttpContentReader
        """
        await self._open_response()
        return self

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure a usable aiohttp session exists.

        :return: Active ``aiohttp.ClientSession``.
        :rtype: aiohttp.ClientSession
        """
        if self._session is not None:
            if self._session.closed:
                raise RuntimeError("HTTP session is closed")
            return self._session

        timeout = aiohttp.ClientTimeout(total=self._timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        self._owns_session = True
        return self._session

    async def _open_response(self) -> None:
        """Open the HTTP response stream with retry support."""
        if self._stream is not None:
            return

        @http_retry(max_retries=self._max_retries)
        async def _open_once() -> tuple[T.Any, aiohttp.ClientResponse]:
            session = await self._ensure_session()
            response_ctx = session.get(self._uri)  # pyre-ignore[16]
            try:
                response = await response_ctx.__aenter__()
                response.raise_for_status()
            except Exception:
                with contextlib.suppress(Exception):
                    await response_ctx.__aexit__(*sys.exc_info())
                raise
            return response_ctx, response

        try:
            response_ctx, response = await _open_once()
        except Exception as error:
            raise translate_http_error(error, self._uri) from error

        self._response_ctx = response_ctx
        self._response = response
        self._stream = response.content

    def _decode_bytes(self, data: bytes, *, final: bool = False) -> str:
        """Decode bytes to text for text mode.

        :param data: Input bytes.
        :param final: Whether this is the final decode pass.
        :return: Decoded string.
        :rtype: str
        """
        if self._decoder is None:
            return ""
        if final and self._decoder_finalized:
            return ""
        text = self._decoder.decode(data, final=final)
        if final:
            self._decoder_finalized = True
        return text

    async def _read_bytes(self, size: T.Optional[int] = None) -> bytes:
        """Read raw bytes from stream using pending buffer.

        :param size: Maximum bytes to read. ``None`` means read all.
        :return: Raw bytes.
        :rtype: bytes
        """
        await self._open_response()
        if self._stream is None:
            return b""

        if size is None or size < 0:
            prefix = bytes(self._pending)
            self._pending.clear()
            suffix = await self._stream.read(-1)  # pyre-ignore[16]
            return prefix + suffix

        if size == 0:
            return b""

        if len(self._pending) >= size:
            data = bytes(self._pending[:size])
            del self._pending[:size]
            return data

        data = bytes(self._pending)
        self._pending.clear()
        remain = size - len(data)
        if remain > 0:
            data += await self._stream.read(remain)
        return data

    async def _readline_bytes(self, size: T.Optional[int] = None) -> bytes:
        """Read one raw line from stream using pending buffer.

        :param size: Maximum bytes to read.
        :return: Line bytes.
        :rtype: bytes
        """
        await self._open_response()
        if self._stream is None:
            return b""
        if size == 0:
            return b""

        while True:
            newline_index = self._pending.find(b"\n")
            if newline_index >= 0:
                end = newline_index + 1
                if size is not None and size > 0:
                    end = min(end, size)
                line = bytes(self._pending[:end])
                del self._pending[:end]
                return line

            if size is not None and size > 0 and len(self._pending) >= size:
                line = bytes(self._pending[:size])
                del self._pending[:size]
                return line

            chunk = await self._stream.read(8192)  # pyre-ignore[16]
            if not chunk:
                if size is None or size < 0:
                    end = len(self._pending)
                else:
                    end = min(len(self._pending), size)
                line = bytes(self._pending[:end])
                del self._pending[:end]
                return line
            self._pending.extend(chunk)

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read content from current position.

        :param size: Maximum size to read.
        :return: Read bytes or text.
        :rtype: T.AnyStr
        """
        data = await self._read_bytes(size=size)
        self._offset += len(data)
        if "b" in self._mode:
            return T.cast(T.AnyStr, data)
        return T.cast(
            T.AnyStr,
            self._decode_bytes(data, final=(size is None or size < 0)),
        )

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read one line from current position.

        :param size: Maximum size to read.
        :return: One line in bytes or text.
        :rtype: T.AnyStr
        """
        data = await self._readline_bytes(size=size)
        self._offset += len(data)
        if "b" in self._mode:
            return T.cast(T.AnyStr, data)
        final = self._stream is not None and self._stream.at_eof() and not self._pending
        return T.cast(T.AnyStr, self._decode_bytes(data, final=final))

    async def tell(self) -> int:
        """Return current stream offset.

        :return: Current stream offset.
        :rtype: int
        """
        return self._offset

    async def close(self) -> None:
        """Close reader and release HTTP resources."""
        self._pending.clear()
        self._stream = None
        self._response = None
        if self._response_ctx is not None:
            with contextlib.suppress(Exception):
                await self._response_ctx.__aexit__(None, None, None)
            self._response_ctx = None
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
        self._session = None


class AioHttpAdaptiveReader(AioReadable[T.AnyStr], AioSeekable[T.AnyStr]):
    """Adaptive HTTP reader selecting prefetch or full-content strategy."""

    def __init__(
        self,
        uri: str,
        mode: str,
        timeout: float,
        max_retries: int,
        encoding: str,
        errors: str,
        newline: T.Optional[str],
        block_size: int,
        max_buffer_size: int,
        block_forward: T.Optional[int],
    ) -> None:
        """Initialize the adaptive reader.

        :param uri: Full HTTP(S) URL.
        :param mode: File mode.
        :param timeout: Request timeout in seconds.
        :param max_retries: Maximum retry attempts.
        :param encoding: Text encoding for text mode.
        :param errors: Text decoding error strategy.
        :param newline: Newline handling for text mode.
        :param block_size: Prefetch block size in bytes.
        :param max_buffer_size: Maximum prefetch buffer size.
        :param block_forward: Number of prefetched blocks ahead.
        """
        self._uri = uri
        self._mode = mode
        self._timeout = timeout
        self._max_retries = max_retries
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._block_size = block_size
        self._max_buffer_size = max_buffer_size
        self._block_forward = block_forward

        self._reader: T.Optional[
            T.Union[AioHttpPrefetchReader, AioHttpContentReader]
        ] = None
        self._entered = False

    @property
    def name(self) -> str:
        """Return source URI.

        :return: URI string.
        :rtype: str
        """
        return self._uri

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: File mode string.
        :rtype: str
        """
        return self._mode

    async def _select_reader(
        self,
    ) -> T.Union[AioHttpPrefetchReader, AioHttpContentReader]:
        """Select concrete reader implementation lazily.

        :return: Selected reader instance.
        :rtype: T.Union[AioHttpPrefetchReader, AioHttpContentReader]
        """
        if self._reader is not None:
            return self._reader

        try:
            headers, status_code = await request_headers(
                self._uri,
                self._timeout,
                max_retries=self._max_retries,
            )
        except Exception as error:
            raise translate_http_error(error, self._uri) from error

        if is_byte_range_supported(headers, status_code):
            content_size = parse_total_size_from_headers(headers)
            self._reader = AioHttpPrefetchReader(
                self._uri,
                mode=self._mode,
                encoding=self._encoding,
                errors=self._errors,
                newline=self._newline,
                timeout=self._timeout,
                content_size=content_size,
                block_size=self._block_size,
                max_buffer_size=self._max_buffer_size,
                block_forward=self._block_forward,
                max_retries=self._max_retries,
            )
        else:
            self._reader = AioHttpContentReader(
                self._uri,
                mode=self._mode,
                timeout=self._timeout,
                max_retries=self._max_retries,
                encoding=self._encoding,
                errors=self._errors,
                newline=self._newline,
            )

        return self._reader

    async def _active_reader(self):
        """Return initialized reader entered with async context.

        :return: Active reader instance.
        """
        reader = await self._select_reader()
        if not self._entered:
            await reader.__aenter__()
            self._entered = True
        return reader

    async def __aenter__(self):
        """Enter async context and return selected reader.

        :return: Concrete reader instance.
        """
        return await self._active_reader()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context and close selected reader."""
        if self._reader is None:
            return

        if self._entered and hasattr(self._reader, "__aexit__"):
            await self._reader.__aexit__(exc_type, exc_val, exc_tb)  # pyre-ignore[16]
        else:
            await self._reader.close()  # pyre-ignore[16]
        self._entered = False

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read content from current position.

        :param size: Maximum size to read.
        :return: Read bytes or text.
        :rtype: T.AnyStr
        """
        reader = await self._active_reader()
        return await reader.read(size)

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read one line from current position.

        :param size: Maximum size to read.
        :return: One line in bytes or text.
        :rtype: T.AnyStr
        """
        reader = await self._active_reader()
        return await reader.readline(size)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move stream cursor and return new offset.

        :param offset: Target offset.
        :param whence: Seek reference position.
        :return: New absolute offset.
        :rtype: int
        """
        reader = await self._active_reader()
        return await reader.seek(offset, whence)

    async def tell(self) -> int:
        """Return current stream offset.

        :return: Current stream offset.
        :rtype: int
        """
        if self._reader is None:
            return 0
        return await self._reader.tell()

    async def close(self) -> None:
        """Close selected reader if initialized."""
        if self._reader is None:
            return
        await self._reader.close()
        self._entered = False


class HttpFileSystem(BaseFileSystem):
    """Filesystem adapter for read-only HTTP resources."""

    protocol = "http"

    def __init__(self, timeout: float = DEFAULT_HTTP_TIMEOUT) -> None:
        """Initialize the HTTP filesystem.

        :param timeout: Request timeout in seconds.
        """
        self._timeout = timeout

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return whether path points to a directory.

        HTTP resources are treated as files only.

        :param path: Target path without protocol.
        :param followlinks: Ignored for HTTP protocol.
        :return: Always False.
        :rtype: bool
        """
        return False

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return whether path points to an existing HTTP resource.

        :param path: Target path without protocol.
        :param followlinks: Ignored for HTTP protocol.
        :return: True when resource exists, otherwise False.
        :rtype: bool
        """
        return await self.exists(path, followlinks=followlinks)

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether path points to an existing HTTP resource.

        :param path: Target path without protocol.
        :param followlinks: Ignored for HTTP protocol.
        :return: True when resource exists, otherwise False.
        :rtype: bool
        """
        url = self.build_uri(path)
        try:
            await request_headers(url, self._timeout)
            return True
        except aiohttp.ClientResponseError as error:
            return error.status not in HTTP_NOT_FOUND_STATUS_CODES
        except Exception:
            return False

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Get metadata for an HTTP resource.

        :param path: Target path without protocol.
        :param followlinks: Ignored for HTTP protocol.
        :return: StatResult for the HTTP resource.
        :rtype: StatResult
        :raises FileNotFoundError: When resource does not exist.
        :raises PermissionError: When request is denied.
        :raises OSError: When request fails for other reasons.
        """
        url = self.build_uri(path)
        try:
            headers, _ = await request_headers(url, self._timeout)
        except Exception as error:
            raise translate_http_error(error, url) from error

        size = parse_total_size_from_headers(headers)
        mtime = _parse_http_timestamp(headers.get("Last-Modified"))
        return StatResult(
            st_size=size or 0,
            st_ctime=mtime,
            st_mtime=mtime,
            isdir=False,
            islnk=False,
            extra=dict(headers.items()),
        )

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,  # noqa: ARG002
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        **kwargs: T.Any,
    ) -> T.AsyncContextManager:
        """Open a read-only HTTP resource.

        :param path: Target path without protocol.
        :param mode: Open mode, supports ``r``/``rt``/``rb``.
        :param buffering: Ignored, kept for API compatibility.
        :param encoding: Text encoding in text mode.
        :param errors: Text decoding error handling strategy.
        :param newline: Newline handling in text mode.
        :param kwargs: Extra options for compatibility with megfile APIs.

            Supported options:

            - ``block_size``: Prefetch block size in bytes.
            - ``max_buffer_size``: Maximum in-memory prefetch buffer size in bytes.
            - ``block_forward``: Number of blocks to prefetch ahead.
            - ``max_retries``: Maximum retry attempts for transient HTTP errors.

        :return: Async reader context manager.
        :rtype: T.AsyncContextManager
        """
        if any(flag in mode for flag in ("w", "a", "x", "+")):
            raise ValueError("HTTP resources are read-only")
        if mode not in ("r", "rt", "rb"):
            raise ValueError(f"unacceptable mode: {mode!r}")

        block_size = kwargs.get("block_size")
        max_buffer_size = kwargs.get("max_buffer_size")
        block_forward = kwargs.get("block_forward")
        max_retries = kwargs.get("max_retries")

        text_encoding = encoding or "utf-8"
        text_errors = errors or "strict"

        return AioHttpAdaptiveReader(
            uri=self.build_uri(path),
            mode=mode,
            timeout=self._timeout,
            max_retries=(
                int(max_retries) if max_retries is not None else DEFAULT_MAX_RETRY_TIMES
            ),
            encoding=text_encoding,
            errors=text_errors,
            newline=newline,
            block_size=int(block_size) if block_size is not None else READER_BLOCK_SIZE,
            max_buffer_size=(
                int(max_buffer_size)
                if max_buffer_size is not None
                else READER_MAX_BUFFER_SIZE
            ),
            block_forward=int(block_forward) if block_forward is not None else None,
        )

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[T.Any]]:
        """Scan entries under a path.

        HTTP protocol does not support directory listing.

        :param path: Target path without protocol.
        :raises NotADirectoryError: Always raised for HTTP resources.
        """
        raise NotADirectoryError(f"Not a directory: {self.build_uri(path)}")

    def scanfile(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[T.Any]]:
        """Scan files under a path.

        HTTP protocol does not support directory traversal.

        :param path: Target path without protocol.
        :raises NotADirectoryError: Always raised for HTTP resources.
        """
        raise NotADirectoryError(f"Not a directory: {self.build_uri(path)}")

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """Return whether this filesystem points to the same endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True when both are HTTP filesystems with same protocol.
        :rtype: bool
        """
        return (
            isinstance(other_filesystem, HttpFileSystem)
            and self.protocol == other_filesystem.protocol
        )

    def parse_uri(self, uri: str) -> str:
        """Parse path part from URI.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        _, path, _ = split_uri(uri)
        return path

    def build_uri(self, path: str) -> str:
        """Build URI from path.

        :param path: Path without protocol.
        :return: URI string.
        :rtype: str
        """
        return f"{self.protocol}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "HttpFileSystem":
        """Create a new filesystem instance from URI.

        :param uri: URI string.
        :return: HttpFileSystem instance.
        :rtype: HttpFileSystem
        """
        return cls()


class HttpsFileSystem(HttpFileSystem):
    """Filesystem adapter for HTTPS resources."""

    protocol = "https"
