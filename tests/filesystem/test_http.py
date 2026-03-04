"""Tests for HttpFileSystem and is_http."""

import io
import re
import threading
from functools import partial
from http.server import (
    BaseHTTPRequestHandler,
    SimpleHTTPRequestHandler,
    ThreadingHTTPServer,
)

import pytest

from aiomegfile.filesystem.http import HttpFileSystem, HttpsFileSystem, is_http
from aiomegfile.interfaces import get_filesystem_by_uri
from aiomegfile.lib.prefetch_reader.http_prefetch_reader import AioHttpPrefetchReader


class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with suppressed request logs."""

    def log_message(self, format, *args):
        """Suppress request logs for deterministic test output."""
        return


@pytest.fixture
def plain_http_server(tmp_path):
    """Start a plain HTTP server without byte-range support."""
    content = "hello http\nline2"
    file_path = tmp_path / "data.txt"
    file_path.write_text(content, encoding="utf-8")

    handler = partial(QuietSimpleHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield {
            "file_url": f"{base_url}/data.txt",
            "missing_url": f"{base_url}/missing.txt",
            "content": content,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture
def range_http_server():
    """Start an HTTP server with byte-range support."""
    content = b"line1\nline2\nline3\n"

    class RangeHandler(BaseHTTPRequestHandler):
        """HTTP handler with byte-range support for one file."""

        def _send_not_found(self) -> None:
            """Send a 404 response."""
            self.send_response(404)
            self.end_headers()

        def _send_headers(
            self,
            status_code: int,
            content_length: int,
            *,
            content_range: str | None = None,
        ) -> None:
            """Send HTTP headers for current response.

            :param status_code: HTTP status code.
            :param content_length: Body length.
            :param content_range: Optional Content-Range header value.
            """
            self.send_response(status_code)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(content_length))
            if content_range is not None:
                self.send_header("Content-Range", content_range)
            self.end_headers()

        def do_HEAD(self) -> None:
            """Handle HEAD request."""
            if self.path != "/data.txt":
                self._send_not_found()
                return
            self._send_headers(200, len(content))

        def do_GET(self) -> None:
            """Handle GET request with optional Range header."""
            if self.path != "/data.txt":
                self._send_not_found()
                return

            range_header = self.headers.get("Range")
            if not range_header:
                self._send_headers(200, len(content))
                self.wfile.write(content)
                return

            match = re.match(r"^bytes=(\d+)-(\d+)$", range_header)
            if match is None:
                self.send_response(416)
                self.end_headers()
                return

            start = int(match.group(1))
            end = min(int(match.group(2)), len(content) - 1)
            if start >= len(content):
                self.send_response(416)
                self.end_headers()
                return

            chunk = content[start : end + 1]
            self._send_headers(
                206,
                len(chunk),
                content_range=f"bytes {start}-{end}/{len(content)}",
            )
            self.wfile.write(chunk)

        def log_message(self, format, *args) -> None:
            """Suppress request logs for deterministic test output."""
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield {
            "file_url": f"{base_url}/data.txt",
            "content": content,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture
def head_405_range_http_server():
    """Start an HTTP server where ``HEAD`` is not allowed but range works."""
    content = b"range with head 405\nline2\n"

    class Head405RangeHandler(BaseHTTPRequestHandler):
        """HTTP handler with ``HEAD`` 405 and byte-range ``GET`` support."""

        def _send_not_found(self) -> None:
            """Send a 404 response."""
            self.send_response(404)
            self.end_headers()

        def _send_headers(
            self,
            status_code: int,
            content_length: int,
            *,
            content_range: str | None = None,
        ) -> None:
            """Send common response headers.

            :param status_code: HTTP status code.
            :param content_length: Body length.
            :param content_range: Optional ``Content-Range`` value.
            """
            self.send_response(status_code)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(content_length))
            if content_range is not None:
                self.send_header("Content-Range", content_range)
            self.end_headers()

        def do_HEAD(self) -> None:
            """Handle HEAD request."""
            if self.path != "/data.txt":
                self._send_not_found()
                return
            self.send_response(405)
            self.send_header("Allow", "GET")
            self.end_headers()

        def do_GET(self) -> None:
            """Handle GET request with optional Range header."""
            if self.path != "/data.txt":
                self._send_not_found()
                return

            range_header = self.headers.get("Range")
            if not range_header:
                self._send_headers(200, len(content))
                self.wfile.write(content)
                return

            match = re.match(r"^bytes=(\d+)-(\d+)$", range_header)
            if match is None:
                self.send_response(416)
                self.end_headers()
                return

            start = int(match.group(1))
            end = min(int(match.group(2)), len(content) - 1)
            if start >= len(content):
                self.send_response(416)
                self.end_headers()
                return

            chunk = content[start : end + 1]
            self._send_headers(
                206,
                len(chunk),
                content_range=f"bytes {start}-{end}/{len(content)}",
            )
            self.wfile.write(chunk)

        def log_message(self, format, *args) -> None:
            """Suppress request logs for deterministic test output."""
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Head405RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield {
            "file_url": f"{base_url}/data.txt",
            "content": content,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


class TestHttpFileSystem:
    """Test cases for HttpFileSystem."""

    def _parse_http_path(self, uri: str) -> str:
        """Parse URI into protocol-stripped path.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        return HttpFileSystem().parse_uri(uri)

    async def test_is_http_supports_http_and_https(self):
        """Test is_http returns True for both http and https URLs."""
        assert is_http("http://example.com/path") is True
        assert is_http("https://example.com/path") is True
        assert is_http("s3://bucket/key") is False
        assert is_http("/tmp/example.txt") is False

    async def test_get_filesystem_by_uri_supports_http_and_https(self):
        """Test filesystem registry resolves both HTTP protocols."""
        http_fs = get_filesystem_by_uri("http://example.com/path")
        https_fs = get_filesystem_by_uri("https://example.com/path")

        assert isinstance(http_fs, HttpFileSystem)
        assert isinstance(https_fs, HttpsFileSystem)

    async def test_exists_and_is_file(self, plain_http_server):
        """Test exists and is_file for HTTP resources."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(plain_http_server["file_url"])
        missing_path = self._parse_http_path(plain_http_server["missing_url"])

        assert await filesystem.exists(path) is True
        assert await filesystem.is_file(path) is True
        assert await filesystem.is_dir(path) is False
        assert await filesystem.exists(missing_path) is False

    async def test_stat(self, plain_http_server):
        """Test stat reads size and metadata from HTTP headers."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(plain_http_server["file_url"])

        stat_result = await filesystem.stat(path)
        assert stat_result.st_size == len(plain_http_server["content"].encode("utf-8"))
        assert stat_result.isdir is False
        assert stat_result.islnk is False

    async def test_open_plain_server_binary_and_text(self, plain_http_server):
        """Test open falls back to full-content reader on non-range server."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(plain_http_server["file_url"])

        async with filesystem.open(path, "rb") as file_obj:
            assert not isinstance(file_obj, AioHttpPrefetchReader)
            assert await file_obj.read() == plain_http_server["content"].encode("utf-8")

        async with filesystem.open(path, "r", encoding="utf-8") as file_obj:
            line = await file_obj.readline()
            assert isinstance(line, str)
            assert line == "hello http\n"

            text = await file_obj.read()
            assert isinstance(text, str)
            assert text == "line2"

            with pytest.raises(io.UnsupportedOperation):
                await file_obj.seek(0)

    async def test_open_range_server_uses_prefetch(self, range_http_server):
        """Test open uses prefetch reader when server supports byte-range."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(range_http_server["file_url"])

        async with filesystem.open(path, "rb") as file_obj:
            assert isinstance(file_obj, AioHttpPrefetchReader)
            assert await file_obj.read() == range_http_server["content"]

    async def test_stat_head_405_range_server_uses_total_content_size(
        self,
        head_405_range_http_server,
    ):
        """Test stat size prefers total size from ``Content-Range``."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(head_405_range_http_server["file_url"])

        stat_result = await filesystem.stat(path)
        assert stat_result.st_size == len(head_405_range_http_server["content"])

    async def test_open_head_405_range_server_uses_prefetch(
        self,
        head_405_range_http_server,
    ):
        """Test fallback header probing still selects prefetch reader."""
        filesystem = HttpFileSystem()
        path = self._parse_http_path(head_405_range_http_server["file_url"])

        async with filesystem.open(path, "rb") as file_obj:
            assert isinstance(file_obj, AioHttpPrefetchReader)
            assert await file_obj.read() == head_405_range_http_server["content"]

    async def test_missing_file_behavior(self, plain_http_server):
        """Test missing HTTP resources for stat and open."""
        filesystem = HttpFileSystem()
        missing_path = self._parse_http_path(plain_http_server["missing_url"])

        with pytest.raises(FileNotFoundError):
            await filesystem.stat(missing_path)

        with pytest.raises(FileNotFoundError):
            async with filesystem.open(missing_path, "rb"):
                pass

    async def test_parse_and_build_uri(self):
        """Test URI parse/build roundtrip for HTTP and HTTPS."""
        http_filesystem = HttpFileSystem.from_uri("http://example.com/alpha")
        https_filesystem = HttpsFileSystem.from_uri("https://example.com/alpha")

        assert (
            http_filesystem.parse_uri("http://example.com/alpha") == "example.com/alpha"
        )
        assert (
            http_filesystem.build_uri("example.com/alpha") == "http://example.com/alpha"
        )

        assert (
            https_filesystem.parse_uri("https://example.com/alpha")
            == "example.com/alpha"
        )
        assert (
            https_filesystem.build_uri("example.com/alpha")
            == "https://example.com/alpha"
        )
