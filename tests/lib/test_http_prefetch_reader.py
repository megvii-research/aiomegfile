"""Tests for AioHttpPrefetchReader."""

import re
import threading
from functools import partial
from http.server import (
    BaseHTTPRequestHandler,
    SimpleHTTPRequestHandler,
    ThreadingHTTPServer,
)

import pytest

from aiomegfile.lib.http_prefetch_reader import AioHttpPrefetchReader


@pytest.fixture
def range_http_server():
    """Start a local HTTP server that supports range requests."""
    content = b"line1\nline2\nline3\n"

    class RangeHandler(BaseHTTPRequestHandler):
        """HTTP handler with byte-range support for a single file."""

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
            """Send common HTTP headers.

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
            end = int(match.group(2))
            if start >= len(content):
                self.send_response(416)
                self.end_headers()
                return

            end = min(end, len(content) - 1)
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

    try:
        yield {
            "url": f"http://127.0.0.1:{server.server_port}/data.txt",
            "content": content,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture
def plain_http_server(tmp_path):
    """Start a local HTTP server without range support."""
    content = b"plain content for no range server"
    file_path = tmp_path / "data.txt"
    file_path.write_bytes(content)

    handler = partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield {
            "url": f"http://127.0.0.1:{server.server_port}/data.txt",
            "content": content,
            "missing_url": f"http://127.0.0.1:{server.server_port}/missing.txt",
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


class TestAioHttpPrefetchReader:
    """Tests for AioHttpPrefetchReader."""

    async def test_read_binary_with_range_support(self, range_http_server):
        """Test full binary read for a range-capable server."""
        async with AioHttpPrefetchReader(
            range_http_server["url"],
            mode="rb",
            block_size=5,
            max_buffer_size=20,
        ) as reader:
            content = await reader.read()

        assert content == range_http_server["content"]

    async def test_text_seek_and_readline(self, range_http_server):
        """Test text mode line reading and seek operations."""
        async with AioHttpPrefetchReader(
            range_http_server["url"],
            mode="r",
            encoding="utf-8",
            block_size=4,
            max_buffer_size=16,
        ) as reader:
            first_line = await reader.readline()
            assert first_line == "line1\n"

            await reader.seek(0)
            first_five_chars = await reader.read(5)
            assert first_five_chars == "line1"

    async def test_read_without_range_support_raises(self, plain_http_server):
        """Test non-range servers are rejected by prefetch reader."""
        with pytest.raises(OSError, match="byte-range"):
            async with AioHttpPrefetchReader(
                plain_http_server["url"],
                mode="rb",
                block_size=4,
                max_buffer_size=8,
            ):
                pass

    async def test_missing_url_raises_file_not_found(self, plain_http_server):
        """Test missing URL raises FileNotFoundError during initialization."""
        with pytest.raises(FileNotFoundError):
            async with AioHttpPrefetchReader(plain_http_server["missing_url"]):
                pass
