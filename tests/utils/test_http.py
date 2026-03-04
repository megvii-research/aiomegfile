"""Tests for HTTP utility helpers."""

import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from aiomegfile.utils.http import (
    parse_content_length,
    parse_total_size_from_headers,
    request_headers,
)


def test_parse_content_length_invalid_values():
    """Test invalid ``Content-Length`` values return ``None``."""
    assert parse_content_length(None) is None
    assert parse_content_length("-1") is None
    assert parse_content_length("invalid") is None
    assert parse_content_length("12") == 12


def test_parse_total_size_from_headers_prefers_content_range():
    """Test total size parsing prefers ``Content-Range`` when available."""
    headers = {
        "Content-Range": "bytes 0-0/1234",
        "Content-Length": "1",
    }
    assert parse_total_size_from_headers(headers) == 1234


async def test_request_headers_fallback_to_range_probe():
    """Test ``request_headers`` uses range probe when ``HEAD`` is not allowed."""
    content = b"hello world"

    class Head405RangeHandler(BaseHTTPRequestHandler):
        """HTTP handler with ``HEAD`` 405 and range-capable ``GET``."""

        def do_HEAD(self) -> None:
            """Handle HEAD request."""
            self.send_response(405)
            self.end_headers()

        def do_GET(self) -> None:
            """Handle GET request with optional range support."""
            range_header = self.headers.get("Range")
            if not range_header:
                self.send_response(200)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

            match = re.match(r"^bytes=(\d+)-(\d+)$", range_header)
            if match is None:
                self.send_response(416)
                self.end_headers()
                return

            start = int(match.group(1))
            end = min(int(match.group(2)), len(content) - 1)
            body = content[start : end + 1]

            self.send_response(206)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(content)}")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args) -> None:
            """Suppress request logs for deterministic test output."""
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Head405RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        url = f"http://127.0.0.1:{server.server_port}/data.txt"
        headers, status_code = await request_headers(url, timeout=10)
        assert status_code == 200
        assert headers["Content-Length"] == "11"
        assert parse_total_size_from_headers(headers) == len(content)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
