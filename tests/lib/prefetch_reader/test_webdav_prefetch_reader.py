"""Tests for ``AioWebdavPrefetchReader``."""

import pytest

from aiomegfile.filesystem.webdav import WebdavFileSystem
from aiomegfile.lib.prefetch_reader.webdav_prefetch_reader import (
    AioWebdavPrefetchReader,
)
from tests.utils.fake_webdav import FakeAiodavResponse, FakeWebdavClient


@pytest.fixture
def fake_filesystem(monkeypatch):
    """Create a WebDAV filesystem backed by fake client.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[WebdavFileSystem, FakeWebdavClient]
    """
    client = FakeWebdavClient()
    client.dirs.add("/dir")
    client.files["/dir/sample.txt"] = b"line1\nline2\nline3\n"

    filesystem = WebdavFileSystem(host="example.com", username="demo", password="pwd")
    monkeypatch.setattr(filesystem, "_create_client", lambda: client)
    return filesystem, client


class TestAioWebdavPrefetchReader:
    """Test cases for ``AioWebdavPrefetchReader``."""

    async def test_read_binary(self, fake_filesystem):
        """Test full binary read with prefetch logic."""
        filesystem, _ = fake_filesystem

        async with AioWebdavPrefetchReader(
            "/dir/sample.txt",
            filesystem=filesystem,
            mode="rb",
            block_size=5,
            max_buffer_size=20,
        ) as reader:
            data = await reader.read()

        assert data == b"line1\nline2\nline3\n"

    async def test_text_seek_and_readline(self, fake_filesystem):
        """Test text mode line reading and seek."""
        filesystem, _ = fake_filesystem

        async with AioWebdavPrefetchReader(
            "/dir/sample.txt",
            filesystem=filesystem,
            mode="r",
            encoding="utf-8",
            block_size=4,
            max_buffer_size=16,
        ) as reader:
            first_line = await reader.readline()
            assert first_line == "line1\n"

            await reader.seek(0)
            first_five = await reader.read(5)
            assert first_five == "line1"

    async def test_missing_file_raises(self, fake_filesystem):
        """Test missing file raises FileNotFoundError."""
        filesystem, _ = fake_filesystem

        with pytest.raises(FileNotFoundError):
            async with AioWebdavPrefetchReader(
                "/dir/not-found.txt",
                filesystem=filesystem,
                mode="rb",
            ):
                pass

    async def test_non_range_server_raises(self, fake_filesystem, monkeypatch):
        """Test non-range servers are rejected by prefetch reader."""
        filesystem, client = fake_filesystem

        async def _download_without_range(
            action: str,
            path: str,
            data=None,
            headers_ext=None,
        ):
            _ = action, data
            normalized = client._normalize(path)
            content = client.files[normalized]
            if headers_ext and "Range" in headers_ext:
                return FakeAiodavResponse(
                    body=content,
                    status=200,
                    headers={"Content-Length": str(len(content))},
                )
            return FakeAiodavResponse(
                body=content,
                status=200,
                headers={"Content-Length": str(len(content))},
            )

        monkeypatch.setattr(client, "_execute_request", _download_without_range)

        with pytest.raises(OSError, match="byte-range"):
            async with AioWebdavPrefetchReader(
                "/dir/sample.txt",
                filesystem=filesystem,
                mode="rb",
            ):
                pass
