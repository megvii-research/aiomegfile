"""Tests for AioHdfsPrefetchReader."""

from __future__ import annotations

import io

import pytest

from aiomegfile.errors.hdfs import HdfsIsADirectoryError
from aiomegfile.filesystem.hdfs import HdfsFileSystem
from aiomegfile.lib.prefetch_reader.hdfs_prefetch_reader import AioHdfsPrefetchReader
from tests.utils.fake_hdfs import FakeHdfsClient


@pytest.fixture
def fake_filesystem(monkeypatch):
    """Create a filesystem backed by a fake HDFS client.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[HdfsFileSystem, FakeHdfsClient]
    """
    client = FakeHdfsClient(root="/workspace")
    client._store_file("sample.txt", b"line1\nline2\nline3\n")

    filesystem = HdfsFileSystem()
    monkeypatch.setattr(
        "aiomegfile.filesystem.hdfs.get_hdfs_client",
        lambda _=None: client,
    )

    return filesystem, client


class TestAioHdfsPrefetchReader:
    """Test cases for AioHdfsPrefetchReader."""

    async def test_read_binary(self, fake_filesystem) -> None:
        """Test full binary read with prefetch logic.

        :param fake_filesystem: Fake filesystem fixture.
        """
        filesystem, _client = fake_filesystem

        async with AioHdfsPrefetchReader(
            "sample.txt",
            filesystem=filesystem,
            mode="rb",
            block_size=5,
            max_buffer_size=20,
        ) as reader:
            data = await reader.read()

        assert data == b"line1\nline2\nline3\n"

    async def test_text_seek_and_readline(self, fake_filesystem) -> None:
        """Test text mode line reading and seek.

        :param fake_filesystem: Fake filesystem fixture.
        """
        filesystem, _client = fake_filesystem

        async with AioHdfsPrefetchReader(
            "sample.txt",
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

    async def test_missing_file_raises(self, fake_filesystem) -> None:
        """Test missing file raises ``FileNotFoundError``.

        :param fake_filesystem: Fake filesystem fixture.
        """
        filesystem, _client = fake_filesystem

        with pytest.raises(FileNotFoundError):
            async with AioHdfsPrefetchReader(
                "not-found.txt",
                filesystem=filesystem,
                mode="rb",
                block_size=4,
                max_buffer_size=16,
            ):
                pass

    async def test_name_empty_file_and_empty_range(self, fake_filesystem) -> None:
        """Test helper branches for name and empty responses.

        :param fake_filesystem: Fake filesystem fixture.
        """
        filesystem, client = fake_filesystem
        client._store_file("empty.txt", b"")

        async with AioHdfsPrefetchReader(
            "empty.txt",
            filesystem=filesystem,
            mode="rb",
            block_size=4,
            max_buffer_size=16,
        ) as reader:
            assert reader.name == "hdfs://empty.txt"
            assert await reader.read() == b""
            response = await reader._fetch_response(start=2, end=1)
            assert response["ContentLength"] == 0
            assert response["Body"].read() == b""

    async def test_directory_and_text_response_branches(
        self,
        fake_filesystem,
        monkeypatch,
    ) -> None:
        """Test directory errors and string-to-bytes conversion.

        :param fake_filesystem: Fake filesystem fixture.
        :param monkeypatch: Pytest monkeypatch fixture.
        """
        filesystem, client = fake_filesystem
        client.makedirs("dir-only")

        with pytest.raises(HdfsIsADirectoryError):
            async with AioHdfsPrefetchReader(
                "dir-only",
                filesystem=filesystem,
                mode="rb",
                block_size=4,
                max_buffer_size=16,
            ):
                pass

        class TextReadContext:
            """Context manager returning text content."""

            def __enter__(self) -> io.StringIO:
                """Return text buffer.

                :return: Text buffer.
                :rtype: io.StringIO
                """
                return io.StringIO("abc")

            def __exit__(self, exc_type, exc_value, traceback) -> None:
                """No-op context exit."""

        monkeypatch.setattr(client, "read", lambda *args, **kwargs: TextReadContext())
        async with AioHdfsPrefetchReader(
            "sample.txt",
            filesystem=filesystem,
            mode="rb",
            block_size=4,
            max_buffer_size=16,
        ) as reader:
            response = await reader._fetch_response(start=0, end=2)
            assert response["Body"].read() == b"abc"
