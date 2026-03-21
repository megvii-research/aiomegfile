"""Tests for AioHdfsPrefetchReader."""

from __future__ import annotations

import pytest

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
