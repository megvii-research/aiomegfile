"""Tests for AioSftpPrefetchReader."""

import pytest

from aiomegfile.filesystem.sftp import SftpFileSystem
from aiomegfile.lib.prefetch_reader.sftp_prefetch_reader import AioSftpPrefetchReader
from tests.utils.fake_sftp import FakeSFTPClient, FakeSSHConnection


@pytest.fixture
def fake_filesystem(monkeypatch):
    """Create a filesystem backed by fake SFTP client.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[SftpFileSystem, FakeSFTPClient]
    """
    client = FakeSFTPClient()
    client.files["/sample.txt"] = b"line1\nline2\nline3\n"

    filesystem = SftpFileSystem(host="example.com", username="demo")

    async def _open_client():
        return FakeSSHConnection(), client

    monkeypatch.setattr(filesystem, "_open_client", _open_client)

    return filesystem, client


class TestAioSftpPrefetchReader:
    """Test cases for AioSftpPrefetchReader."""

    async def test_read_binary(self, fake_filesystem):
        """Test full binary read with prefetch logic."""
        filesystem, _ = fake_filesystem

        async with AioSftpPrefetchReader(
            "//sample.txt",
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

        async with AioSftpPrefetchReader(
            "//sample.txt",
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
            async with AioSftpPrefetchReader(
                "//not-found.txt",
                filesystem=filesystem,
                mode="rb",
                block_size=4,
                max_buffer_size=16,
            ):
                pass
