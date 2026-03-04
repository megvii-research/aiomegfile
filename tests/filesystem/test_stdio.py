"""Tests for StdioFileSystem."""

import io
import sys

import pytest

from aiomegfile.filesystem.stdio import StdioFileSystem


class _FakeStdin:
    """Fake ``sys.stdin`` implementation for tests."""

    def __init__(self, text_data: str, binary_data: bytes) -> None:
        """Initialize fake stdin.

        :param text_data: Text content for text mode.
        :param binary_data: Binary content for binary mode.
        """
        self._text = io.StringIO(text_data)
        self.buffer = io.BytesIO(binary_data)

    def read(self, size: int = -1) -> str:
        """Read text data.

        :param size: Maximum size to read.
        :return: Read text.
        :rtype: str
        """
        return self._text.read(size)

    def readline(self, size: int = -1) -> str:
        """Read one text line.

        :param size: Maximum size to read.
        :return: Read line.
        :rtype: str
        """
        return self._text.readline(size)


class _FakeStdout:
    """Fake ``sys.stdout`` / ``sys.stderr`` implementation for tests."""

    def __init__(self) -> None:
        """Initialize fake std output streams."""
        self._text = io.StringIO()
        self.buffer = io.BytesIO()

    def write(self, data: str) -> int:
        """Write text data.

        :param data: Text to write.
        :return: Number of written characters.
        :rtype: int
        """
        return self._text.write(data)

    def flush(self) -> None:
        """Flush output."""
        return None

    @property
    def text(self) -> str:
        """Return captured text output.

        :return: Captured text output.
        :rtype: str
        """
        return self._text.getvalue()


class TestStdioFileSystem:
    """Test cases for StdioFileSystem."""

    def _create_filesystem(self) -> StdioFileSystem:
        """Create a StdioFileSystem instance for tests.

        :return: Filesystem instance.
        :rtype: StdioFileSystem
        """
        return StdioFileSystem()

    async def test_open_reader_binary(self, monkeypatch) -> None:
        """Reader should consume bytes from ``sys.stdin.buffer``.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        fake_stdin = _FakeStdin("ignored\n", b"hello\nworld\n")
        monkeypatch.setattr(sys, "stdin", fake_stdin)

        filesystem = self._create_filesystem()
        async with filesystem.open("-", "rb") as reader:
            assert reader.name == "stdin"
            assert reader.mode == "rb"
            assert await reader.read() == b"hello\nworld\n"

    async def test_open_reader_text(self, monkeypatch) -> None:
        """Reader should consume text from ``sys.stdin``.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        fake_stdin = _FakeStdin("line1\nline2\n", b"ignored\n")
        monkeypatch.setattr(sys, "stdin", fake_stdin)

        filesystem = self._create_filesystem()
        async with filesystem.open("-", "r") as reader:
            assert await reader.readline() == "line1\n"
            assert await reader.read() == "line2\n"

    async def test_open_writer_binary_stdout_and_stderr(self, monkeypatch) -> None:
        """Writer should route bytes to stdout or stderr buffer.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        fake_stdout = _FakeStdout()
        fake_stderr = _FakeStdout()
        monkeypatch.setattr(sys, "stdout", fake_stdout)
        monkeypatch.setattr(sys, "stderr", fake_stderr)

        filesystem = self._create_filesystem()
        async with filesystem.open("-", "wb") as writer:
            assert writer.name == "stdout"
            assert writer.mode == "wb"
            await writer.write(b"hello")
        assert fake_stdout.buffer.getvalue() == b"hello"

        async with filesystem.open("2", "wb") as writer:
            assert writer.name == "stderr"
            await writer.write(b"error")
        assert fake_stderr.buffer.getvalue() == b"error"

    async def test_open_writer_text(self, monkeypatch) -> None:
        """Writer should route text to stdout text stream.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        fake_stdout = _FakeStdout()
        monkeypatch.setattr(sys, "stdout", fake_stdout)

        filesystem = self._create_filesystem()
        async with filesystem.open("-", "w") as writer:
            await writer.write("hello")
        assert fake_stdout.text == "hello"

    async def test_close_marks_stream_closed(self, monkeypatch) -> None:
        """Closed stdio writer should reject further writes.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        fake_stdout = _FakeStdout()
        monkeypatch.setattr(sys, "stdout", fake_stdout)

        filesystem = self._create_filesystem()
        writer = filesystem.open("-", "wb")
        await writer.close()
        assert writer.closed

        with pytest.raises(IOError, match="file already closed"):
            await writer.write(b"x")

    async def test_open_validation_errors(self) -> None:
        """Open should reject invalid mode or path."""
        filesystem = self._create_filesystem()

        with pytest.raises(ValueError, match="unacceptable mode"):
            filesystem.open("-", "io")
        with pytest.raises(ValueError, match="unacceptable path"):
            filesystem.open("a", "rb")
        with pytest.raises(ValueError, match="cannot open for reading"):
            filesystem.open("1", "rb")
        with pytest.raises(ValueError, match="cannot open for writing"):
            filesystem.open("0", "wb")

    async def test_exists_is_file_and_stat(self) -> None:
        """Filesystem metadata methods should reflect supported stdio paths."""
        filesystem = self._create_filesystem()

        assert await filesystem.exists("-")
        assert await filesystem.exists("0")
        assert await filesystem.is_file("1")
        assert not await filesystem.is_dir("-")
        assert not await filesystem.exists("unknown")

        with pytest.raises(OSError, match="does not support stat"):
            await filesystem.stat("-")
        with pytest.raises(OSError, match="does not support stat"):
            await filesystem.stat("unknown")

    async def test_unsupported_directory_and_remove_operations(self) -> None:
        """Directory and remove operations should be rejected."""
        filesystem = self._create_filesystem()

        with pytest.raises(NotADirectoryError):
            filesystem.scandir("-")
        with pytest.raises(NotADirectoryError):
            filesystem.scanfile("-")
        with pytest.raises(NotADirectoryError):
            await filesystem.mkdir("-")
        with pytest.raises(OSError, match="cannot be removed"):
            await filesystem.remove("-")

    async def test_parse_build_and_same_endpoint(self) -> None:
        """Parse/build URI helpers and endpoint comparison should work."""
        filesystem = self._create_filesystem()
        other = self._create_filesystem()

        assert filesystem.parse_uri("stdio://-") == "-"
        assert filesystem.build_uri("2") == "stdio://2"
        assert filesystem.same_endpoint(other)
        assert await filesystem.samefile("-", "-")
        assert not await filesystem.samefile("-", "2")
