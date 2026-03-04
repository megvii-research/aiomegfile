"""Tests for aiomegfile.stdio module."""

import io
import sys

import pytest

from aiomegfile.stdio import is_stdio, stdio_open


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
    """Fake ``sys.stdout`` implementation for tests."""

    def __init__(self) -> None:
        """Initialize fake stdout."""
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


def test_is_stdio() -> None:
    """is_stdio should recognize stdio paths."""
    assert is_stdio("stdio://-")
    assert is_stdio("stdio://2")
    assert not is_stdio("s3://bucket/key")
    assert not is_stdio("/tmp/file.txt")


async def test_stdio_open_reader(monkeypatch) -> None:
    """stdio_open should read from stdin for stdio URI.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    fake_stdin = _FakeStdin("line1\nline2\n", b"bytes\n")
    monkeypatch.setattr(sys, "stdin", fake_stdin)

    async with stdio_open("stdio://-", "r") as reader:
        assert await reader.readline() == "line1\n"
        assert await reader.read() == "line2\n"


async def test_stdio_open_writer(monkeypatch) -> None:
    """stdio_open should write to stdout for stdio URI.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    fake_stdout = _FakeStdout()
    monkeypatch.setattr(sys, "stdout", fake_stdout)

    async with stdio_open("stdio://-", "w") as writer:
        await writer.write("hello")

    assert fake_stdout.text == "hello"


async def test_stdio_open_invalid_path() -> None:
    """stdio_open should reject non-stdio paths."""
    with pytest.raises(ValueError, match="unacceptable path"):
        stdio_open("file:///tmp/test.txt", "r")
