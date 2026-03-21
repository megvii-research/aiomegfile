"""Tests for HDFS error translation helpers."""

from __future__ import annotations

from aiomegfile.errors.hdfs import (
    HdfsFileExistsError,
    HdfsFileNotFoundError,
    HdfsInvalidError,
    HdfsIsADirectoryError,
    HdfsNotADirectoryError,
    HdfsPermissionError,
    HdfsTimeoutError,
    HdfsUnknownError,
    hdfs_retry,
    hdfs_should_retry,
    translate_hdfs_error,
)
from tests.utils.fake_hdfs import FakeHdfsError


def test_translate_hdfs_error_uses_hdfs_specific_types() -> None:
    """HDFS translation should return HDFS-specific exception classes."""
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="Path is not a file"),
            "hdfs://data/dir",
        ),
        HdfsIsADirectoryError,
    )
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="Path is not a directory"),
            "hdfs://data/file",
        ),
        HdfsNotADirectoryError,
    )
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="missing", status_code=404),
            "hdfs://data/missing",
        ),
        HdfsFileNotFoundError,
    )
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="exists", status_code=409),
            "hdfs://data/existing",
        ),
        HdfsFileExistsError,
    )
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="denied", status_code=403),
            "hdfs://data/file",
        ),
        HdfsPermissionError,
    )
    assert isinstance(
        translate_hdfs_error(
            FakeHdfsError(message="bad request", status_code=400),
            "hdfs://data/file",
        ),
        HdfsInvalidError,
    )


def test_translate_stdlib_error_to_hdfs_error() -> None:
    """Standard library exceptions should be wrapped as HDFS-specific ones."""
    assert isinstance(
        translate_hdfs_error(FileNotFoundError("missing"), "hdfs://data/missing"),
        HdfsFileNotFoundError,
    )
    assert isinstance(
        translate_hdfs_error(IsADirectoryError("dir"), "hdfs://data/dir"),
        HdfsIsADirectoryError,
    )


def test_hdfs_should_retry_conditions() -> None:
    """Retry helper should classify retryable HDFS failures."""
    assert hdfs_should_retry(TimeoutError("timed out")) is True
    assert hdfs_should_retry(FakeHdfsError("busy", status_code=503)) is True
    assert hdfs_should_retry(RuntimeError("connection reset by peer")) is True
    assert hdfs_should_retry(RuntimeError("permanent failure")) is False


async def test_hdfs_retry_retries_once() -> None:
    """Retry decorator should retry retryable HDFS errors."""
    attempts = []

    @hdfs_retry(max_retries=2)
    async def flaky() -> str:
        """Return success after one retry.

        :return: Success marker.
        :rtype: str
        """
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError("timed out")
        return "ok"

    assert await flaky() == "ok"
    assert len(attempts) == 2


def test_translate_hdfs_error_misc_cases() -> None:
    """Miscellaneous translation branches should return HDFS-specific types."""
    timeout_error = translate_hdfs_error(TimeoutError("slow"), "hdfs://data/file")
    assert isinstance(timeout_error, HdfsTimeoutError)

    invalid_error = translate_hdfs_error(ValueError("bad"), "hdfs://data/file")
    assert isinstance(invalid_error, HdfsInvalidError)

    unknown_error = translate_hdfs_error(OSError("boom"), "hdfs://data/file")
    assert isinstance(unknown_error, HdfsUnknownError)
