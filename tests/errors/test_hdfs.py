"""Tests for HDFS error translation helpers."""

from __future__ import annotations

from aiomegfile.errors.hdfs import (
    HdfsFileExistsError,
    HdfsFileNotFoundError,
    HdfsInvalidError,
    HdfsIsADirectoryError,
    HdfsNotADirectoryError,
    HdfsPermissionError,
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
