"""SFTP protocol retry and error translation helpers."""

from __future__ import annotations

import asyncio

import asyncssh

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.errors.core import aioretry

__all__ = [
    "SftpException",
    "SftpFileExistsError",
    "SftpFileNotFoundError",
    "SftpNotADirectoryError",
    "SftpPermissionError",
    "SftpTimeoutError",
    "SftpUnknownError",
    "sftp_retry",
    "sftp_should_retry",
    "translate_sftp_error",
]


class SftpException(Exception):
    """Base type for SFTP-specific errors."""


class SftpFileNotFoundError(SftpException, FileNotFoundError):
    """Raised when SFTP resource does not exist."""


class SftpFileExistsError(SftpException, FileExistsError):
    """Raised when creating an SFTP resource that already exists."""


class SftpPermissionError(SftpException, PermissionError):
    """Raised when SFTP access is denied."""


class SftpNotADirectoryError(SftpException, NotADirectoryError):
    """Raised when path is not a directory on SFTP server."""


class SftpTimeoutError(SftpException, TimeoutError):
    """Raised when SFTP operation times out."""


class SftpUnknownError(SftpException, OSError):
    """Raised for unmapped SFTP failures."""


def sftp_should_retry(error: Exception) -> bool:
    """Return whether an SFTP exception should trigger retry.

    :param error: Exception raised by SFTP operation.
    :return: True if operation should be retried.
    :rtype: bool
    """
    if isinstance(error, (asyncio.TimeoutError, ConnectionError, TimeoutError)):
        return True

    non_retry_errors = (
        asyncssh.sftp.SFTPNoSuchFile,
        asyncssh.sftp.SFTPPermissionDenied,
        asyncssh.sftp.SFTPFileAlreadyExists,
        asyncssh.sftp.SFTPNotADirectory,
    )
    if isinstance(error, non_retry_errors):
        return False

    retry_errors = (
        asyncssh.ConnectionLost,
        asyncssh.DisconnectError,
        asyncssh.ChannelOpenError,
        asyncssh.sftp.SFTPConnectionLost,
        asyncssh.sftp.SFTPFailure,
    )
    if isinstance(error, retry_errors):
        return True

    if isinstance(error, OSError):
        retry_messages = (
            "connection lost",
            "connection reset",
            "broken pipe",
            "timed out",
            "socket is closed",
            "cannot assign requested address",
        )
        message = str(error).lower()
        return any(item in message for item in retry_messages)

    return False


def translate_sftp_error(error: Exception, uri: str) -> Exception:
    """Translate asyncssh SFTP errors to filesystem-like exceptions.

    :param error: Original exception raised by SFTP operation.
    :param uri: URI used in the failed operation.
    :return: Translated exception.
    :rtype: Exception
    """
    if isinstance(error, SftpException):
        return error

    if isinstance(error, (FileNotFoundError, FileExistsError, PermissionError)):
        return error

    if isinstance(error, asyncssh.sftp.SFTPNoSuchFile):
        return SftpFileNotFoundError(f"No such file: {uri!r}")
    if isinstance(error, asyncssh.sftp.SFTPFileAlreadyExists):
        return SftpFileExistsError(f"File exists: {uri!r}")
    if isinstance(error, asyncssh.sftp.SFTPPermissionDenied):
        return SftpPermissionError(f"Permission denied: {uri!r}")
    if isinstance(error, asyncssh.sftp.SFTPNotADirectory):
        return SftpNotADirectoryError(f"Not a directory: {uri!r}")

    if isinstance(error, TimeoutError):
        return SftpTimeoutError(f"Operation timed out: {uri!r}")

    if isinstance(error, OSError):
        return error

    return SftpUnknownError(f"SFTP operation failed on {uri!r}: {error}")


def sftp_retry(max_retries: int = DEFAULT_MAX_RETRY_TIMES):
    """Return retry decorator configured for SFTP operations.

    :param max_retries: Maximum retry attempts.
    :return: Retry decorator for async functions.
    """
    return aioretry(
        should_retry=sftp_should_retry,
        max_retries=max_retries,
    )
