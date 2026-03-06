"""Tests for SFTP retry helpers."""

import asyncio

import asyncssh
import pytest

from aiomegfile.utils.retry.sftp import (
    sftp_retry,
    sftp_should_retry,
    translate_sftp_error,
)

ASYNCSSH_RETRYABLE_ERROR_TYPES = (
    asyncssh.ConnectionLost,
    asyncssh.DisconnectError,
    asyncssh.ChannelOpenError,
    asyncssh.sftp.SFTPConnectionLost,
    asyncssh.sftp.SFTPFailure,
)

ASYNCSSH_NON_RETRYABLE_ERROR_TYPES = (
    asyncssh.sftp.SFTPNoSuchFile,
    asyncssh.sftp.SFTPPermissionDenied,
    asyncssh.sftp.SFTPFileAlreadyExists,
    asyncssh.sftp.SFTPNotADirectory,
)


def _make_asyncssh_error(error_type):
    """Create an instance for a concrete asyncssh exception class.

    :param error_type: asyncssh exception class.
    :return: Instantiated exception.
    :rtype: Exception
    """
    if error_type in (asyncssh.DisconnectError, asyncssh.ChannelOpenError):
        return error_type(1, "transient")
    return error_type("transient")


@pytest.mark.parametrize(
    "error_type",
    ASYNCSSH_RETRYABLE_ERROR_TYPES + ASYNCSSH_NON_RETRYABLE_ERROR_TYPES,
    ids=lambda exc: exc.__name__,
)
def test_asyncssh_sftp_retry_types_are_real(error_type) -> None:
    """Verify every configured asyncssh retry type exists and is usable.

    :param error_type: asyncssh exception class under test.
    """
    assert isinstance(error_type, type)
    assert issubclass(error_type, Exception)
    error = _make_asyncssh_error(error_type)
    assert isinstance(error, error_type)


@pytest.mark.parametrize(
    "error_type",
    ASYNCSSH_RETRYABLE_ERROR_TYPES,
    ids=lambda exc: exc.__name__,
)
def test_sftp_should_retry_all_retryable_asyncssh_errors(error_type) -> None:
    """Verify retryable asyncssh exceptions return ``True``.

    :param error_type: asyncssh retryable exception class.
    """
    assert sftp_should_retry(_make_asyncssh_error(error_type)) is True


@pytest.mark.parametrize(
    "error_type",
    ASYNCSSH_NON_RETRYABLE_ERROR_TYPES,
    ids=lambda exc: exc.__name__,
)
def test_sftp_should_retry_all_non_retryable_asyncssh_errors(error_type) -> None:
    """Verify non-retryable asyncssh exceptions return ``False``.

    :param error_type: asyncssh non-retryable exception class.
    """
    assert sftp_should_retry(_make_asyncssh_error(error_type)) is False


@pytest.mark.parametrize(
    "error",
    [
        asyncio.TimeoutError("timeout"),
        TimeoutError("timeout"),
        ConnectionError("connection lost"),
    ],
    ids=["asyncio_timeout", "builtin_timeout", "connection_error"],
)
def test_sftp_should_retry_builtin_retryable_errors(error: Exception) -> None:
    """Verify built-in retryable exceptions always return ``True``.

    :param error: Exception instance under test.
    """
    assert sftp_should_retry(error) is True


@pytest.mark.parametrize(
    "error",
    [
        OSError("connection lost"),
        OSError("connection reset"),
        OSError("broken pipe"),
        OSError("timed out"),
        OSError("socket is closed"),
        OSError("cannot assign requested address"),
    ],
)
def test_sftp_should_retry_oserror_message_patterns(error: OSError) -> None:
    """Verify known transient OSError messages are retryable.

    :param error: OSError with transient network message.
    """
    assert sftp_should_retry(error) is True


def test_sftp_should_retry_non_retryable_oserror() -> None:
    """Verify unrelated OSError messages are not retryable."""
    assert sftp_should_retry(OSError("permission denied")) is False


def test_sftp_should_retry_non_oserror_when_asyncssh_available() -> None:
    """Verify non-OSError exceptions are not retried when asyncssh is loaded."""
    assert sftp_should_retry(ValueError("invalid")) is False


def test_translate_sftp_error_no_such_file() -> None:
    """Verify ``SFTPNoSuchFile`` maps to ``FileNotFoundError``."""
    uri = "sftp://demo@example.com//missing.txt"
    translated = translate_sftp_error(asyncssh.sftp.SFTPNoSuchFile("missing"), uri)
    assert isinstance(translated, FileNotFoundError)
    assert "No such file" in str(translated)
    assert uri in str(translated)


def test_translate_sftp_error_permission_denied() -> None:
    """Verify ``SFTPPermissionDenied`` maps to ``PermissionError``."""
    uri = "sftp://demo@example.com//denied.txt"
    translated = translate_sftp_error(asyncssh.sftp.SFTPPermissionDenied("denied"), uri)
    assert isinstance(translated, PermissionError)
    assert "Permission denied" in str(translated)
    assert uri in str(translated)


def test_translate_sftp_error_already_exists() -> None:
    """Verify ``SFTPFileAlreadyExists`` maps to ``FileExistsError``."""
    uri = "sftp://demo@example.com//exists.txt"
    translated = translate_sftp_error(
        asyncssh.sftp.SFTPFileAlreadyExists("exists"),
        uri,
    )
    assert isinstance(translated, FileExistsError)
    assert "File exists" in str(translated)
    assert uri in str(translated)


def test_translate_sftp_error_not_a_directory() -> None:
    """Verify ``SFTPNotADirectory`` maps to ``NotADirectoryError``."""
    uri = "sftp://demo@example.com//file.txt"
    translated = translate_sftp_error(asyncssh.sftp.SFTPNotADirectory("not-dir"), uri)
    assert isinstance(translated, NotADirectoryError)
    assert "Not a directory" in str(translated)
    assert uri in str(translated)


def test_translate_sftp_error_timeout() -> None:
    """Verify timeout errors are translated with URI context."""
    uri = "sftp://demo@example.com//timeout.txt"
    translated = translate_sftp_error(TimeoutError("timeout"), uri)
    assert isinstance(translated, TimeoutError)
    assert "Operation timed out" in str(translated)
    assert uri in str(translated)


def test_translate_sftp_error_passthrough_oserror() -> None:
    """Verify plain ``OSError`` is returned unchanged."""
    error = OSError("socket is closed")
    translated = translate_sftp_error(error, "sftp://demo@example.com//a.txt")
    assert translated is error


def test_translate_sftp_error_passthrough_builtin_error() -> None:
    """Verify builtin file-related errors are returned unchanged."""
    error = FileNotFoundError("missing")
    translated = translate_sftp_error(error, "sftp://demo@example.com//a.txt")
    assert translated is error


def test_translate_sftp_error_unknown_error_to_oserror() -> None:
    """Verify unknown non-OSError becomes wrapped ``OSError``."""
    uri = "sftp://demo@example.com//a.txt"
    translated = translate_sftp_error(ValueError("invalid"), uri)
    assert isinstance(translated, OSError)
    assert "SFTP operation failed" in str(translated)
    assert uri in str(translated)


async def test_sftp_retry_decorator_retries_once() -> None:
    """Verify retry decorator re-runs coroutine on transient failures."""
    state = {"calls": 0}

    @sftp_retry(max_retries=2)
    async def _flaky() -> str:
        """Fail once before succeeding.

        :return: Static success marker.
        :rtype: str
        """
        state["calls"] += 1
        if state["calls"] == 1:
            raise TimeoutError("transient")
        return "ok"

    assert await _flaky() == "ok"
    assert state["calls"] == 2
