"""Tests for error translation and retry helpers."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError

from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3ConfigError,
    S3FileNotFoundError,
    S3InvalidRangeError,
    S3PermissionError,
    S3UnknownError,
    aioretry,
    full_class_name,
    full_error_message,
    raise_s3_error,
    translate_s3_error,
)


def _make_client_error(code: str, message: str = "boom") -> ClientError:
    """Create a botocore ClientError with the provided code.

    :param code: Error code to embed.
    :param message: Error message to embed.
    :return: ClientError instance.
    :rtype: ClientError
    """
    return ClientError(
        error_response={"Error": {"Code": code, "Message": message}},
        operation_name="Op",
    )


def test_full_error_message_formats_class_name() -> None:
    """full_error_message should include class name and details.

    :return: None
    :rtype: None
    """
    err = ValueError("bad")
    assert full_class_name(err) == "ValueError"
    message = full_error_message(err)
    assert "ValueError" in message
    assert "bad" in message


@pytest.mark.parametrize(
    ("code", "expected_type"),
    [
        ("NoSuchBucket", S3BucketNotFoundError),
        ("NoSuchKey", S3FileNotFoundError),
        ("404", S3FileNotFoundError),
        ("AccessDenied", S3PermissionError),
        ("403", S3PermissionError),
        ("InvalidAccessKeyId", S3ConfigError),
        ("SignatureDoesNotMatch", S3ConfigError),
        ("InvalidRange", S3InvalidRangeError),
    ],
)
def test_translate_s3_error_client_codes(code: str, expected_type: type[Exception]):
    """Translate client errors to specific S3 exceptions.

    :param code: Botocore error code to test.
    :param expected_type: Expected exception type.
    :return: None
    :rtype: None
    """
    error = _make_client_error(code)
    translated = translate_s3_error(error, "bucket/key")
    assert isinstance(translated, expected_type)


def test_translate_s3_error_unknown_client_error():
    """Unknown client error should map to S3UnknownError.

    :return: None
    :rtype: None
    """
    error = _make_client_error("WeirdError")
    translated = translate_s3_error(error, "bucket/key")
    assert isinstance(translated, S3UnknownError)


def test_translate_s3_error_param_validation_bucket():
    """ParamValidationError for bucket name should map to bucket error.

    :return: None
    :rtype: None
    """
    error = ParamValidationError(report="Invalid bucket name")
    translated = translate_s3_error(error, "bucket/key")
    assert isinstance(translated, S3BucketNotFoundError)


def test_translate_s3_error_param_validation_key():
    """ParamValidationError for key length should map to file error.

    :return: None
    :rtype: None
    """
    error = ParamValidationError(report="Invalid length for parameter Key")
    translated = translate_s3_error(error, "bucket/key")
    assert isinstance(translated, S3FileNotFoundError)


def test_translate_s3_error_no_credentials():
    """NoCredentialsError should map to S3ConfigError.

    :return: None
    :rtype: None
    """
    translated = translate_s3_error(NoCredentialsError(), "bucket/key")
    assert isinstance(translated, S3ConfigError)


def test_raise_s3_error_suppressed():
    """raise_s3_error should suppress when callback returns True.

    :return: None
    :rtype: None
    """
    with raise_s3_error("bucket/key", suppress_error_callback=lambda err: True):
        raise _make_client_error("NoSuchKey")


def test_raise_s3_error_raises_translated():
    """raise_s3_error should raise translated errors.

    :return: None
    :rtype: None
    """
    with pytest.raises(S3FileNotFoundError):
        with raise_s3_error("bucket/key"):
            raise _make_client_error("NoSuchKey")


async def test_aioretry_retries_and_callbacks():
    """aioretry should retry and invoke callbacks.

    :return: None
    :rtype: None
    """
    calls: dict[str, int] = {"attempts": 0, "before": 0, "after": 0, "retry": 0}

    async def before() -> None:
        """Track before-callback invocation.

        :return: None
        :rtype: None
        """
        calls["before"] += 1

    async def after(result: str) -> str:
        """Track after-callback invocation.

        :param result: Result to return.
        :return: Result unchanged.
        :rtype: str
        """
        calls["after"] += 1
        return result

    async def on_retry(error: Exception) -> None:
        """Track retry callback invocation.

        :param error: Exception that triggered retry.
        :return: None
        :rtype: None
        """
        calls["retry"] += 1

    @aioretry(
        lambda exc: isinstance(exc, ValueError),
        max_retries=3,
        before_callback=before,
        after_callback=after,
        retry_callback=on_retry,
    )
    async def flaky() -> str:
        """Fail once then succeed.

        :return: Success marker.
        :rtype: str
        """
        calls["attempts"] += 1
        if calls["attempts"] < 2:
            raise ValueError("fail")
        return "ok"

    result = await flaky()
    assert result == "ok"
    assert calls["attempts"] == 2
    assert calls["before"] == 1
    assert calls["after"] == 1
    assert calls["retry"] == 1


async def test_aioretry_does_not_retry_on_non_retryable():
    """aioretry should not retry when should_retry is False.

    :return: None
    :rtype: None
    """

    @aioretry(lambda exc: False, max_retries=2)
    async def always_fails() -> None:
        """Always raise a KeyError.

        :return: None
        :rtype: None
        """
        raise KeyError("stop")

    with pytest.raises(KeyError):
        await always_fails()
