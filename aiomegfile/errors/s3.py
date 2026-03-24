"""S3 protocol errors, translation utilities, and retry helpers."""

from __future__ import annotations

import logging
import typing as T
from contextlib import contextmanager
from shutil import SameFileError

import botocore
import requests
import urllib3
from aiobotocore.retries.standard import (
    AioRetryHandler,
    AioRetryPolicy,
    AioStandardRetryConditions,
)
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from botocore.retries.standard import (
    ExponentialBackoff,
    MaxAttemptsChecker,
    RetryEventAdapter,
    RetryQuotaChecker,
    quota,
)

from aiomegfile.config import S3_MAX_RETRY_TIMES
from aiomegfile.errors.core import UnknownError
from aiomegfile.utils.path import PathLike

if T.TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client  # pyre-ignore[21]

logger = logging.getLogger(__name__)

DEFAULT_RETRY_CAPACITY = 1000

S3_RETRY_EXCEPTIONS = (
    botocore.exceptions.IncompleteReadError,
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ProxyConnectionError,
    botocore.exceptions.ConnectionClosedError,
    botocore.exceptions.ResponseStreamingError,
    botocore.exceptions.SSLError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectTimeout,
    urllib3.exceptions.IncompleteRead,
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.HeaderParsingError,
)

S3_RETRY_ERROR_CODES = (
    "429",  # TOS ExceedAccountQPSLimit
    "499",  # Cloud providers may send 499 when idle timeout happens.
    "500",
    "501",
    "502",
    "503",
    "InternalError",
    "ServiceUnavailable",
    "SlowDown",
    "ContextCanceled",
    "Timeout",  # TOS Timeout
    "RequestTimeout",
    "RequestTimeTooSkewed",
    "ExceedAccountQPSLimit",
    "ExceedAccountRateLimit",
    "ExceedBucketQPSLimit",
    "ExceedBucketRateLimit",
    "DownloadTrafficRateLimitExceeded",  # OSS RateLimitExceeded
    "UploadTrafficRateLimitExceeded",
    "MetaOperationQpsLimitExceeded",
    "TotalQpsLimitExceeded",
    "PartitionQpsLimitted",
    "ActiveRequestLimitExceeded",
    "CpuLimitExceeded",
    "QpsLimitExceeded",
)

__all__ = [
    "AioMegfileRetryConditions",
    "DEFAULT_RETRY_CAPACITY",
    "S3BucketNotFoundError",
    "S3ConfigError",
    "S3Exception",
    "S3FileChangedError",
    "S3FileExistsError",
    "S3FileNotFoundError",
    "S3InvalidRangeError",
    "S3IsADirectoryError",
    "S3NameTooLongError",
    "S3NotADirectoryError",
    "S3NotALinkError",
    "S3PermissionError",
    "S3_RETRY_ERROR_CODES",
    "S3_RETRY_EXCEPTIONS",
    "S3UnknownError",
    "SameFileError",
    "client_error_code",
    "client_error_message",
    "param_validation_error_report",
    "raise_s3_error",
    "register_retry_handler",
    "s3_should_retry",
    "translate_s3_error",
]


class S3Exception(Exception):
    """Base type for S3-specific errors."""


class S3FileNotFoundError(S3Exception, FileNotFoundError):
    """Raised when S3 object does not exist."""


class S3BucketNotFoundError(S3FileNotFoundError, PermissionError):
    """Raised when target S3 bucket does not exist."""


class S3FileExistsError(S3Exception, FileExistsError):
    """Raised when creating an object that already exists."""


class S3NotADirectoryError(S3Exception, NotADirectoryError):
    """Raised when expected directory-like prefix is not a directory."""


class S3IsADirectoryError(S3Exception, IsADirectoryError):
    """Raised when expected file-like key is a directory-like prefix."""


class S3FileChangedError(S3Exception):
    """Raised when object changes unexpectedly during read/write."""


class S3PermissionError(S3Exception, PermissionError):
    """Raised on permission denied operations."""


class S3ConfigError(S3Exception, EnvironmentError):
    """Raised when S3 client configuration or credentials are invalid."""


class S3NotALinkError(S3FileNotFoundError, PermissionError):
    """Raised when symlink operation target is not a link."""


class S3NameTooLongError(S3FileNotFoundError, PermissionError):
    """Raised when key name exceeds storage backend limits."""


class S3InvalidRangeError(S3Exception):
    """Raised when requested byte range is invalid."""


class S3UnknownError(S3Exception, UnknownError):
    """Raised when an S3 error cannot be mapped to a specific subtype."""

    def __init__(self, error: Exception, path: PathLike, extra: str | None = None):
        """Initialize ``S3UnknownError`` with original cause.

        :param error: Original exception.
        :param path: Related S3 path.
        :param extra: Optional extra context.
        """
        UnknownError.__init__(self, error, path, extra)


def client_error_code(error: ClientError) -> str:
    """Return normalized error code from ``ClientError``.

    :param error: Botocore client error.
    :return: Error code string.
    :rtype: str
    """
    error_data = error.response.get("Error", {})
    return error_data.get("Code") or error_data.get("code", "Unknown")


def client_error_message(error: ClientError) -> str:
    """Return message field from ``ClientError``.

    :param error: Botocore client error.
    :return: Message text.
    :rtype: str
    """
    return error.response.get("Error", {}).get("Message", "Unknown")


def param_validation_error_report(error: ParamValidationError) -> str:
    """Return report text from ``ParamValidationError``.

    :param error: Param validation error.
    :return: Report string.
    :rtype: str
    """
    return error.kwargs.get("report", "Unknown")


def translate_s3_error(s3_error: Exception, s3_url: PathLike) -> Exception:
    """Translate boto/aiobotocore exceptions to S3-friendly exceptions.

    :param s3_error: Exception raised by S3 client.
    :param s3_url: S3 path associated with the failed operation.
    :return: Translated exception.
    :rtype: Exception
    """
    if isinstance(s3_error, S3Exception):
        return s3_error
    if isinstance(s3_error, ClientError):
        code = client_error_code(s3_error)
        if code in ("NoSuchBucket",):
            response = getattr(s3_error, "response", {})
            error_data = response.get("Error", {}) if isinstance(response, dict) else {}
            bucket_or_url = (
                error_data.get("BucketName") if isinstance(error_data, dict) else None
            ) or s3_url
            return S3BucketNotFoundError(f"No such bucket: {bucket_or_url!r}")
        if code in ("404", "NoSuchKey"):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        if code in ("401", "403", "AccessDenied"):
            message = client_error_message(s3_error)
            return S3PermissionError(
                f"Permission denied: {s3_url!r}, code: {code}, message: {message!r}"
            )
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            message = client_error_message(s3_error)
            return S3ConfigError(
                f"Invalid configuration: {s3_url!r}, code: {code}, message: {message!r}"
            )
        if code in ("InvalidRange", "Requested Range Not Satisfiable"):
            return S3InvalidRangeError(
                f"Invalid range: {s3_url!r}, code: {code}, "
                f"message: {client_error_message(s3_error)!r}"
            )
        return S3UnknownError(s3_error, s3_url)

    if isinstance(s3_error, ParamValidationError):
        report = param_validation_error_report(s3_error)
        if "Invalid bucket name" in report:
            return S3BucketNotFoundError("Invalid bucket name: %r" % s3_url)
        if "Invalid length for parameter Key" in report:
            return S3FileNotFoundError("Invalid length for parameter Key: %r" % s3_url)
        return S3UnknownError(s3_error, s3_url)

    if isinstance(s3_error, NoCredentialsError):
        return S3ConfigError(str(s3_error))
    return S3UnknownError(s3_error, s3_url)


@contextmanager
def raise_s3_error(s3_url: PathLike, suppress_error_callback=None):
    """Context manager that translates errors raised inside the block.

    :param s3_url: S3 path for error context.
    :param suppress_error_callback: Optional callback deciding whether to suppress.
    :yield: Context block execution.
    :raises Exception: Translated S3 exception when not suppressed.
    """
    try:
        yield
    except Exception as error:
        translated = translate_s3_error(error, s3_url)
        if suppress_error_callback and suppress_error_callback(translated):
            return
        raise translated from error


def s3_should_retry(exception: Exception) -> bool:
    """Return whether an S3 exception should trigger retry.

    :param exception: Exception raised by an S3 operation.
    :return: True if retry should be attempted.
    :rtype: bool
    """
    if isinstance(exception, S3_RETRY_EXCEPTIONS):
        logger.debug("Retryable exception encountered: %s", exception)
        return True
    if isinstance(exception, botocore.exceptions.ClientError):
        response = getattr(exception, "response", {})
        error_data = response.get("Error", {}) if isinstance(response, dict) else {}
        error_code = error_data.get("Code") or error_data.get("code", "Unknown")
        if error_code in S3_RETRY_ERROR_CODES:
            logger.debug("Retryable error code encountered: %s", error_code)
            return True
    return False


class AioMegfileRetryConditions(AioStandardRetryConditions):
    """Retry condition set used by aiobotocore clients in aiomegfile."""

    def __init__(
        self,
        max_attempts: int = S3_MAX_RETRY_TIMES,
    ):
        """Initialize retry conditions.

        :param max_attempts: Maximum retry attempts.
        """
        self._max_attempts_checker = MaxAttemptsChecker(max_attempts)

    async def is_retryable(self, context) -> bool:
        """Return whether current request context is retryable.

        :param context: aiobotocore retry context.
        :return: True when request should be retried.
        :rtype: bool
        """
        if not self._max_attempts_checker.is_retryable(context):
            return False

        if isinstance(context.caught_exception, S3_RETRY_EXCEPTIONS):
            logger.debug(
                "Retryable exception encountered: %s",
                context.caught_exception,
            )
            return True

        error_code = context.get_error_code()
        if error_code in S3_RETRY_ERROR_CODES:
            logger.debug("Retryable error code encountered: %s", error_code)
            return True
        return False


def register_retry_handler(client: "S3Client", max_attempts: int = S3_MAX_RETRY_TIMES):
    """Register aiobotocore retry handler for an S3-like client.

    :param client: aiobotocore client instance.
    :param max_attempts: Maximum retry attempts.
    :return: Registered retry handler.
    :rtype: AioRetryHandler
    """
    retry_quota = RetryQuotaChecker(
        quota.RetryQuota(
            initial_capacity=DEFAULT_RETRY_CAPACITY,
        )
    )

    service_id = client.meta.service_model.service_id
    service_event_name = service_id.hyphenize()
    client.meta.events.register(
        f"after-call.{service_event_name}", retry_quota.release_retry_quota
    )

    handler = AioRetryHandler(
        retry_policy=AioRetryPolicy(
            retry_checker=AioMegfileRetryConditions(max_attempts=max_attempts),
            retry_backoff=ExponentialBackoff(),
        ),
        retry_event_adapter=RetryEventAdapter(),
        retry_quota=retry_quota,  # pyre-ignore[6]
    )

    event_name = f"needs-retry.{service_event_name}"
    unique_id = f"retry-config-{service_event_name}"
    client.meta.events.unregister(event_name, unique_id=unique_id)
    client.meta.events.register(
        event_name,
        handler.needs_retry,  # pyre-ignore[6]
        unique_id=unique_id,
    )
    return handler
