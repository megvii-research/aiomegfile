import logging

import botocore
import requests
import urllib3
from aiobotocore.retries.standard import (
    AioRetryHandler,
    AioRetryPolicy,
    AioStandardRetryConditions,
)
from botocore.retries.standard import (
    ExponentialBackoff,
    MaxAttemptsChecker,
    RetryEventAdapter,
    RetryQuotaChecker,
    quota,
)

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES

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
    "429",  # noqa: E501 # TOS ExceedAccountQPSLimit
    "499",  # noqa: E501 # Some cloud providers may send response with http code 499 if the connection not send data in 1 min.
    "500",
    "501",
    "502",
    "503",
    "InternalError",
    "ServiceUnavailable",
    "SlowDown",
    "ContextCanceled",
    "Timeout",  # noqa: E501 # TOS Timeout
    "RequestTimeout",
    "RequestTimeTooSkewed",
    "ExceedAccountQPSLimit",
    "ExceedAccountRateLimit",
    "ExceedBucketQPSLimit",
    "ExceedBucketRateLimit",
    "DownloadTrafficRateLimitExceeded",  # noqa: E501 # OSS RateLimitExceeded
    "UploadTrafficRateLimitExceeded",
    "MetaOperationQpsLimitExceeded",
    "TotalQpsLimitExceeded",
    "PartitionQpsLimitted",
    "ActiveRequestLimitExceeded",
    "CpuLimitExceeded",
    "QpsLimitExceeded",
)


def s3_should_retry(exception: Exception):
    if isinstance(exception, S3_RETRY_EXCEPTIONS):
        logger.debug("Retryable exception encountered: %s", exception)
        return True
    if isinstance(exception, botocore.exceptions.ClientError):
        error_data = exception.response.get("Error", {})
        error_code = error_data.get("Code") or error_data.get("code", "Unknown")
        if error_code in S3_RETRY_ERROR_CODES:
            logger.debug("Retryable error code encountered: %s", error_code)
            return True
    return False


class AioMegfileRetryConditions(AioStandardRetryConditions):
    def __init__(self, max_attempts=DEFAULT_MAX_RETRY_TIMES):  # noqa: E501, lgtm [py/missing-call-to-init]
        # Note: This class is for convenience so you can have the
        # standard retry condition in a single class.
        self._max_attempts_checker = MaxAttemptsChecker(max_attempts)

    async def is_retryable(self, context):
        if not self._max_attempts_checker.is_retryable(context):
            return False

        if isinstance(context.caught_exception, S3_RETRY_EXCEPTIONS):
            logger.debug(
                "Retryable exception encountered: %s", context.caught_exception
            )
            return True
        error_code = context.get_error_code()
        if error_code in S3_RETRY_ERROR_CODES:
            logger.debug("Retryable error code encountered: %s", error_code)
            return True
        return False


def register_retry_handler(client, max_attempts=DEFAULT_MAX_RETRY_TIMES):
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
        retry_quota=retry_quota,
    )

    event_name = f"needs-retry.{service_event_name}"
    unique_id = f"retry-config-{service_event_name}"
    client.meta.events.unregister(event_name, unique_id=unique_id)
    client.meta.events.register(
        event_name,
        handler.needs_retry,
        unique_id=unique_id,
    )
    return handler
