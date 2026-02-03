"""Tests for S3 retry logic."""

from unittest.mock import MagicMock

import botocore.exceptions
import pytest
import urllib3.exceptions
from aiobotocore.retries.standard import AioRetryHandler

from aiomegfile.filesystem.s3 import S3FileSystem
from aiomegfile.lib.s3_retry import (
    AioMegfileRetryConditions,
    register_retry_handler,
    s3_should_retry,
)

_aws_access_key_id = "testing"
_aws_secret_access_key = "testing"


@pytest.fixture
def mock_s3_env(monkeypatch):
    """Mock AWS credentials in environment variables."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _aws_access_key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _aws_secret_access_key)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")


class TestS3ShouldRetry:
    """Test s3_should_retry function."""

    def test_retry_on_read_timeout_error(self):
        """Test that ReadTimeoutError triggers retry."""
        exception = botocore.exceptions.ReadTimeoutError(
            endpoint_url="http://example.com"
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_endpoint_connection_error(self):
        """Test that EndpointConnectionError triggers retry."""
        exception = botocore.exceptions.EndpointConnectionError(
            endpoint_url="http://example.com"
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_incomplete_read_error(self):
        """Test that IncompleteReadError from botocore triggers retry."""
        exception = botocore.exceptions.IncompleteReadError(
            actual_bytes=50, expected_bytes=100
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_urllib3_incomplete_read(self):
        """Test that IncompleteRead from urllib3 triggers retry."""
        exception = urllib3.exceptions.IncompleteRead(partial=b"", expected=100)
        assert s3_should_retry(exception) is True

    def test_retry_on_client_error_with_retryable_code(self):
        """Test that ClientError with retryable error code triggers retry."""
        # Mock a ClientError with a retryable error code
        exception = botocore.exceptions.ClientError(
            error_response={
                "Error": {"Code": "503", "Message": "Service Unavailable"},
                "ResponseMetadata": {"HTTPStatusCode": 503},
            },
            operation_name="HeadObject",
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_client_error_with_request_timeout(self):
        """Test that ClientError with RequestTimeout code triggers retry."""
        exception = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "RequestTimeout",
                    "Message": (
                        "Your socket connection to the server was not read "
                        "from or written to within the timeout period."
                    ),
                },
                "ResponseMetadata": {"HTTPStatusCode": 400},
            },
            operation_name="HeadObject",
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_client_error_with_internal_error(self):
        """Test that ClientError with InternalError code triggers retry."""
        exception = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "InternalError",
                    "Message": "We encountered an internal error. Please try again.",
                },
                "ResponseMetadata": {"HTTPStatusCode": 500},
            },
            operation_name="GetObject",
        )
        assert s3_should_retry(exception) is True

    def test_retry_on_client_error_with_qps_limit_exceeded(self):
        """Test that ClientError with QpsLimitExceeded code triggers retry."""
        exception = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "QpsLimitExceeded",
                    "Message": "Please reduce your request rate.",
                },
                "ResponseMetadata": {"HTTPStatusCode": 429},
            },
            operation_name="ListObjects",
        )
        assert s3_should_retry(exception) is True

    def test_no_retry_on_client_error_with_non_retryable_code(self):
        """Test ClientError with non-retryable code doesn't trigger retry."""
        exception = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchKey",
                    "Message": "The specified key does not exist.",
                },
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="GetObject",
        )
        assert s3_should_retry(exception) is False

    def test_no_retry_on_non_retryable_exception(self):
        """Test that non-retryable exceptions don't trigger retry."""
        exception = ValueError("Not a retryable error")
        assert s3_should_retry(exception) is False

    def test_no_retry_on_key_error(self):
        """Test that KeyError doesn't trigger retry."""
        exception = KeyError("missing_key")
        assert s3_should_retry(exception) is False


class TestAioMegfileRetryConditions:
    """Test AioMegfileRetryConditions class."""

    def _create_mock_context(
        self, attempt_number, caught_exception=None, error_code=None
    ):
        """Helper to create a properly mocked context."""
        context = MagicMock()
        context.attempt_number = attempt_number
        context.caught_exception = caught_exception
        context.get_error_code = MagicMock(return_value=error_code)
        # Add request_context to avoid TypeError in MaxAttemptsChecker
        context.request_context = {}
        return context

    async def test_retry_on_read_timeout_error(self):
        """Test that ReadTimeoutError triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1,
            caught_exception=botocore.exceptions.ReadTimeoutError(
                endpoint_url="http://example.com"
            ),
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_endpoint_connection_error(self):
        """Test that EndpointConnectionError triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1,
            caught_exception=botocore.exceptions.EndpointConnectionError(
                endpoint_url="http://example.com"
            ),
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_incomplete_read_error(self):
        """Test that IncompleteReadError from urllib3 triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1,
            caught_exception=urllib3.exceptions.IncompleteRead(
                partial=b"", expected=100
            ),
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_503_error_code(self):
        """Test that 503 error code triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(attempt_number=1, error_code="503")

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_internal_error_code(self):
        """Test that InternalError error code triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1, error_code="InternalError"
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_service_unavailable_error_code(self):
        """Test that ServiceUnavailable error code triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1, error_code="ServiceUnavailable"
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_retry_on_qps_limit_exceeded_error_code(self):
        """Test that QpsLimitExceeded error code triggers retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1, error_code="QpsLimitExceeded"
        )

        # Should be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is True

    async def test_no_retry_when_max_attempts_reached(self):
        """Test that retry is not attempted when max attempts is reached."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=3,  # Already at max attempts
            caught_exception=botocore.exceptions.ReadTimeoutError(
                endpoint_url="http://example.com"
            ),
        )

        # Should not be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is False

    async def test_no_retry_on_non_retryable_exception(self):
        """Test that non-retryable exceptions don't trigger retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(
            attempt_number=1, caught_exception=ValueError("Not a retryable error")
        )

        # Should not be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is False

    async def test_no_retry_on_non_retryable_error_code(self):
        """Test that non-retryable error codes don't trigger retry."""
        retry_conditions = AioMegfileRetryConditions(max_attempts=3)

        context = self._create_mock_context(attempt_number=1, error_code="NoSuchKey")

        # Should not be retryable
        is_retryable = await retry_conditions.is_retryable(context)
        assert is_retryable is False


class TestRegisterRetryHandler:
    """Test register_retry_handler function."""

    async def test_register_retry_handler_returns_handler(self):
        """Test that register_retry_handler returns an AioRetryHandler."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.meta.service_model.service_id.hyphenize.return_value = "s3"
        mock_client.meta.events.register = MagicMock()
        mock_client.meta.events.unregister = MagicMock()

        handler = register_retry_handler(mock_client, max_attempts=3)

        assert isinstance(handler, AioRetryHandler)
        assert mock_client.meta.events.register.called
        assert mock_client.meta.events.unregister.called

    async def test_register_retry_handler_uses_custom_max_attempts(self):
        """Test that register_retry_handler respects custom max_attempts."""
        # Create a mock client
        mock_client = MagicMock()
        mock_client.meta.service_model.service_id.hyphenize.return_value = "s3"
        mock_client.meta.events.register = MagicMock()
        mock_client.meta.events.unregister = MagicMock()

        max_attempts = 5
        handler = register_retry_handler(mock_client, max_attempts=max_attempts)

        # Verify that the retry checker has the correct max attempts
        assert isinstance(
            handler._retry_policy._retry_checker, AioMegfileRetryConditions
        )
        assert (
            handler._retry_policy._retry_checker._max_attempts_checker._max_attempts
            == max_attempts
        )


class TestS3FileSystemRetry:
    """Integration tests for S3FileSystem retry logic.

    Note: Actual retry behavior is tested at the unit level in
    TestAioMegfileRetryConditions since retry logic is handled by boto/aiobotocore.
    These tests verify that the retry handler infrastructure is properly set up.
    """

    async def test_retry_handler_is_registered(self, mock_s3_env):  # noqa: ARG002
        """Test that retry handler is registered when creating S3 client."""
        filesystem = S3FileSystem()
        client = await filesystem._get_client()

        # Verify that the client has the necessary retry infrastructure
        assert client.meta.events is not None, "Client events not initialized"
        assert client.meta.service_model is not None, "Service model not initialized"

        # Verify the service ID can be retrieved (used for event registration)
        service_id = client.meta.service_model.service_id.hyphenize()
        assert service_id == "s3", f"Expected service_id 's3', got '{service_id}'"

        # The actual registration is tested in TestRegisterRetryHandler
        # Here we just verify the client is properly configured


class TestS3RetryWithMotoServer:
    """Integration tests for S3 retry behavior with moto server.

    These tests verify that the retry mechanism works end-to-end by simulating
    transient errors at the HTTP layer.
    """

    @pytest.fixture(scope="class")
    def moto_server(self):
        """Start moto server for S3 mock."""
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer()
        try:
            server.start()
            host, port = server.get_host_and_port()
            if host == "0.0.0.0":
                host = "localhost"
            yield f"http://{host}:{port}"
        finally:
            server.stop()

    @pytest.fixture
    def mock_s3_with_server(self, moto_server, monkeypatch):
        """Configure environment to use moto server."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", _aws_access_key_id)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _aws_secret_access_key)
        monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)

    async def test_retry_on_request_timeout_succeeds(self, mock_s3_with_server):
        """Test that first RequestTimeout triggers retry and second request succeeds.

        This test simulates a scenario where the first HEAD request returns a
        RequestTimeout error, and verifies that the retry mechanism kicks in
        and the second request succeeds.
        """
        from unittest.mock import patch

        from aiobotocore.httpsession import AIOHTTPSession

        bucket_name = "retry-test-bucket"
        filesystem = S3FileSystem()
        client = await filesystem._get_client()

        # Setup: create bucket and test file
        await client.create_bucket(Bucket=bucket_name)
        await client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello")

        # Track HEAD request count
        head_request_count = {"value": 0}
        original_send = AIOHTTPSession.send

        async def mock_send(self, request):
            """Mock send that returns RequestTimeout on first HEAD request."""
            if request.method == "HEAD":
                head_request_count["value"] += 1
                if head_request_count["value"] == 1:
                    # First HEAD request: return RequestTimeout error response
                    error_body = (
                        b'<?xml version="1.0" encoding="UTF-8"?>'
                        b"<Error>"
                        b"<Code>RequestTimeout</Code>"
                        b"<Message>Your socket connection to the server was not "
                        b"read from or written to within the timeout period.</Message>"
                        b"<RequestId>test-request-id</RequestId>"
                        b"<HostId>test-host-id</HostId>"
                        b"</Error>"
                    )

                    # Create a mock response object compatible with aiobotocore
                    # aiobotocore expects `content` to be awaitable
                    class MockAWSResponse:
                        """Mock AWS response for error simulation."""

                        def __init__(self):
                            self.status_code = 400
                            self.headers = {
                                "Content-Type": "application/xml",
                                "x-amz-request-id": "test-request-id",
                            }
                            self.url = str(request.url)
                            self._content = error_body

                        @property
                        async def content(self):
                            """Return content as awaitable (aiobotocore requirement)."""
                            return self._content

                        @property
                        def text(self):
                            return self._content.decode("utf-8")

                    return MockAWSResponse()

            # All other requests: pass through to moto server
            return await original_send(self, request)

        with patch.object(AIOHTTPSession, "send", mock_send):
            # This should trigger retry and succeed on second attempt
            result = await filesystem.is_file(f"{bucket_name}/test.txt")

        assert result is True, "Expected is_file to return True after retry"
        assert head_request_count["value"] >= 2, (
            f"Expected at least 2 HEAD requests (1 failed + 1 retry success), "
            f"got {head_request_count['value']}"
        )
