import asyncio
import typing as T

import pytest
from moto.server import ThreadedMotoServer

from aiomegfile.config import (
    DEFAULT_WRITER_BLOCK_AUTOSCALE,
    WRITER_BLOCK_SIZE,
    WRITER_MAX_BUFFER_SIZE,
)
from aiomegfile.filesystem.s3 import get_s3_client
from aiomegfile.interfaces import AsyncIOManager
from aiomegfile.lib.s3_buffered_writer import (
    AsyncS3BufferedWriter,
)

_aws_access_key_id = "testing"
_aws_secret_access_key = "testing"
_bucket_name = "test-bucket"


@pytest.fixture(scope="module")
def moto_server():
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
def mock_s3(moto_server, monkeypatch):
    """Mock AWS credentials and endpoint URL to environment variables."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _aws_access_key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _aws_secret_access_key)
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)


def async_s3_buffered_open(
    bucket: str,
    key: str,
    *,
    s3_client,
    mode: str = "wb",
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    newline: T.Optional[str] = None,
    block_size: int = WRITER_BLOCK_SIZE,
    block_autoscale: bool = DEFAULT_WRITER_BLOCK_AUTOSCALE,
    max_buffer_size: int = WRITER_MAX_BUFFER_SIZE,
):
    """Async context manager for S3 buffered writing.

    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :param s3_client: Async S3 client from aiobotocore.
    :param mode: File mode, either 'w' (text) or 'wb' (binary).
    :param encoding: Text encoding for 'w' mode, defaults to 'utf-8'.
    :param errors: Error handling for encoding, defaults to 'strict'.
    :param newline: Newline handling for text mode.
    :param block_size: Size of each upload part in bytes.
    :param block_autoscale: Whether to auto-scale block size based on part number.
    :param max_buffer_size: Maximum total buffer size for pending uploads.
    :yields: AsyncS3BufferedWriter instance.
    """
    writer = AsyncS3BufferedWriter(
        bucket,
        key,
        s3_client=s3_client,
        mode=mode,
        encoding=encoding,
        errors=errors,
        newline=newline,
        block_size=block_size,
        block_autoscale=block_autoscale,
        max_buffer_size=max_buffer_size,
    )
    return AsyncIOManager(writer)


class TestAsyncS3BufferedWriter:
    """Test AsyncS3BufferedWriter class."""

    @pytest.fixture
    async def s3_client(self, mock_s3):  # noqa: ARG002
        """Create async S3 client."""
        return await get_s3_client()

    @pytest.fixture
    async def create_bucket(self, s3_client):
        """Create test bucket."""
        try:
            await s3_client.create_bucket(Bucket=_bucket_name)
        except Exception:
            pass  # Bucket may already exist

    async def _get_object_content(self, s3_client, key: str) -> bytes:
        """Helper to get object content."""
        resp = await s3_client.get_object(Bucket=_bucket_name, Key=key)
        return await resp["Body"].read()

    async def test_small_file_write(self, s3_client, create_bucket):  # noqa: ARG002
        """Test writing small file (no multipart upload)."""
        content = b"hello world"
        key = "small_file.txt"

        writer = AsyncS3BufferedWriter(
            _bucket_name,
            key,
            s3_client=s3_client,
        )
        await writer.write(content)
        await writer.close()

        # Verify content
        result = await self._get_object_content(s3_client, key)
        assert result == content

    async def test_write_with_context_manager(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test writing using async context manager."""
        content = b"context manager content"
        key = "context_manager.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            await writer.write(content)

        # Verify content
        result = await self._get_object_content(s3_client, key)
        assert result == content

    async def test_multiple_writes(self, s3_client, create_bucket):  # noqa: ARG002
        """Test multiple sequential writes."""
        key = "multiple_writes.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            await writer.write(b"hello ")
            await writer.write(b"world")

        # Verify content
        result = await self._get_object_content(s3_client, key)
        assert result == b"hello world"

    async def test_multipart_upload(self, s3_client, create_bucket):  # noqa: ARG002
        """Test multipart upload with large content."""
        # Use small block size to trigger multipart upload
        block_size = 5 * 1024 * 1024  # 5MB minimum for multipart
        content_size = block_size * 2 + 1024  # Force 3 parts
        content = b"x" * content_size
        key = "multipart.txt"

        async with async_s3_buffered_open(
            _bucket_name,
            key,
            s3_client=s3_client,
            block_size=block_size,
            block_autoscale=False,
        ) as writer:
            await writer.write(content)

        # Verify content
        result = await self._get_object_content(s3_client, key)
        assert len(result) == content_size
        assert result == content

    async def test_tell_position(self, s3_client, create_bucket):  # noqa: ARG002
        """Test tell() returns correct position."""
        key = "tell_test.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            assert await writer.tell() == 0
            await writer.write(b"hello")
            assert await writer.tell() == 5
            await writer.write(b" world")
            assert await writer.tell() == 11

    async def test_name_property(self, s3_client, create_bucket):  # noqa: ARG002
        """Test name property returns correct S3 URI."""
        key = "name_test.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            assert writer.name == f"{_bucket_name}/{key}"

    async def test_mode_property(self, s3_client, create_bucket):  # noqa: ARG002
        """Test mode property returns 'wb'."""
        key = "mode_test.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            assert writer.mode == "wb"

    async def test_writable(self, s3_client, create_bucket):  # noqa: ARG002
        """Test writable() returns True."""
        key = "writable_test.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            assert await writer.writable() is True

    async def test_closed_property(self, s3_client, create_bucket):  # noqa: ARG002
        """Test closed property."""
        key = "closed_test.txt"

        writer = AsyncS3BufferedWriter(_bucket_name, key, s3_client=s3_client)
        assert writer.closed is False
        await writer.write(b"test")
        await writer.close()
        assert writer.closed is True

    async def test_write_after_close_raises_error(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test writing after close raises IOError."""
        key = "write_after_close.txt"

        writer = AsyncS3BufferedWriter(_bucket_name, key, s3_client=s3_client)
        await writer.write(b"test")
        await writer.close()

        with pytest.raises(IOError):
            await writer.write(b"more data")

    async def test_double_close_safe(self, s3_client, create_bucket):  # noqa: ARG002
        """Test closing twice is safe."""
        key = "double_close.txt"

        writer = AsyncS3BufferedWriter(_bucket_name, key, s3_client=s3_client)
        await writer.write(b"test")
        await writer.close()
        await writer.close()  # Should not raise

    async def test_writelines(self, s3_client, create_bucket):  # noqa: ARG002
        """Test writelines method."""
        key = "writelines.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            await writer.writelines([b"line1\n", b"line2\n", b"line3\n"])

        result = await self._get_object_content(s3_client, key)
        assert result == b"line1\nline2\nline3\n"

    async def test_empty_file(self, s3_client, create_bucket):  # noqa: ARG002
        """Test writing empty file."""
        key = "empty.txt"

        async with async_s3_buffered_open(_bucket_name, key, s3_client=s3_client):
            pass  # Write nothing

        result = await self._get_object_content(s3_client, key)
        assert result == b""

    async def test_max_buffer_size_control(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test that max_buffer_size limits pending uploads."""
        # Use small sizes to trigger buffer control
        block_size = 5 * 1024 * 1024  # 5MB
        max_buffer_size = 10 * 1024 * 1024  # 10MB (allows 2 pending parts)
        content_size = block_size * 4  # 20MB (4 parts)
        content = b"x" * content_size
        key = "buffer_control.txt"

        async with async_s3_buffered_open(
            _bucket_name,
            key,
            s3_client=s3_client,
            block_size=block_size,
            max_buffer_size=max_buffer_size,
            block_autoscale=False,
        ) as writer:
            await writer.write(content)

        result = await self._get_object_content(s3_client, key)
        assert len(result) == content_size

    async def test_block_autoscale(self, s3_client, create_bucket):  # noqa: ARG002
        """Test block autoscale feature."""
        key = "autoscale.txt"
        base_block_size = 8 * 1024 * 1024  # 8MB

        writer = AsyncS3BufferedWriter(
            _bucket_name,
            key,
            s3_client=s3_client,
            block_size=base_block_size,
            block_autoscale=True,
        )

        # Test autoscale logic
        writer._part_number = 0
        assert writer._block_size == base_block_size

        writer._part_number = 10
        assert writer._block_size == base_block_size * 2

        writer._part_number = 100
        assert writer._block_size == base_block_size * 4

        writer._part_number = 1000
        assert writer._block_size == base_block_size * 8

        await writer.close()

    async def test_concurrent_writes(self, s3_client, create_bucket):  # noqa: ARG002
        """Test concurrent writes to different files."""
        content1 = b"content1"
        content2 = b"content2"
        key1 = "concurrent1.txt"
        key2 = "concurrent2.txt"

        async def write_file(key, content):
            async with async_s3_buffered_open(
                _bucket_name, key, s3_client=s3_client
            ) as writer:
                await writer.write(content)

        await asyncio.gather(
            write_file(key1, content1),
            write_file(key2, content2),
        )

        result1 = await self._get_object_content(s3_client, key1)
        result2 = await self._get_object_content(s3_client, key2)
        assert result1 == content1
        assert result2 == content2

    async def test_repr(self, s3_client, create_bucket):  # noqa: ARG002
        """Test __repr__ method."""
        key = "repr_test.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client
        ) as writer:
            repr_str = repr(writer)
            assert "AsyncS3BufferedWriter" in repr_str
            assert f"{_bucket_name}/{key}" in repr_str
            assert "wb" in repr_str

    # Text mode tests

    async def test_text_mode_write(self, s3_client, create_bucket):  # noqa: ARG002
        """Test writing in text mode."""
        content = "hello world"
        key = "text_mode.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w"
        ) as writer:
            await writer.write(content)

        result = await self._get_object_content(s3_client, key)
        assert result == content.encode()

    async def test_text_mode_property(self, s3_client, create_bucket):  # noqa: ARG002
        """Test mode property returns 'w' for text mode."""
        key = "text_mode_prop.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w"
        ) as writer:
            assert writer.mode == "w"

    async def test_text_mode_with_encoding(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test writing with custom encoding."""
        content = "你好世界"
        key = "text_encoding.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w", encoding="utf-8"
        ) as writer:
            await writer.write(content)

        result = await self._get_object_content(s3_client, key)
        assert result == content.encode()

    async def test_text_mode_writelines(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test writelines in text mode."""
        key = "text_writelines.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w"
        ) as writer:
            await writer.writelines(["line1\n", "line2\n", "line3\n"])

        result = await self._get_object_content(s3_client, key)
        assert result == b"line1\nline2\nline3\n"

    async def test_text_mode_newline_crlf(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test newline translation to CRLF."""
        content = "line1\nline2\nline3"
        key = "text_newline_crlf.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w", newline="\r\n"
        ) as writer:
            await writer.write(content)

        result = await self._get_object_content(s3_client, key)
        assert result == b"line1\r\nline2\r\nline3"

    async def test_text_mode_newline_cr(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test newline translation to CR."""
        content = "line1\nline2"
        key = "text_newline_cr.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w", newline="\r"
        ) as writer:
            await writer.write(content)

        result = await self._get_object_content(s3_client, key)
        assert result == b"line1\rline2"

    async def test_text_mode_type_error(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test that writing bytes to text mode raises TypeError."""
        key = "text_type_error.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w"
        ) as writer:
            with pytest.raises(TypeError, match="must be str"):
                await writer.write(b"bytes data")

    async def test_binary_mode_type_error(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test that writing str to binary mode raises TypeError."""
        key = "binary_type_error.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="wb"
        ) as writer:
            with pytest.raises(TypeError, match="bytes-like object is required"):
                await writer.write("string data")

    async def test_invalid_mode_raises_error(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test that invalid mode raises ValueError."""
        key = "invalid_mode.txt"

        with pytest.raises(ValueError, match="Invalid mode"):
            AsyncS3BufferedWriter(_bucket_name, key, s3_client=s3_client, mode="r")

    async def test_text_mode_tell_position(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test tell() returns byte position in text mode."""
        key = "text_tell.txt"

        async with async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="w"
        ) as writer:
            assert await writer.tell() == 0
            await writer.write("hello")
            assert await writer.tell() == 5  # 5 length in UTF-8
            await writer.write("你好")  # 2 length in UTF-8
            assert await writer.tell() == 7

    async def test_open_without_with(
        self,
        s3_client,
        create_bucket,  # noqa: ARG002
    ):
        """Test that writing str to binary mode raises TypeError."""
        key = "binary_type_error.txt"

        writer = await async_s3_buffered_open(
            _bucket_name, key, s3_client=s3_client, mode="wb"
        )
        await writer.write(b"data without with")
        await writer.close()

        result = await self._get_object_content(s3_client, key)
        assert result == b"data without with"
