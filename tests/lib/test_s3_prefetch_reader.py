import typing as T

import pytest
from moto.server import ThreadedMotoServer

from aiomegfile.filesystem.s3 import S3FileSystem, get_s3_client
from aiomegfile.lib.s3_prefetch_reader import AioS3PrefetchReader

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


def aio_s3_prefetch_open(
    bucket: str,
    key: str,
    *,
    filesystem,
    mode: str = "rb",
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    newline: T.Optional[str] = None,
    block_size: int = 8 * 1024 * 1024,
    max_buffer_size: int = 32 * 1024 * 1024,
    block_forward: T.Optional[int] = None,
):
    """Async context manager for S3 prefetch reading.

    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :param filesystem: S3FileSystem instance.
    :param mode: File mode, either 'r' (text) or 'rb' (binary).
    :param encoding: Text encoding for 'r' mode, defaults to 'utf-8'.
    :param errors: Error handling for encoding, defaults to 'strict'.
    :param newline: Newline handling for text mode.
    :param block_size: Size of each prefetch block in bytes.
    :param max_buffer_size: Maximum total buffer size for prefetch.
    :param block_forward: Number of blocks to prefetch ahead.
    :yields: AioS3PrefetchReader instance.
    """
    return AioS3PrefetchReader(
        bucket,
        key,
        filesystem=filesystem,
        mode=mode,
        encoding=encoding,
        errors=errors,
        newline=newline,
        block_size=block_size,
        max_buffer_size=max_buffer_size,
        block_forward=block_forward,
    )


class TestAsyncS3PrefetchReader:
    """Test AioS3PrefetchReader class."""

    @pytest.fixture
    async def s3_client(self, mock_s3):  # noqa: ARG002
        """Create async S3 client."""
        return await get_s3_client()

    @pytest.fixture
    async def filesystem(self, mock_s3):  # noqa: ARG002
        """Create S3FileSystem instance."""
        return S3FileSystem()

    @pytest.fixture
    async def create_bucket(self, s3_client):
        """Create test bucket."""
        try:
            await s3_client.create_bucket(Bucket=_bucket_name)
        except Exception:
            pass  # Bucket may already exist

    async def _put_object(self, s3_client, key: str, body: bytes):
        """Helper to put object in test bucket."""
        await s3_client.put_object(Bucket=_bucket_name, Key=key, Body=body)

    # Test read() method in binary mode

    async def test_read_all_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading entire file in binary mode."""
        content = b"hello world, this is a test file"
        key = "test_read_all_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_read_with_size_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading with size argument in binary mode."""
        content = b"hello world, this is a test file"
        key = "test_read_with_size_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.read(5)
            assert result == b"hello"
            result = await reader.read(6)
            assert result == b" world"

    async def test_read_chunked_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading file in chunks with small block_size in binary mode."""
        content = b"x" * 1000  # 1KB content
        key = "test_read_chunked_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=100,  # Small block size to force chunked reading
            max_buffer_size=300,
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_read_empty_file_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading empty file in binary mode."""
        content = b""
        key = "test_read_empty_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.read()
            assert result == b""

    async def test_read_zero_size_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading with size=0 in binary mode."""
        content = b"hello world"
        key = "test_read_zero_size_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.read(0)
            assert result == b""

    # Test read() method in text mode

    async def test_read_all_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading entire file in text mode."""
        content = "hello world, this is a test file"
        key = "test_read_all_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_read_with_size_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading with size argument in text mode."""
        content = "hello world, this is a test file"
        key = "test_read_with_size_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            result = await reader.read(5)
            assert result == "hello"

    async def test_read_chunked_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading file in chunks with small block_size in text mode."""
        content = "x" * 1000  # 1KB content
        key = "test_read_chunked_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="r",
            block_size=100,  # Small block size to force chunked reading
            max_buffer_size=300,
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_read_empty_file_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading empty file in text mode."""
        content = ""
        key = "test_read_empty_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            result = await reader.read()
            assert result == ""

    async def test_read_unicode_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading unicode content in text mode."""
        content = "你好世界 Hello World"
        key = "test_read_unicode_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            result = await reader.read()
            assert result == content

    # Test readline() method in binary mode

    async def test_readline_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline in binary mode."""
        content = b"line1\nline2\nline3"
        key = "test_readline_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            line1 = await reader.readline()
            assert line1 == b"line1\n"
            line2 = await reader.readline()
            assert line2 == b"line2\n"
            line3 = await reader.readline()
            assert line3 == b"line3"

    async def test_readline_with_size_binary(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test readline with size argument in binary mode."""
        content = b"hello world\nline2"
        key = "test_readline_with_size_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.readline(5)
            assert result == b"hello"

    async def test_readline_chunked_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline with small block_size in binary mode."""
        content = b"x" * 500 + b"\n" + b"y" * 500
        key = "test_readline_chunked_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=100,  # Small block size
            max_buffer_size=300,
        ) as reader:
            line1 = await reader.readline()
            assert line1 == b"x" * 500 + b"\n"
            line2 = await reader.readline()
            assert line2 == b"y" * 500

    async def test_readline_empty_file_binary(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test readline on empty file in binary mode."""
        content = b""
        key = "test_readline_empty_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            result = await reader.readline()
            assert result == b""

    # Test readline() method in text mode

    async def test_readline_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline in text mode."""
        content = "line1\nline2\nline3"
        key = "test_readline_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            line1 = await reader.readline()
            assert line1 == "line1\n"
            line2 = await reader.readline()
            assert line2 == "line2\n"
            line3 = await reader.readline()
            assert line3 == "line3"

    async def test_readline_with_size_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline with size argument in text mode."""
        content = "hello world\nline2"
        key = "test_readline_with_size_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            result = await reader.readline(5)
            assert result == "hello"

    async def test_readline_chunked_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline with small block_size in text mode."""
        content = "x" * 500 + "\n" + "y" * 500
        key = "test_readline_chunked_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="r",
            block_size=100,  # Small block size
            max_buffer_size=300,
        ) as reader:
            line1 = await reader.readline()
            assert line1 == "x" * 500 + "\n"
            line2 = await reader.readline()
            assert line2 == "y" * 500

    async def test_readline_unicode_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readline with unicode content in text mode."""
        content = "你好\n世界"
        key = "test_readline_unicode_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            line1 = await reader.readline()
            assert line1 == "你好\n"
            line2 = await reader.readline()
            assert line2 == "世界"

    async def test_readline_with_size_unicode_text(
        self,
        s3_client,
        filesystem,
        create_bucket,  # noqa: ARG002
    ):
        """Test readline with size in bytes for unicode text."""
        content = "你好\n世界"
        key = "test_readline_with_size_unicode_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            line1 = await reader.readline(2)
            assert line1 == "你好"
            line2 = await reader.readline()
            assert line2 == "\n"
            line3 = await reader.readline()
            assert line3 == "世界"

    # Test readinto() method (binary mode only)

    async def test_readinto_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readinto in binary mode."""
        content = b"hello world"
        key = "test_readinto_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            buffer = bytearray(11)
            n = await reader.readinto(buffer)
            assert n == 11
            assert bytes(buffer) == content

    async def test_readinto_partial_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readinto with partial read in binary mode."""
        content = b"hello world"
        key = "test_readinto_partial_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            buffer = bytearray(5)
            n = await reader.readinto(buffer)
            assert n == 5
            assert bytes(buffer) == b"hello"

    async def test_readinto_chunked_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readinto with small block_size in binary mode."""
        content = b"x" * 1000
        key = "test_readinto_chunked_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=100,  # Small block size
            max_buffer_size=300,
        ) as reader:
            buffer = bytearray(1000)
            n = await reader.readinto(buffer)
            assert n == 1000
            assert bytes(buffer) == content

    async def test_readinto_multiple_calls_binary(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test multiple readinto calls in binary mode."""
        content = b"hello world"
        key = "test_readinto_multiple_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            buffer1 = bytearray(5)
            n1 = await reader.readinto(buffer1)
            assert n1 == 5
            assert bytes(buffer1) == b"hello"

            buffer2 = bytearray(6)
            n2 = await reader.readinto(buffer2)
            assert n2 == 6
            assert bytes(buffer2) == b" world"

    async def test_readinto_eof_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test readinto at EOF in binary mode."""
        content = b"hello"
        key = "test_readinto_eof_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            buffer = bytearray(10)
            n = await reader.readinto(buffer)
            assert n == 5
            assert bytes(buffer[:5]) == content

            # Try to read again at EOF
            n = await reader.readinto(buffer)
            assert n == 0

    # Test max_buffer_size=0 (no prefetch)

    async def test_no_prefetch_read_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading with max_buffer_size=0 in binary mode."""
        content = b"hello world, this is a test"
        key = "test_no_prefetch_read_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            max_buffer_size=0,
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_no_prefetch_readline_binary(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test readline with max_buffer_size=0 in binary mode."""
        content = b"line1\nline2\nline3"
        key = "test_no_prefetch_readline_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            max_buffer_size=0,
        ) as reader:
            line1 = await reader.readline()
            assert line1 == b"line1\n"
            line2 = await reader.readline()
            assert line2 == b"line2\n"
            line3 = await reader.readline()
            assert line3 == b"line3"

    async def test_no_prefetch_readinto_binary(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test readinto with max_buffer_size=0 in binary mode."""
        content = b"hello world"
        key = "test_no_prefetch_readinto_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            max_buffer_size=0,
        ) as reader:
            buffer = bytearray(11)
            n = await reader.readinto(buffer)
            assert n == 11
            assert bytes(buffer) == content

    async def test_no_prefetch_read_text(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading with max_buffer_size=0 in text mode."""
        content = "hello world, this is a test"
        key = "test_no_prefetch_read_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="r",
            max_buffer_size=0,
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_no_prefetch_readline_text(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test readline with max_buffer_size=0 in text mode."""
        content = "line1\nline2\nline3"
        key = "test_no_prefetch_readline_text.txt"
        await self._put_object(s3_client, key, content.encode())

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="r",
            max_buffer_size=0,
        ) as reader:
            line1 = await reader.readline()
            assert line1 == "line1\n"
            line2 = await reader.readline()
            assert line2 == "line2\n"
            line3 = await reader.readline()
            assert line3 == "line3"

    # Test seek and tell

    async def test_seek_and_tell_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test seek and tell in binary mode."""
        content = b"hello world"
        key = "test_seek_tell_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            assert await reader.tell() == 0

            # Read 5 bytes
            await reader.read(5)
            assert await reader.tell() == 5

            # Seek to beginning
            await reader.seek(0)
            assert await reader.tell() == 0

            # Read again
            result = await reader.read(5)
            assert result == b"hello"

    async def test_seek_and_read_binary(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test seek to specific position and read in binary mode."""
        content = b"hello world"
        key = "test_seek_read_binary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            await reader.seek(6)
            result = await reader.read()
            assert result == b"world"

    # Test properties and methods

    async def test_name_property(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test name property returns correct value."""
        key = "test_name.txt"
        await self._put_object(s3_client, key, b"test")

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            assert reader.name == f"s3://{_bucket_name}/{key}"

    async def test_mode_property(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test mode property returns correct value."""
        key = "test_mode.txt"
        await self._put_object(s3_client, key, b"test")

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        ) as reader:
            assert reader.mode == "rb"

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            assert reader.mode == "r"

    async def test_closed_property(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test closed property."""
        key = "test_closed.txt"
        await self._put_object(s3_client, key, b"test")

        reader = AioS3PrefetchReader(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        )
        async with reader:
            assert reader.closed is False
        assert reader.closed is True

    async def test_read_after_close_raises_error(
        self,
        s3_client,
        filesystem,
        create_bucket,  # noqa: ARG002
    ):
        """Test reading after close raises IOError."""
        key = "test_read_after_close.txt"
        await self._put_object(s3_client, key, b"test")

        reader = AioS3PrefetchReader(
            _bucket_name, key, filesystem=filesystem, mode="rb"
        )
        async with reader:
            pass

        with pytest.raises(IOError, match="file already closed"):
            await reader.read()

    async def test_readinto_not_supported_in_text_mode(
        self,
        s3_client,
        filesystem,
        create_bucket,  # noqa: ARG002
    ):
        """Test readinto raises error in text mode."""
        key = "test_readinto_text.txt"
        await self._put_object(s3_client, key, b"test")

        async with aio_s3_prefetch_open(
            _bucket_name, key, filesystem=filesystem, mode="r"
        ) as reader:
            buffer = bytearray(10)
            with pytest.raises(IOError, match="readinto.*not supported.*text mode"):
                await reader.readinto(buffer)

    # Test with different block configurations

    async def test_single_block_read(self, s3_client, filesystem, create_bucket):  # noqa: ARG002
        """Test reading file that fits in single block."""
        content = b"small content"
        key = "test_single_block.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=1024,  # Larger than content
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_exact_block_boundary_read(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test reading file with size exactly at block boundary."""
        content = b"x" * 100
        key = "test_exact_boundary.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=100,  # Exactly same as content
        ) as reader:
            result = await reader.read()
            assert result == content

    async def test_multiple_blocks_sequential_read(
        self, s3_client, filesystem, create_bucket
    ):  # noqa: ARG002
        """Test sequential reading across multiple blocks."""
        content = b"a" * 100 + b"b" * 100 + b"c" * 100
        key = "test_multiple_blocks_sequential.txt"
        await self._put_object(s3_client, key, content)

        async with aio_s3_prefetch_open(
            _bucket_name,
            key,
            filesystem=filesystem,
            mode="rb",
            block_size=100,
            max_buffer_size=200,
        ) as reader:
            result1 = await reader.read(100)
            assert result1 == b"a" * 100
            result2 = await reader.read(100)
            assert result2 == b"b" * 100
            result3 = await reader.read(100)
            assert result3 == b"c" * 100
