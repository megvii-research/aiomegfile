import pytest
from moto.server import ThreadedMotoServer

from aiomegfile.filesystem.s3 import S3FileSystem

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


class TestS3FileSystem:
    @pytest.fixture
    def filesystem(self, mock_s3):  # noqa: ARG002
        """Create S3FileSystem that reads credentials from environment."""
        return S3FileSystem()

    async def _create_bucket(self, filesystem: S3FileSystem):
        """Helper to create test bucket."""
        client = await filesystem._get_client()
        try:
            await client.create_bucket(Bucket=_bucket_name)
        except Exception:
            pass  # Bucket may already exist

    async def _put_object(self, filesystem: S3FileSystem, key: str, body: bytes = b"0"):
        """Helper to put object in test bucket."""
        client = await filesystem._get_client()
        await client.put_object(Bucket=_bucket_name, Key=key, Body=body)

    async def _head_object(self, filesystem: S3FileSystem, key: str):
        """Helper to head object in test bucket."""
        client = await filesystem._get_client()
        return await client.head_object(Bucket=_bucket_name, Key=key)

    async def test_is_file(self, filesystem):
        await self._create_bucket(filesystem)

        filename = "0.txt"
        await self._put_object(filesystem, filename)
        assert await filesystem.is_file(f"{_bucket_name}/{filename}") is True
        assert await filesystem.is_file(f"{_bucket_name}") is False
        assert await filesystem.is_file(f"{_bucket_name}/") is False
        assert await filesystem.is_file(f"{_bucket_name}/null.txt") is False

    async def test_is_dir(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "subdir"
        filename = "0.txt"
        await self._put_object(filesystem, f"{subdir}/{filename}")
        assert await filesystem.is_dir(f"{_bucket_name}") is True
        assert await filesystem.is_dir(f"{_bucket_name}/") is True
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}") is True
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}/") is True
        assert await filesystem.is_dir(f"{_bucket_name}/null") is False
        assert await filesystem.is_dir(f"{_bucket_name}/null/") is False
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}/{filename}") is False

    async def test_exists(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "subdir"
        filename = "0.txt"
        await self._put_object(filesystem, f"{subdir}/{filename}")
        assert await filesystem.exists(f"{_bucket_name}") is True
        assert await filesystem.exists(f"{_bucket_name}/") is True
        assert await filesystem.exists(f"{_bucket_name}/{subdir}") is True
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/") is True
        assert await filesystem.exists(f"{_bucket_name}/null") is False
        assert await filesystem.exists(f"{_bucket_name}/null/") is False
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/{filename}") is True

    async def test_remove_file(self, filesystem):
        await self._create_bucket(filesystem)

        filename = "remove_file.txt"
        await self._put_object(filesystem, filename)

        assert await filesystem.exists(f"{_bucket_name}/{filename}") is True
        await filesystem.remove(f"{_bucket_name}/{filename}")
        assert await filesystem.exists(f"{_bucket_name}/{filename}") is False

    async def test_remove_directory(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "remove_subdir"
        filename1 = "1.txt"
        filename2 = "2.txt"
        await self._put_object(filesystem, f"{subdir}/{filename1}")
        await self._put_object(filesystem, f"{subdir}/{filename2}")

        await filesystem.remove(f"{_bucket_name}/{subdir}")
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/{filename1}") is False
        assert await filesystem.exists(f"{_bucket_name}/{subdir}") is False
        assert await filesystem.exists(f"{_bucket_name}") is True
