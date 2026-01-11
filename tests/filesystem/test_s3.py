import aiobotocore.session
import pytest
from botocore.exceptions import ClientError
from moto.server import ThreadedMotoServer

from aiomegfile.filesystem.s3 import S3FileSystem


@pytest.fixture
def moto_server():
    server = ThreadedMotoServer()
    try:
        server.start()
        host, port = server.get_host_and_port()
        yield f"http://{host}:{port}"
    finally:
        server.stop()


_region_name = "us-east-1"
_aws_access_key_id = "testing"
_aws_secret_access_key = "testing"
_bucket_name = "test-bucket"


@pytest.fixture
async def s3_client(moto_server):
    session = aiobotocore.session.get_session()
    async with session.create_client(
        "s3",
        endpoint_url=moto_server,
        region_name=_region_name,
        aws_access_key_id=_aws_access_key_id,
        aws_secret_access_key=_aws_secret_access_key,
    ) as client:
        yield client


class TestS3FileSystem:
    def _create_filesystem(self, moto_server):
        return S3FileSystem(
            protocol_in_path=False,
            endpoint_url=moto_server,
            region_name=_region_name,
            aws_access_key_id=_aws_access_key_id,
            aws_secret_access_key=_aws_secret_access_key,
        )

    async def test_is_file(self, s3_client, moto_server):
        filesystem = self._create_filesystem(moto_server)

        filename = "0.txt"
        await s3_client.create_bucket(Bucket=_bucket_name)
        await s3_client.put_object(Bucket=_bucket_name, Key=filename, Body=b"0")
        assert await filesystem.is_file(f"s3://{_bucket_name}/{filename}") is True
        assert await filesystem.is_file(f"s3://{_bucket_name}") is False
        assert await filesystem.is_file(f"s3://{_bucket_name}/") is False
        assert await filesystem.is_file(f"s3://{_bucket_name}/null.txt") is False

    async def test_is_dir(self, s3_client, moto_server):
        filesystem = self._create_filesystem(moto_server)

        subdir = "subdir"
        filename = "0.txt"
        await s3_client.create_bucket(Bucket=_bucket_name)
        await s3_client.put_object(
            Bucket=_bucket_name, Key=f"{subdir}/{filename}", Body=b"0"
        )
        assert await filesystem.is_dir(f"s3://{_bucket_name}") is True
        assert await filesystem.is_dir(f"s3://{_bucket_name}/") is True
        assert await filesystem.is_dir(f"s3://{_bucket_name}/{subdir}") is True
        assert await filesystem.is_dir(f"s3://{_bucket_name}/{subdir}/") is True
        assert await filesystem.is_dir(f"s3://{_bucket_name}/null") is False
        assert await filesystem.is_dir(f"s3://{_bucket_name}/null/") is False
        assert (
            await filesystem.is_dir(f"s3://{_bucket_name}/{subdir}/{filename}") is False
        )

    async def test_exists(self, s3_client, moto_server):
        filesystem = self._create_filesystem(moto_server)

        subdir = "subdir"
        filename = "0.txt"
        await s3_client.create_bucket(Bucket=_bucket_name)
        await s3_client.put_object(
            Bucket=_bucket_name, Key=f"{subdir}/{filename}", Body=b"0"
        )
        assert await filesystem.exists(f"s3://{_bucket_name}") is True
        assert await filesystem.exists(f"s3://{_bucket_name}/") is True
        assert await filesystem.exists(f"s3://{_bucket_name}/{subdir}") is True
        assert await filesystem.exists(f"s3://{_bucket_name}/{subdir}/") is True
        assert await filesystem.exists(f"s3://{_bucket_name}/null") is False
        assert await filesystem.exists(f"s3://{_bucket_name}/null/") is False
        assert (
            await filesystem.exists(f"s3://{_bucket_name}/{subdir}/{filename}") is True
        )

    async def test_unlink(self, s3_client, moto_server):
        filesystem = self._create_filesystem(moto_server)

        filename = "0.txt"
        await s3_client.create_bucket(Bucket=_bucket_name)
        await s3_client.put_object(Bucket=_bucket_name, Key=filename, Body=b"0")

        assert await filesystem.exists(f"s3://{_bucket_name}/{filename}") is True
        await filesystem.unlink(f"s3://{_bucket_name}/{filename}")
        assert await filesystem.exists(f"s3://{_bucket_name}/{filename}") is False
        assert await filesystem.is_file(f"s3://{_bucket_name}") is False

        with pytest.raises(ClientError):
            # 404 Not Found -> ClientError
            await s3_client.head_object(Bucket=_bucket_name, Key=filename)

    async def test_rmdir(self, s3_client, moto_server):
        filesystem = self._create_filesystem(moto_server)

        subdir = "subdir"
        filename1 = "1.txt"
        filename2 = "2.txt"
        await s3_client.create_bucket(Bucket=_bucket_name)
        await s3_client.put_object(
            Bucket=_bucket_name, Key=f"{subdir}/{filename1}", Body=b"0"
        )
        await s3_client.put_object(
            Bucket=_bucket_name, Key=f"{subdir}/{filename2}", Body=b"0"
        )

        await filesystem.rmdir(f"s3://{_bucket_name}/{subdir}")
        assert (
            await filesystem.exists(f"s3://{_bucket_name}/{subdir}/{filename1}")
            is False
        )
        assert await filesystem.exists(f"s3://{_bucket_name}/{subdir}") is False
        assert await filesystem.exists(f"s3://{_bucket_name}") is True
