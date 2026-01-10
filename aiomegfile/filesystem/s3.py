import asyncio
import re
from typing import TYPE_CHECKING, TypedDict

import aiobotocore.session

from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3ConfigError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3PermissionError,
    S3UnknownError,
    translate_s3_error,
)
from aiomegfile.interfaces import BaseFileSystem, StatResult
from aiomegfile.lib.compact import fspath
from aiomegfile.lib.url import split_uri
from aiomegfile.pathlike import PathLike

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


def is_s3(path: PathLike) -> bool:
    """
    1. According to
       `aws-cli <https://docs.aws.amazon.com/cli/latest/reference/s3/index.html>`_ ,
       test if a path is s3 path.
    2. megfile also support the path like `s3[+profile_name]://bucket/key`

    :param path: Path to be tested
    :returns: True if path is s3 path, else False
    """
    path = fspath(path)
    if re.match(r"^s3(\+\w+)?:\/\/", path):
        return True
    return False


def parse_s3_url(s3_url: PathLike) -> tuple[str, str]:
    s3_url = fspath(s3_url)
    if not is_s3(s3_url):
        raise ValueError("Not a s3 url: %r" % s3_url)
    right_part = s3_url.split("://", maxsplit=1)[1]
    bucket_pattern = re.match("(.*?)/", right_part)
    if bucket_pattern is None:
        bucket = right_part
        path = ""
    else:
        bucket = bucket_pattern.group(1)
        path = right_part[len(bucket) + 1 :]
    return bucket, path


def _become_prefix(prefix: str) -> str:
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"
    return prefix


class S3Config(TypedDict):
    endpoint_url: str | None
    region_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None


class S3FileSystem(BaseFileSystem):
    """
    Protocol for s3 operations.
    """

    protocol = "s3"

    def __init__(
        self,
        protocol_in_path: bool,
        *,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        """Create a S3FileSystem instance.

        :param protocol_in_path: Whether incoming paths include the ``s3://`` prefix.
        """
        self.protocol_in_path = protocol_in_path

        self._client: "S3Client | None" = None
        self._s3_config: S3Config = {
            "endpoint_url": endpoint_url,
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }

    async def _get_client(self) -> "S3Client":
        if self._client is not None:
            return self._client
        session = aiobotocore.session.get_session()
        context = session.create_client("s3", **self._s3_config)
        self._client = await context.__aenter__()
        return self._client

    async def _close_client(self):
        if self._client is None:
            return
        await self._client.__aexit__(None, None, None)
        self._client = None

    def __del__(self):
        if self._client is not None:
            try:
                asyncio.get_running_loop().create_task(self._close_client())
            except RuntimeError:
                pass

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a regular file, otherwise False.
        """
        client = await self._get_client()
        bucket, key = parse_s3_url(path)
        if not bucket or not key or key.endswith("/"):
            # s3://, s3:///key, s3://bucket, s3://bucket/prefix/
            return False

        # TODO
        # if followlinks:
        #     try:
        #         s3_url = self.readlink().path_with_protocol
        #         bucket, key = parse_s3_url(s3_url)
        #     except S3NotALinkError:
        #         pass

        try:
            await client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            error = translate_s3_error(error, path)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False
        return True

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a directory, otherwise False.
        """
        client = await self._get_client()
        bucket, key = parse_s3_url(path)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key
        prefix = _become_prefix(key)
        try:
            resp = await client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
            )
        except Exception as error:
            error = translate_s3_error(error, path)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False

        if not key:  # bucket is accessible
            return True

        if "KeyCount" in resp:
            return resp["KeyCount"] > 0

        return (
            len(resp.get("Contents", [])) > 0 or len(resp.get("CommonPrefixes", [])) > 0
        )

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path exists, otherwise False.
        """
        bucket, key = parse_s3_url(path)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key

        return await self.is_file(path, followlinks) or await self.is_dir(path)

    async def stat(self, path: str, followlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param path: Path to stat.
        :param followlinks: Whether to follow symbolic links.
        :raises FileNotFoundError: If the path does not exist.
        :return: Populated StatResult for the path.
        """
        client = await self._get_client()
        islnk = False
        bucket, key = parse_s3_url(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {path!r}")

        if not await self.is_file(path):
            return await self._get_dir_stat(path)

        client = await self._get_client()
        content = await client.head_object(Bucket=bucket, Key=key)
        if "Metadata" in content:
            metadata = dict(
                (key.lower(), value) for key, value in content["Metadata"].items()
            )
            if metadata and "symlink_to" in metadata:
                islnk = True
                if islnk and followlinks:
                    s3_url = metadata["symlink_to"]
                    bucket, key = parse_s3_url(s3_url)
                    content = await client.head_object(Bucket=bucket, Key=key)
        stat_record = StatResult(
            st_size=content["ContentLength"],
            st_mtime=content["LastModified"].timestamp(),
            islnk=islnk,
            extra=content,
        )
        return stat_record

    async def _get_dir_stat(self, path: str) -> StatResult:
        """
        Return StatResult of given s3_url directory, including:

        1. Directory size: the sum of all file size in it,
           including file in subdirectories (if exist).
           The result excludes the size of directory itself.
           In other words, return 0 Byte on an empty directory path
        2. Last-modified time of directory: return the latest modified time
           of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

        :returns: An int indicates size in Bytes
        """
        bucket, key = parse_s3_url(path)
        prefix = _become_prefix(key)
        count, size, mtime = 0, 0, 0.0

        async for resp in self._list_objects_recursive(bucket, prefix):
            for content in resp.get("Contents", []):
                count += 1
                size += content["Size"]
                last_modified = content["LastModified"].timestamp()
                if mtime < last_modified:
                    mtime = last_modified

        if count == 0:
            raise S3FileNotFoundError(f"No such file or directory: {path!r}")

        return StatResult(
            st_size=size,
            st_mtime=mtime,
            isdir=True,
        )

    async def _list_objects_recursive(
        self,
        bucket: str,
        prefix: str,
        delimiter: str = "",
    ):
        """List objects recursively."""
        client = await self._get_client()
        resp = await client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=1000
        )

        while True:
            yield resp

            if not resp["IsTruncated"]:
                break

            resp = await client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                Delimiter=delimiter,
                ContinuationToken=resp["NextContinuationToken"],
                MaxKeys=1000,
            )

    async def unlink(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param path: Path to remove.
        :param missing_ok: If False, raise when the file does not exist.
        :raises FileNotFoundError: When missing_ok is False and the file is absent.
        """
        bucket, key = parse_s3_url(path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {path!r}")
        if not await self.is_file(path):
            if missing_ok:
                return
            raise S3FileNotFoundError(f"No such file: {path!r}")

        client = await self._get_client()
        await client.delete_object(Bucket=bucket, Key=key)

    async def rmdir(self, path: str, missing_ok: bool = False) -> None:
        """
        Remove (delete) the directory and all its contents.

        :param path: The directory path to remove.
        :param missing_ok: If False, raise when the directory does not exist.
        :raises FileNotFoundError: When missing_ok is False and the directory is absent.
        """
        bucket, key = parse_s3_url(path)
        if not bucket or not key:
            # TODO: "bucket != '' and key = ''" should raise S3IsABucketError
            raise S3IsADirectoryError(f"Is a directory: {path!r}")
        if not await self.is_dir(path):
            if missing_ok:
                return
            raise S3FileNotFoundError(f"No such file: {path!r}")

        client = await self._get_client()
        prefix = _become_prefix(key)

        async for resp in self._list_objects_recursive(bucket, prefix):
            contents = resp.get("Contents", [])
            if not contents:
                continue

            objects_to_delete = [{"Key": obj["Key"]} for obj in contents]
            await client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete, "Quiet": True},
            )

    async def hasbucket(self, path: str) -> bool:
        """
        Test if the bucket of s3_url exists

        :returns: True if bucket of s3_url exists, else False
        """
        bucket, _ = parse_s3_url(path)
        if not bucket:
            return False

        client = await self._get_client()
        try:
            await client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        except Exception as error:
            error = translate_s3_error(error, path)
            if isinstance(error, S3PermissionError):
                # Aliyun OSS doesn't give bucket api permission when you only have read
                # and write permission
                try:
                    await client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                    return True
                except Exception as error2:
                    error2 = translate_s3_error(error2, path)
                    if isinstance(
                        error2, (S3UnknownError, S3ConfigError, S3PermissionError)
                    ):
                        raise error2
                    return False
            elif isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            elif isinstance(error, S3FileNotFoundError):
                return False

        return True

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param path: Directory path to create.
        :param mode: Permission bits for the new directory.
        :param parents: Whether to create missing parents.
        :param exist_ok: Whether to ignore if the directory exists.
        :raises FileExistsError: When directory exists and exist_ok is False.
        """
        bucket, _ = parse_s3_url(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {path!r}")
        try:
            if not await self.hasbucket(path):
                raise S3BucketNotFoundError(f"No such bucket: {path!r}")
        except S3PermissionError:
            pass
        if exist_ok:
            return
        if await self.exists(path):
            raise S3FileExistsError(f"File exists: {path!r}")

    def same_endpoint(self, other_filesystem: "BaseFileSystem") -> bool:
        """
        Return whether this filesystem points to the same endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True if both represent the same endpoint.
        """
        return isinstance(other_filesystem, S3FileSystem)

    def parse_uri(self, uri: str) -> str:
        """
        Parse the path part from a URI.

        :param uri: URI string.
        :return: Path part string.
        """
        _, path, _ = split_uri(uri)
        return path

    def build_uri(self, path: str) -> str:
        """
        Build URI for the filesystem by path part.

        :param path: Path without protocol.
        :return: Generated URI string.
        """
        return f"{self.protocol}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "S3FileSystem":
        """Return new instance of this class

        :param uri: URI string.
        :return: new instance of new path
        """
        return cls(protocol_in_path=f"{cls.protocol}://" in uri)
