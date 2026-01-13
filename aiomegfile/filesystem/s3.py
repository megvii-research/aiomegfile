import asyncio
import os
import re
import typing as T

import aiobotocore.session
import botocore

from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3ConfigError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3NameTooLongError,
    S3NotALinkError,
    S3PermissionError,
    S3UnknownError,
    translate_s3_error,
)
from aiomegfile.interfaces import BaseFileSystem, FileEntry, StatResult
from aiomegfile.utils.path import PathLike, fspath, split_uri

if T.TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


DEFAULT_ENDPOINT_URL = "https://s3.amazonaws.com"


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


def get_endpoint_url(
    profile_name: T.Optional[str] = None, config: T.Optional[dict] = None
) -> str:
    """Get the endpoint url of S3

    :returns: S3 endpoint url
    """
    profile_name = profile_name or os.environ.get("AWS_PROFILE")
    environ_keys = ("OSS_ENDPOINT", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
    if profile_name:
        environ_keys = tuple(
            f"{profile_name}__{environ_key}".upper() for environ_key in environ_keys
        )
    for environ_key in environ_keys:
        environ_endpoint_url = os.environ.get(environ_key)
        if environ_endpoint_url:
            return environ_endpoint_url
    if config is None:
        config = get_config_in_file(profile_name=profile_name)
    config_endpoint_url = config.get("s3", {}).get("endpoint_url")
    config_endpoint_url = config_endpoint_url or config.get("endpoint_url")
    if config_endpoint_url:
        return config_endpoint_url
    return DEFAULT_ENDPOINT_URL


def get_env_var(env_name: str, profile_name: T.Optional[str] = None) -> T.Optional[str]:
    profile_name = profile_name or os.environ.get("AWS_PROFILE")
    if profile_name:
        return os.getenv(f"{profile_name}__{env_name}".upper())
    return os.getenv(env_name.upper())


def get_session(profile_name: T.Optional[str] = None) -> aiobotocore.session.AioSession:
    session = aiobotocore.session.get_session()
    if profile_name:
        session.set_config_variable("profile", profile_name)
    return session


def get_config_in_file(
    profile_name: T.Optional[str] = None,
    session: T.Optional[aiobotocore.session.AioSession] = None,
) -> dict:
    if session is None:
        session = get_session(profile_name=profile_name)
    return session.get_scoped_config().get("s3", {})


def get_access_token(
    profile_name: T.Optional[str] = None, config: T.Optional[dict] = None
) -> T.Tuple[T.Optional[str], T.Optional[str], T.Optional[str]]:
    access_key = get_env_var("AWS_ACCESS_KEY_ID", profile_name=profile_name)
    secret_key = get_env_var("AWS_SECRET_ACCESS_KEY", profile_name=profile_name)
    session_token = get_env_var("AWS_SESSION_TOKEN", profile_name=profile_name)
    if access_key and secret_key:
        return access_key, secret_key, session_token

    try:
        if not config:
            config = get_config_in_file(profile_name=profile_name)
        if not access_key:
            access_key = config.get("aws_access_key_id")
        if not secret_key:
            secret_key = config.get("aws_secret_access_key")
        if not session_token:
            session_token = config.get("aws_session_token")
    except botocore.exceptions.ProfileNotFound:
        pass
    return access_key, secret_key, session_token


def get_s3_client(
    config: T.Optional[botocore.config.Config] = None,
    profile_name: T.Optional[str] = None,
    *,
    aws_access_key_id: T.Optional[str] = None,
    aws_secret_access_key: T.Optional[str] = None,
    aws_session_token: T.Optional[str] = None,
    endpoint_url: T.Optional[str] = None,
    addressing_style: bool = False,
) -> "S3Client":
    """Get S3 client

    :returns: S3 client
    """

    default_config = botocore.config.Config()
    if config:
        config = default_config.merge(config)
    else:
        config = default_config

    if not addressing_style:
        addressing_style = get_env_var(
            "AWS_S3_ADDRESSING_STYLE", profile_name=profile_name
        )
        if addressing_style:
            config = config.merge(
                botocore.config.Config(s3={"addressing_style": addressing_style})
            )

    session = get_session(profile_name=profile_name)
    s3_config = get_config_in_file(profile_name=profile_name, session=session)

    if not aws_session_token or (not aws_access_key_id or not aws_secret_access_key):
        aws_access_key_id, aws_secret_access_key, aws_session_token = get_access_token(
            profile_name, config=s3_config
        )
    if not endpoint_url:
        endpoint_url = get_endpoint_url(profile_name=profile_name, config=s3_config)

    client = session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        config=config,
    )
    return client


class S3FileSystem(BaseFileSystem):
    """
    Protocol for s3 operations.
    """

    protocol = "s3"

    def __init__(
        self,
        profile_name: T.Optional[str] = None,
        *,
        aws_access_key_id: T.Optional[str] = None,
        aws_secret_access_key: T.Optional[str] = None,
        endpoint_url: T.Optional[str] = None,
        addressing_style: bool = False,
    ):
        """Create a S3FileSystem instance.

        :param protocol_in_path: Whether incoming paths include the ``s3://`` prefix.
        """
        self._client: "S3Client | None" = None
        self._profile_name = profile_name

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._endpoint_url = endpoint_url
        self._addressing_style = addressing_style

    async def _get_client(self) -> "S3Client":
        if self._client is not None:
            return self._client
        client = get_s3_client(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            endpoint_url=self._endpoint_url,
            addressing_style=self._addressing_style,
        )
        self._client = await client.__aenter__()
        return self._client

    async def _close_client(self):
        if self._client is None:
            return
        await self._client.__aexit__(None, None, None)
        self._client = None

    def __del__(self):
        if self._client is not None:
            try:
                asyncio.get_running_loop().run_until_complete(self._close_client())
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

        if followlinks:
            try:
                s3_url = await self.readlink(path)
                bucket, key = parse_s3_url(s3_url)
            except S3NotALinkError:
                pass

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

    remove = unlink

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an iterator of ``FileEntry`` objects corresponding to the entries
            in the directory given by path.

        :param path: Directory path to scan.
        :type path: str
        :return: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.T.AsyncContextManager[T.T.AsyncIterator[FileEntry]]
        """
        return "S3ScandirContextManager(self, path)"

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

    async def upload(self, src_path: str, dst_path: str) -> None:
        """
        upload file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        client = await self._get_client()
        bucket, key = parse_s3_url(dst_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {dst_path!r}")
        if not await self.is_file(dst_path):
            raise S3FileNotFoundError(f"No such file: {dst_path!r}")

        await client.upload_file(src_path, bucket, key)

    async def download(self, src_path: str, dst_path: str) -> None:
        """
        download file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        if not await self.exists(src_path):
            raise FileNotFoundError(f"No such file: {src_path!r}")

        bucket, key = parse_s3_url(src_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {src_path!r}")
        if not await self.is_file(src_path):
            raise S3FileNotFoundError(f"No such file: {src_path!r}")

        client = await self._get_client()
        await client.download_file(bucket, key, dst_path)

    async def copy(self, src_path: str, dst_path: str) -> str:
        """
        Copy single file, not directory

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: Destination path after copy.
        """
        bucket, key = parse_s3_url(src_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {src_path!r}")
        if not await self.is_file(src_path):
            raise S3FileNotFoundError(f"No such file: {src_path!r}")

        dst_bucket, dst_key = parse_s3_url(dst_path)
        if not dst_bucket or not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {dst_path!r}")
        if await self.exists(dst_path):
            raise FileExistsError(f"File exists: {dst_path!r}")

        client = await self._get_client()
        await client.copy(
            {
                "Bucket": bucket,
                "Key": key,
            },
            dst_bucket,
            dst_key,
        )
        return dst_path

    async def sync(self, src_path: str, dst_path: str) -> None:
        """
        Sync file or directory.

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        async with self.scandir(src_path) as it:
            async for src_entry in it:
                src_path = src_entry.path
                dst_path = src_entry.path.replace(src_path, dst_path, 1)
                if src_entry.is_dir():
                    await self.mkdir(dst_path, exist_ok=True)
                    await self.sync(src_path, dst_path)
                else:
                    await self.copy(src_path, dst_path)

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        move file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :return: Destination path after move.
        :raises FileExistsError: If destination exists and overwrite is False.
        """
        async with self.scandir(src_path) as it:
            async for src_entry in it:
                src_path = src_entry.path
                dst_path = src_entry.path.replace(src_path, dst_path, 1)
                if src_entry.is_dir():
                    await self.mkdir(dst_path, exist_ok=True)
                    await self.sync(src_path, dst_path)
                else:
                    await self.copy(src_path, dst_path)

        if await self.exists(src_path):
            await self.unlink(src_path)
        return dst_path

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param src_path: The source path the symbolic link points to.
        :param dst_path: The symbolic link path.
        """
        if len(fspath(dst_path).encode()) > 1024:
            raise S3NameTooLongError("File name too long: %r" % dst_path)
        src_bucket, _ = parse_s3_url(src_path)
        dst_bucket, dst_key = parse_s3_url(dst_path)

        if not src_bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {src_path!r}")
        if not dst_bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {dst_path!r}")
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % dst_path)

        try:
            src_path = await self.readlink(src_path)
        except S3NotALinkError:
            pass
        client = await self._get_client()
        await client.put_object(
            Bucket=dst_bucket, Key=dst_key, Metadata={"symlink_to": src_path}
        )

    async def readlink(self, path: str) -> str:
        """
        Return a new path representing the symbolic link's target.

        :param path: The symbolic link path.
        :return: Target path of the symbolic link.
        """
        bucket, key = parse_s3_url(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {path!r}")
        if not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {path!r}")
        metadata = await self._s3_get_metadata(path)

        if "symlink_to" not in metadata:
            raise S3NotALinkError(f"Not a symbolic link: {path!r}")
        else:
            return metadata["symlink_to"]

    async def _s3_get_metadata(self, path: str) -> dict[str, T.Any]:
        """
        Get object metadata

        :param path: Object path
        :returns: Object metadata
        """
        bucket, key = parse_s3_url(path)
        if not bucket:
            return {}
        if not key or key.endswith("/"):
            return {}
        client = await self._get_client()
        try:
            resp = await client.head_object(Bucket=bucket, Key=key)
            return dict((key.lower(), value) for key, value in resp["Metadata"].items())
        except Exception as error:
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return {}

    def same_endpoint(self, other_filesystem: "BaseFileSystem") -> bool:
        """
        Return whether this filesystem points to the same endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True if both represent the same endpoint.
        """
        if not isinstance(other_filesystem, S3FileSystem):
            return False
        if other_filesystem._endpoint_url != self._endpoint_url:
            return False
        if other_filesystem._profile_name != self._profile_name:
            return False
        return True

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
        if self._profile_name:
            return f"{self.protocol}+{self._profile_name}://{path}"
        return f"{self.protocol}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "S3FileSystem":
        """Return new instance of this class

        :param uri: URI string.
        :return: new instance of new path
        """
        _, _, profile_name = split_uri(uri)
        return cls(profile_name=profile_name)
