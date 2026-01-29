import asyncio
import os
import re
import typing as T

import aiobotocore.session  # type: ignore
import botocore
from aiobotocore.config import AioConfig

from aiomegfile.config import GLOBAL_MAX_WORKERS
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
    SameFileError,
    raise_s3_error,
    translate_s3_error,
)
from aiomegfile.interfaces import (
    BaseFileSystem,
    FileEntry,
    ScanContextManager,
    StatResult,
)
from aiomegfile.utils.path import PathLike, fspath, split_uri

if T.TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client  # pyre-ignore[21]


DEFAULT_ENDPOINT_URL = "https://s3.amazonaws.com"
MAX_KEYS = 1000


DEFAULT_ENDPOINT_URL = "https://s3.amazonaws.com"
MAX_KEYS = 1000


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


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    bucket_pattern = re.match("(.*?)/", s3_path)
    if bucket_pattern is None:
        bucket = s3_path
        path = ""
    else:
        bucket = bucket_pattern.group(1)
        path = s3_path[len(bucket) + 1 :]
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
    config: T.Optional[AioConfig] = None,
    profile_name: T.Optional[str] = None,
    *,
    aws_access_key_id: T.Optional[str] = None,
    aws_secret_access_key: T.Optional[str] = None,
    aws_session_token: T.Optional[str] = None,
    endpoint_url: T.Optional[str] = None,
    addressing_style: T.Optional[str] = None,
) -> T.AsyncContextManager["S3Client"]:
    """Get S3 client

    :returns: S3 client
    """

    try:
        default_config = AioConfig(
            connect_timeout=5,
            max_pool_connections=GLOBAL_MAX_WORKERS,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    except TypeError:  # botocore < 1.36.0
        default_config = AioConfig(
            connect_timeout=5,
            max_pool_connections=GLOBAL_MAX_WORKERS,
        )

    if config:
        config = default_config.merge(config)
    else:
        config = default_config

    if not addressing_style:
        addressing_style = get_env_var(
            "AWS_S3_ADDRESSING_STYLE", profile_name=profile_name
        )
        if addressing_style:
            config = config.merge(AioConfig(s3={"addressing_style": addressing_style}))

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
        addressing_style: T.Optional[str] = None,
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

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a directory, otherwise False.
        """
        client = await self._get_client()
        bucket, key = parse_s3_path(path)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key
        prefix = _become_prefix(key)
        try:
            resp = await client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
            )
        except Exception as e:
            error = translate_s3_error(e, path)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error from e
            return False

        if not key:  # bucket is accessible
            return True

        if "KeyCount" in resp:
            return resp["KeyCount"] > 0

        return (
            len(resp.get("Contents", [])) > 0 or len(resp.get("CommonPrefixes", [])) > 0
        )

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a regular file, otherwise False.
        """
        if followlinks:
            try:
                path = await self.readlink(path)
            except S3NotALinkError:
                pass

        client = await self._get_client()
        bucket, key = parse_s3_path(path)
        if not bucket or not key or key.endswith("/"):
            # s3://, s3:///key, s3://bucket, s3://bucket/prefix/
            return False

        try:
            await client.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            error = translate_s3_error(e, path)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error from e
            return False
        return True

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param path: Path to check.
        :param followlinks: Whether to follow symbolic links.
        :return: True if the path exists, otherwise False.
        """
        if followlinks:
            try:
                path = await self.readlink(path)
            except S3NotALinkError:
                pass

        bucket, key = parse_s3_path(path)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key

        return await self.is_file(path) or await self.is_dir(path)

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Get the status of the path.

        :param path: Path to stat.
        :param followlinks: Whether to follow symbolic links.
        :raises FileNotFoundError: If the path does not exist.
        :return: Populated StatResult for the path.
        """
        bucket, key = parse_s3_path(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {self.build_uri(path)!r}")

        if not await self.is_file(path):
            return await self._get_dir_stat(path)

        islnk = False
        client = await self._get_client()

        with raise_s3_error(self.build_uri(path)):
            content = await client.head_object(Bucket=bucket, Key=key)
        if "Metadata" in content:
            metadata = dict(
                (key.lower(), value) for key, value in content["Metadata"].items()
            )
            if metadata and "symlink_to" in metadata:
                islnk = True
                if islnk and followlinks:
                    s3_url = metadata["symlink_to"]
                    bucket, key = parse_s3_path(s3_url)
                    with raise_s3_error(self.build_uri(s3_url[len("s3://") :])):
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
        bucket, key = parse_s3_path(path)
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
            raise S3FileNotFoundError(
                f"No such file or directory: {self.build_uri(path)!r}"
            )

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

        with raise_s3_error(self.build_uri(f"{bucket}/{prefix}")):
            resp = await client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=MAX_KEYS
            )

        while True:
            yield resp

            if not resp["IsTruncated"]:
                break

            with raise_s3_error(self.build_uri(f"{bucket}/{prefix}")):
                resp = await client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    Delimiter=delimiter,
                    ContinuationToken=resp["NextContinuationToken"],
                    MaxKeys=MAX_KEYS,
                )

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file or directory.

        If path is a file, remove it directly.
        If path is a directory, remove it and all its contents recursively.

        :param path: Path to remove.
        :param missing_ok: If False, raise when the path does not exist.
        :raises FileNotFoundError: When missing_ok is False and the path is absent.
        """
        bucket, key = parse_s3_path(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {self.build_uri(path)!r}")
        if not key:
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(path)!r}")

        client = await self._get_client()

        filename = os.path.basename(key)

        # guess is file for reduce requests
        if not filename.endswith("/") and "." in filename:
            try:
                await client.delete_object(Bucket=bucket, Key=key)
                return
            except Exception as e:
                error = translate_s3_error(e, path)
                if not isinstance(error, S3FileNotFoundError):
                    raise error from e

        prefix = _become_prefix(key)
        had_file = False
        async for resp in self._list_objects_recursive(bucket, prefix):
            contents = resp.get("Contents", [])
            if not contents:
                continue
            had_file = True

            objects_to_delete = [{"Key": obj["Key"]} for obj in contents]

            with raise_s3_error(self.build_uri(path)):
                await client.delete_objects(
                    Bucket=bucket,
                    Delete={  # pyre-ignore[6]
                        "Objects": objects_to_delete,
                        "Quiet": True,
                    },
                )

        if not had_file and not missing_ok:
            raise S3FileNotFoundError(
                f"No such file or directory: {self.build_uri(path)!r}"
            )

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an iterator of ``FileEntry`` objects corresponding to the entries
            in the directory given by path.

        :param path: Directory path to scan.
        :type path: str
        :return: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            bucket, key = parse_s3_path(path)
            if not bucket:
                return

            prefix = _become_prefix(key)

            async for resp in self._list_objects_recursive(
                bucket, prefix, delimiter="/"
            ):
                # Yield files
                for content in resp.get("Contents", []):
                    obj_key = content["Key"]
                    # Skip the prefix itself
                    if obj_key == prefix:
                        continue
                    name = obj_key[len(prefix) :]
                    if not name or "/" in name:
                        continue
                    current_path = f"{bucket}/{obj_key}"
                    stat_result = StatResult(
                        st_size=content["Size"],
                        st_mtime=content["LastModified"].timestamp(),
                        isdir=False,
                        extra=content,
                    )
                    yield FileEntry(name=name, path=current_path, stat=stat_result)

                # Yield directories (common prefixes)
                for common_prefix in resp.get("CommonPrefixes", []):
                    dir_prefix = common_prefix["Prefix"]
                    name = dir_prefix[len(prefix) :].rstrip("/")
                    if not name:
                        continue
                    current_path = f"{bucket}/{dir_prefix.rstrip('/')}"
                    stat_result = StatResult(
                        st_size=0,
                        st_mtime=0.0,
                        isdir=True,
                    )
                    yield FileEntry(name=name, path=current_path, stat=stat_result)

        return ScanContextManager(aiterator())

    def scanfile(
        self,
        path,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """
        Iteratively traverse only files in given directory.
        Every iteration on generator yields FileEntry object.

        :returns: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            bucket, key = parse_s3_path(path)
            if not bucket:
                return

            prefix = _become_prefix(key)

            async for resp in self._list_objects_recursive(bucket, prefix):
                for content in resp.get("Contents", []):
                    if content["Key"].endswith("/"):
                        continue
                    current_path = f"{bucket}/{content['Key']}"

                    islnk = False
                    if content["Size"] == 0:
                        islnk = await self.is_symlink(current_path)

                    yield FileEntry(
                        os.path.basename(content["Key"]),
                        current_path,
                        StatResult(
                            st_size=content["Size"],
                            st_mtime=content["LastModified"].timestamp(),
                            isdir=False,
                            islnk=islnk,
                            extra=content,
                        ),
                    )

        return ScanContextManager(aiterator())

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
        bucket, key = parse_s3_path(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {self.build_uri(path)!r}")
        elif not key:
            raise S3FileNotFoundError(f"Key not found: {self.build_uri(path)!r}")

        if exist_ok:
            return
        if await self.exists(path):
            raise S3FileExistsError(f"File exists: {self.build_uri(path)!r}")

    async def upload(self, src_path: str, dst_path: str) -> None:
        """
        Upload a local file to S3.

        :param src_path: Local source file path.
        :param dst_path: S3 destination path.
        :return: ``None``.
        """
        bucket, key = parse_s3_path(dst_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(dst_path)!r}")
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"No such file: {self.build_uri(src_path)!r}")
        if os.path.isdir(src_path):
            raise IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        client = await self._get_client()
        with raise_s3_error(self.build_uri(dst_path)):
            await client.upload_file(src_path, bucket, key)

    async def download(self, src_path: str, dst_path: str) -> None:
        """
        download file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: ``None``.
        """
        if not await self.exists(src_path):
            raise FileNotFoundError(f"No such file: {self.build_uri(src_path)!r}")

        bucket, key = parse_s3_path(src_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        client = await self._get_client()
        try:
            await client.download_file(bucket, key, dst_path)
        except Exception as e:
            error = translate_s3_error(e, self.build_uri(src_path))
            if await self.is_dir(src_path):
                raise S3IsADirectoryError(
                    "Is a directory: %r" % self.build_uri(src_path)
                )
            raise error from e

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
        followlinks: bool = False,
    ) -> str:
        """
        Copy single file, not directory

        :param src_path: Given source path
        :param dst_path: Given destination path
        :param callback: Called periodically during copy, and the input parameter is
            the data size (in bytes) of copy since the last call
        :param followlinks: False if regard symlink as file, else True
        :return: Destination path after copy.
        """
        if followlinks:
            try:
                src_path = await self.readlink(src_path)
            except S3NotALinkError:
                pass

        src_bucket, src_key = parse_s3_path(src_path)
        if not src_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(src_path)!r}"
            )
        if not src_key or src_key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        dst_bucket, dst_key = parse_s3_path(dst_path)
        if not dst_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(dst_path)!r}"
            )
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(dst_path)!r}")

        if dst_bucket == src_bucket and src_key.rstrip("/") == dst_key.rstrip("/"):
            raise SameFileError(
                f"'{self.build_uri(src_path)}' and "
                f"'{self.build_uri(dst_path)}' are the same file"
            )

        client = await self._get_client()

        try:
            await client.copy_object(
                CopySource={"Bucket": src_bucket, "Key": src_key},  # pyre-ignore[6]
                Bucket=dst_bucket,
                Key=dst_key,
            )
            if callback:
                stat_result = await self.stat(src_path)
                callback(stat_result.st_size)
            return dst_path
        except Exception as e:
            error = translate_s3_error(
                e, f"'{self.build_uri(src_path)}' or '{self.build_uri(dst_path)}'"
            )
            if await self.is_dir(src_path):
                raise S3IsADirectoryError(
                    "Is a directory: %r" % self.build_uri(src_path)
                )
            raise error from e

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        Move file or directory.

        :param src_path: Given source path.
        :param dst_path: Given destination path.
        :param overwrite: Whether to overwrite file when exists.
        :return: Destination path after move.
        :raises FileExistsError: If destination exists and overwrite is False.
        """
        if not overwrite and await self.exists(dst_path):
            raise S3FileExistsError(f"File exists: {self.build_uri(dst_path)!r}")

        src_bucket, src_key = parse_s3_path(src_path)
        if not src_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(src_path)!r}"
            )
        if not src_key:
            raise FileNotFoundError(f"Key not found: {self.build_uri(src_path)!r}")
        dst_bucket, dst_key = parse_s3_path(dst_path)
        if not dst_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(dst_path)!r}"
            )
        if not dst_key:
            raise FileNotFoundError(f"Key not found: {self.build_uri(dst_path)!r}")

        client = await self._get_client()
        async for resp in self._list_objects_recursive(src_bucket, src_key):
            contents = resp.get("Contents", [])
            if not contents:
                continue

            for obj in contents:
                current_src_key = obj["Key"]
                current_dst_key = current_src_key.replace(src_key, dst_key, 1)

                with raise_s3_error(
                    f"'{self.build_uri(src_path)}' or '{self.build_uri(dst_path)}'"
                ):
                    await client.copy_object(
                        CopySource={  # pyre-ignore[6]
                            "Bucket": src_bucket,
                            "Key": current_src_key,
                        },
                        Bucket=dst_bucket,
                        Key=current_dst_key,
                    )

        await self.remove(src_path, missing_ok=True)
        return dst_path

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param src_path: The source path the symbolic link points to.
        :param dst_path: The symbolic link path.
        """
        if len(fspath(dst_path).encode()) > 1024:
            raise S3NameTooLongError(
                "File name too long: %r" % self.build_uri(dst_path)
            )
        src_bucket, _ = parse_s3_path(src_path)
        dst_bucket, dst_key = parse_s3_path(dst_path)

        if not src_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(src_path)!r}"
            )
        if not dst_bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(dst_path)!r}"
            )
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % self.build_uri(dst_path))

        try:
            src_path = await self.readlink(src_path)
        except S3NotALinkError:
            pass
        client = await self._get_client()
        await client.put_object(
            Bucket=dst_bucket,
            Key=dst_key,
            Metadata={"symlink_to": self.build_uri(src_path)},
        )

    async def readlink(self, path: str) -> str:
        """
        Return a new path representing the symbolic link's target.

        :param path: The symbolic link path.
        :return: Target path of the symbolic link.
        """
        bucket, key = parse_s3_path(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {self.build_uri(path)!r}")
        if not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(path)!r}")

        client = await self._get_client()
        try:
            resp = await client.head_object(Bucket=bucket, Key=key)
            metadata = dict(
                (key.lower(), value) for key, value in resp["Metadata"].items()
            )
        except Exception as error:
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            metadata = {}

        if "symlink_to" not in metadata:
            raise S3NotALinkError(f"Not a symbolic link: {self.build_uri(path)!r}")
        else:
            return self.parse_uri(metadata["symlink_to"])

    async def is_symlink(self, path: str) -> bool:
        """Return True if the path points to a symbolic link.

        :param path: The path to check.
        :return: True if the path is a symbolic link, otherwise False.
        """
        try:
            await self.readlink(path)
            return True
        except (S3NotALinkError, S3FileNotFoundError, S3IsADirectoryError):
            return False

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
