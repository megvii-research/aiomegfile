import asyncio
import io
import logging
import os
import re
import typing as T
import weakref

import aiobotocore.session  # type: ignore
import aiofiles
import botocore
from aiobotocore.config import AioConfig
from botocore.utils import calculate_md5 as botocore_calculate_md5

from aiomegfile.config import GLOBAL_MAX_WORKERS, READER_BLOCK_SIZE
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
    Access,
    AioClosable,
    AioScannableManager,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.lib.cacher import AioCacher
from aiomegfile.lib.fnmatch import translate
from aiomegfile.lib.glob import has_magic, has_magic_ignore_brace, ungloblize
from aiomegfile.lib.s3_buffered_writer import AioS3BufferedWriter
from aiomegfile.lib.s3_prefetch_reader import AioS3PrefetchReader
from aiomegfile.lib.s3_retry import register_retry_handler
from aiomegfile.utils.path import PathLike, fspath, split_uri

if T.TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client  # pyre-ignore[21]

logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT_URL = "https://s3.amazonaws.com"
MAX_KEYS = 1000

# Patch for https://github.com/aws/aws-cli/issues/9214
CALCULATE_MD5_FOR_OPERATIONS = {
    "DeleteObjects",
}

_S3_CLIENT_CACHE = weakref.WeakKeyDictionary()
_S3_CLIENT_LOCKS = weakref.WeakKeyDictionary()


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


def parse_s3_path(s3_path: PathLike) -> tuple[str, str]:
    s3_path = fspath(s3_path)
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


def _s3_entry_name(path: str) -> str:
    """Return the basename for a S3 path.

    :param path: S3 path without protocol.
    :return: Basename of the path, with trailing slashes ignored.
    """
    stripped = path.rstrip("/")
    if not stripped:
        return ""
    return os.path.basename(stripped)


def _parse_s3_path_ignore_brace(s3_pathname: PathLike) -> tuple[str, str]:
    """Parse bucket and key from a path, ignoring braces when splitting.

    :param s3_pathname: S3 path without protocol.
    :return: Tuple of (bucket, key).
    """
    s3_pathname = fspath(s3_pathname)
    left_brace = False
    for current_index, current_character in enumerate(s3_pathname):
        if current_character == "/" and left_brace is False:
            return s3_pathname[:current_index], s3_pathname[current_index + 1 :]
        if current_character == "{":
            left_brace = True
        elif current_character == "}":
            left_brace = False
    return s3_pathname, ""


def _s3_split_magic_ignore_brace(s3_pathname: str) -> tuple[str, str]:
    """Split a path into non-magic and magic parts while ignoring braces.

    :param s3_pathname: S3 path without protocol.
    :return: Tuple of (top_dir, magic_part).
    """
    left_brace, left_index = False, 0
    normal_parts: T.List[str] = []
    magic_parts: T.List[str] = []
    s3_pathname_with_suffix = s3_pathname
    s3_pathname = s3_pathname.rstrip("/")
    suffix = (len(s3_pathname_with_suffix) - len(s3_pathname)) * "/"
    for current_index, current_character in enumerate(s3_pathname):
        if current_character == "/" and left_brace is False:
            segment = s3_pathname[left_index:current_index]
            if has_magic_ignore_brace(segment):
                magic_parts.append(segment)
                if s3_pathname[current_index + 1 :]:
                    magic_parts.append(s3_pathname[current_index + 1 :])
                    left_index = len(s3_pathname)
                break
            normal_parts.append(segment)
            left_index = current_index + 1
        elif current_character == "{":
            left_brace = True
        elif current_character == "}":
            left_brace = False
    if s3_pathname[left_index:]:
        segment = s3_pathname[left_index:]
        if has_magic_ignore_brace(segment):
            magic_parts.append(segment)
        else:
            normal_parts.append(segment)
    top_dir = "/".join(normal_parts)
    magic_part = "/".join(magic_parts)
    if suffix:
        if magic_part:
            magic_part += suffix
        else:
            top_dir += suffix
    return top_dir, magic_part


def _s3_split_magic(s3_pathname: str) -> tuple[str, str]:
    """Split a path into the longest non-magic prefix and the remaining part.

    :param s3_pathname: S3 path without protocol.
    :return: Tuple of (prefix_without_magic, wildcard_part).
    """
    for index in range(len(s3_pathname) - 1, 0, -1):
        if not has_magic(s3_pathname[:index]):
            return s3_pathname[:index], s3_pathname[index:]
    return s3_pathname, ""


def _group_s3path_by_prefix(s3_pathname: str) -> T.List[str]:
    """Expand brace patterns in the prefix part of a S3 path.

    :param s3_pathname: S3 path without protocol.
    :return: List of expanded S3 paths with prefixes resolved.
    """
    _, key = parse_s3_path(s3_pathname)
    if not key:
        return ungloblize(s3_pathname)

    top_dir, magic_part = _s3_split_magic_ignore_brace(s3_pathname)
    if not top_dir:
        return [magic_part]
    grouped_path = []
    for pathname in ungloblize(top_dir):
        if magic_part:
            pathname = "/".join([pathname, magic_part])
        grouped_path.append(pathname)
    return grouped_path


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


def _register_add_md5_header(client: "S3Client") -> None:
    # botocore > 1.36.0 change default checksum from md5sum to crc32.
    # Even though `request_checksum_calculation` and `response_checksum_validation`
    # are set to "when_required", some endpoints are still encountering issues.
    # Therefore, I am applying a patch.
    # Related issues:
    # https://github.com/aws/aws-cli/issues/9214
    # https://github.com/boto/boto3/issues/4409
    # https://github.com/boto/boto3/issues/4400
    def _add_md5_header(params: T.Dict[str, T.Any], **kwargs) -> None:
        if "body" in params and "headers" in params:
            if "Content-MD5" not in params["headers"]:
                md5_digest = botocore_calculate_md5(params["body"])
                params["headers"]["Content-MD5"] = md5_digest

    for operation in CALCULATE_MD5_FOR_OPERATIONS:
        client.meta.events.register(f"before-call.s3.{operation}", _add_md5_header)


async def _get_s3_client(
    profile_name: T.Optional[str] = None,
    *,
    aws_access_key_id: T.Optional[str] = None,
    aws_secret_access_key: T.Optional[str] = None,
    aws_session_token: T.Optional[str] = None,
    endpoint_url: T.Optional[str] = None,
    addressing_style: T.Optional[str] = None,
) -> "S3Client":
    """Get S3 client

    :returns: S3 client
    """

    config = AioConfig(
        connect_timeout=5,
        max_pool_connections=GLOBAL_MAX_WORKERS,
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

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

    client_context = session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        config=config,
    )
    client = await client_context.__aenter__()

    max_attempts = client.meta.config.retries.get(  # pyre-ignore[16]
        "total_max_attempts"
    )
    kwargs = {"client": client}
    if max_attempts is not None:
        kwargs["max_attempts"] = max_attempts
    register_retry_handler(**kwargs)
    _register_add_md5_header(client)
    return client


async def get_s3_client(
    profile_name: T.Optional[str] = None,
    *,
    aws_access_key_id: T.Optional[str] = None,
    aws_secret_access_key: T.Optional[str] = None,
    aws_session_token: T.Optional[str] = None,
    endpoint_url: T.Optional[str] = None,
    addressing_style: T.Optional[str] = None,
) -> "S3Client":
    """Get a cached S3 client bound to the current event loop.

    :param profile_name: Optional AWS profile name.
    :type profile_name: T.Optional[str]
    :param aws_access_key_id: Optional AWS access key ID override.
    :type aws_access_key_id: T.Optional[str]
    :param aws_secret_access_key: Optional AWS secret access key override.
    :type aws_secret_access_key: T.Optional[str]
    :param aws_session_token: Optional AWS session token override.
    :type aws_session_token: T.Optional[str]
    :param endpoint_url: Optional custom S3 endpoint URL.
    :type endpoint_url: T.Optional[str]
    :param addressing_style: Optional S3 addressing style.
    :type addressing_style: T.Optional[str]
    :return: An initialized S3 client bound to the current loop.
    :rtype: S3Client
    """

    loop = asyncio.get_running_loop()
    cache = _S3_CLIENT_CACHE.setdefault(loop, {})
    lock = _S3_CLIENT_LOCKS.setdefault(loop, asyncio.Lock())
    cache_key = (
        profile_name,
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
        endpoint_url,
        addressing_style,
    )
    if cache_key not in cache:
        async with lock:
            if cache_key not in cache:
                cache[cache_key] = await _get_s3_client(
                    profile_name=profile_name,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_session_token=aws_session_token,
                    endpoint_url=endpoint_url,
                    addressing_style=addressing_style,
                )
    return cache[cache_key]


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

        self._client = await get_s3_client(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            endpoint_url=self._endpoint_url,
            addressing_style=self._addressing_style,
        )
        return self._client

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

        file_task = asyncio.create_task(self.is_file(path))
        dir_task = asyncio.create_task(self.is_dir(path))
        if await file_task:
            dir_task.cancel()
            return True
        return await dir_task

    async def absolute(self, path: str) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object.

        :param path: The path to make absolute.
        :return: Absolute path string.
        """
        path_str = fspath(path)
        if "://" in path_str:
            _, path_str, _ = split_uri(path_str)
        return path_str.lstrip("/")

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

        next_resp = None
        try:
            while True:
                if resp["IsTruncated"]:
                    next_resp = asyncio.create_task(
                        client.list_objects_v2(
                            Bucket=bucket,
                            Prefix=prefix,
                            Delimiter=delimiter,
                            ContinuationToken=resp["NextContinuationToken"],
                            MaxKeys=MAX_KEYS,
                        )
                    )

                yield resp

                if not next_resp:
                    break

                with raise_s3_error(self.build_uri(f"{bucket}/{prefix}")):
                    resp = await next_resp
                next_resp = None
        finally:
            if next_resp is not None:
                next_resp.cancel()

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
        max_workers = max(GLOBAL_MAX_WORKERS, 1)
        semaphore = asyncio.Semaphore(max_workers)
        max_in_flight = max_workers * 2
        delete_tasks: set[asyncio.Task[None]] = set()
        error_path = self.build_uri(path)

        async def _delete_batch(objects: T.List[T.Dict[str, str]]) -> None:
            """Delete a batch of S3 objects.

            :param objects: Object dicts with ``Key`` values.
            """
            async with semaphore:
                with raise_s3_error(error_path):
                    await client.delete_objects(
                        Bucket=bucket,
                        Delete={
                            "Objects": objects,
                            "Quiet": True,
                        },
                    )

        async def _drain_delete_tasks(
            tasks: set[asyncio.Task[None]],
        ) -> set[asyncio.Task[None]]:
            """Wait for at least one delete task and propagate errors.

            :param tasks: Active delete tasks.
            :return: Remaining pending tasks.
            """
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_task in done:
                try:
                    await completed_task
                except Exception:
                    for pending_task in pending:
                        pending_task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    raise
            return set(pending)

        async for resp in self._list_objects_recursive(bucket, prefix):
            contents = resp.get("Contents", [])
            if not contents:
                continue
            had_file = True

            objects_to_delete = [{"Key": obj["Key"]} for obj in contents]

            delete_tasks.add(asyncio.create_task(_delete_batch(objects_to_delete)))
            if len(delete_tasks) >= max_in_flight:
                delete_tasks = await _drain_delete_tasks(delete_tasks)

        if delete_tasks:
            try:
                await asyncio.gather(*delete_tasks)
            except Exception:
                for pending_task in delete_tasks:
                    if not pending_task.done():
                        pending_task.cancel()
                await asyncio.gather(*delete_tasks, return_exceptions=True)
                raise

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
                if key:
                    raise S3BucketNotFoundError(
                        "Empty bucket name: %r" % self.build_uri(path)
                    )
                client = await self._get_client()
                response = await client.list_buckets()
                for content in response["Buckets"]:
                    yield FileEntry(
                        content["Name"],
                        content["Name"],
                        StatResult(
                            st_ctime=content["CreationDate"].timestamp(),
                            isdir=True,
                            extra=content,
                        ),
                    )
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

        return AioScannableManager(aiterator())

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
            if key and not key.endswith("/"):
                if await self.is_file(path):
                    stat_result = await self.stat(path)
                    yield FileEntry(
                        os.path.basename(key),
                        f"{bucket}/{key}",
                        stat_result,
                    )
                    return

            prefix = _become_prefix(key)

            async for resp in self._list_objects_recursive(bucket, prefix):
                contents = resp.get("Contents", [])
                if not contents:
                    continue

                max_workers = max(GLOBAL_MAX_WORKERS, 1)
                semaphore = asyncio.Semaphore(max_workers)

                async def _check_symlink(path: str) -> bool:
                    """Check whether a path is a symbolic link with concurrency control.

                    :param path: Path to check.
                    :return: True if path is a symlink, otherwise False.
                    """
                    async with semaphore:
                        return await self.is_symlink(path)

                islnk_values = [False] * len(contents)
                pending_tasks: T.List[T.Tuple[int, asyncio.Task[bool]]] = []
                for index, content in enumerate(contents):
                    if content["Key"].endswith("/"):
                        continue
                    if content["Size"] == 0:
                        current_path = f"{bucket}/{content['Key']}"
                        pending_tasks.append(
                            (index, asyncio.create_task(_check_symlink(current_path)))
                        )

                if pending_tasks:
                    tasks = [task for _, task in pending_tasks]
                    try:
                        results = await asyncio.gather(*tasks)
                    except Exception:
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        raise
                    for (index, _), result in zip(pending_tasks, results):
                        islnk_values[index] = result

                for index, content in enumerate(contents):
                    if content["Key"].endswith("/"):
                        continue
                    current_path = f"{bucket}/{content['Key']}"
                    yield FileEntry(
                        os.path.basename(content["Key"]),
                        current_path,
                        StatResult(
                            st_size=content["Size"],
                            st_mtime=content["LastModified"].timestamp(),
                            isdir=False,
                            islnk=islnk_values[index],
                            extra=content,
                        ),
                    )

        return AioScannableManager(aiterator())

    async def _list_all_buckets(self) -> T.List[str]:
        """Return a list of accessible bucket names.

        :return: List of bucket names.
        """
        client = await self._get_client()
        with raise_s3_error(self.build_uri("")):
            response = await client.list_buckets()
        return [content["Name"] for content in response.get("Buckets", [])]

    async def _group_s3path_by_bucket(self, s3_pathname: str) -> T.List[str]:
        """Expand bucket glob patterns into concrete paths.

        :param s3_pathname: S3 path without protocol.
        :return: List of grouped S3 paths.
        :raises S3BucketNotFoundError: If bucket name is empty.
        """
        bucket, key = _parse_s3_path_ignore_brace(s3_pathname)
        if not bucket:
            raise S3BucketNotFoundError(
                f"Empty bucket name: {self.build_uri(s3_pathname)!r}"
            )

        grouped_path: T.List[str] = []

        def generate_s3_path(bucket_name: str, key_name: str) -> str:
            if key_name:
                return f"{bucket_name}/{key_name}"
            suffix = "/" if s3_pathname.endswith("/") else ""
            return f"{bucket_name}{suffix}"

        all_buckets: T.Optional[T.List[str]] = None
        for bucket_name in ungloblize(bucket):
            if has_magic(bucket_name):
                if all_buckets is None:
                    all_buckets = await self._list_all_buckets()
                split_bucket_name = bucket_name.split("/", 1)
                path_part: T.Optional[str] = None
                if len(split_bucket_name) == 2:
                    bucket_name, path_part = split_bucket_name
                pattern = re.compile(translate(re.sub(r"\*{2,}", "*", bucket_name)))
                for current_bucket in all_buckets:
                    if pattern.fullmatch(current_bucket) is not None:
                        if path_part is not None:
                            current_bucket = f"{current_bucket}/{path_part}"
                        grouped_path.append(generate_s3_path(current_bucket, key))
            else:
                grouped_path.append(generate_s3_path(bucket_name, key))
        return grouped_path

    async def _glob_stat_single_path(
        self,
        s3_pathname: str,
        recursive: bool = True,
        followlinks: bool = False,
    ) -> T.AsyncIterator[FileEntry]:
        """Yield FileEntry objects matching a single glob pattern.

        :param s3_pathname: S3 path without protocol.
        :param recursive: If False, ``**`` will not search directory recursively.
        :param followlinks: Whether to follow symbolic links.
        :return: Async iterator of FileEntry objects.
        """
        s3_pathname = fspath(s3_pathname)
        if not recursive:
            s3_pathname = re.sub(r"\*{2,}", "*", s3_pathname)
        top_prefix, wildcard_part = _s3_split_magic(s3_pathname)
        top_dir = os.path.dirname(top_prefix) if wildcard_part else top_prefix
        search_dir = wildcard_part.endswith("/")

        def should_recursive(wildcard: str) -> bool:
            if "**" in wildcard:
                return True
            for expanded_path in ungloblize(wildcard):
                parts_length = len(expanded_path.split("/"))
                if parts_length + search_dir >= 2:
                    return True
            return False

        if not has_magic(s3_pathname):
            if await self.is_file(s3_pathname, followlinks=followlinks):
                stat_result = await self.stat(s3_pathname, followlinks=followlinks)
                yield FileEntry(
                    _s3_entry_name(s3_pathname),
                    s3_pathname,
                    stat_result,
                )
            if await self.is_dir(s3_pathname, followlinks=followlinks):
                yield FileEntry(
                    _s3_entry_name(s3_pathname),
                    s3_pathname,
                    StatResult(isdir=True),
                )
            return

        delimiter = "" if should_recursive(wildcard_part) else "/"

        pattern = re.compile(translate(s3_pathname))
        bucket, prefix = parse_s3_path(top_prefix)

        active_dirs: T.List[str] = []

        def collect_dir_chain(path: str) -> T.List[str]:
            """Collect directory chain from top_dir to leaf.

            :param path: Path to collect directories for.
            :return: Directory chain from root to leaf, excluding top_dir.
            """
            chain: T.List[str] = []
            dirname = os.path.dirname(path)
            while dirname and dirname != top_dir:
                chain.append(dirname)
                parent = os.path.dirname(dirname)
                if parent == dirname:
                    break
                dirname = parent
            chain.reverse()
            return chain

        def iter_new_dir_entries(path: str) -> T.Iterator[FileEntry]:
            """Yield new directory entries based on current path.

            :param path: Path used to determine new directories.
            :return: Iterator of FileEntry for newly discovered directories.
            """
            nonlocal active_dirs
            current_dirs = collect_dir_chain(path)
            common_length = 0
            for left_dir, right_dir in zip(active_dirs, current_dirs):
                if left_dir != right_dir:
                    break
                common_length += 1
            for dirname in reversed(current_dirs[common_length:]):
                match_path = dirname + "/" if search_dir else dirname
                if pattern.match(match_path):
                    yield FileEntry(
                        _s3_entry_name(match_path),
                        match_path,
                        StatResult(isdir=True),
                    )
            active_dirs = current_dirs

        async for resp in self._list_objects_recursive(bucket, prefix, delimiter):
            for content in resp.get("Contents", []):
                obj_key = content["Key"]
                path = f"{bucket}/{obj_key}"
                if not search_dir and pattern.match(path):
                    if path.endswith("/"):
                        continue
                    stat_result = StatResult(
                        st_size=content["Size"],
                        st_mtime=content["LastModified"].timestamp(),
                        isdir=False,
                        islnk=bool(content.get("islnk", False)),
                        extra=content,
                    )
                    yield FileEntry(
                        _s3_entry_name(path),
                        path,
                        stat_result,
                    )
                for file_entry in iter_new_dir_entries(path):
                    yield file_entry
            for common_prefix in resp.get("CommonPrefixes", []):
                path = f"{bucket}/{common_prefix['Prefix']}"
                for file_entry in iter_new_dir_entries(path):
                    yield file_entry

    async def glob_stat(
        self,
        path: str,
        recursive: bool = True,
        missing_ok: bool = True,
    ) -> T.AsyncIterator[FileEntry]:
        """Return entries whose paths match the glob pattern.

        :param path: S3 glob pattern without protocol.
        :param recursive: If False, ``**`` will not search directory recursively.
        :param missing_ok: If False and no matches exist, raise FileNotFoundError.
        :return: Async iterator of FileEntry objects.
        :raises S3FileNotFoundError: If no matches and missing_ok is False.
        """
        s3_pathname = fspath(path)
        grouped_paths: T.List[str] = []
        for group_path_bucket in await self._group_s3path_by_bucket(s3_pathname):
            grouped_paths.extend(_group_s3path_by_prefix(group_path_bucket))

        matched = False
        if grouped_paths:
            max_workers = max(GLOBAL_MAX_WORKERS, 1)
            semaphore = asyncio.Semaphore(max_workers)
            done_sentinel = object()
            result_queue: asyncio.Queue[object] = asyncio.Queue()

            async def _run_group(group_path_prefix: str) -> None:
                """Run glob_stat for a grouped prefix and enqueue results.

                :param group_path_prefix: Grouped path prefix to scan.
                """
                async with semaphore:
                    async for file_entry in self._glob_stat_single_path(
                        group_path_prefix,
                        recursive=recursive,
                    ):
                        await result_queue.put(file_entry)

            tasks = [
                asyncio.create_task(_run_group(group_path_prefix))
                for group_path_prefix in grouped_paths
            ]

            async def _finish_groups() -> T.Tuple[object, ...]:
                """Wait for group tasks and signal completion.

                :return: Tuple of task results or exceptions.
                """
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await result_queue.put(done_sentinel)
                return results

            finisher = asyncio.create_task(_finish_groups())
            try:
                while True:
                    item = await result_queue.get()
                    if item is done_sentinel:
                        break
                    matched = True
                    yield T.cast(FileEntry, item)
            finally:
                if not finisher.done():
                    for task in tasks:
                        task.cancel()
                task_results = await finisher
                for result in task_results:
                    if isinstance(result, Exception) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        raise result

        if not matched and not missing_ok:
            raise S3FileNotFoundError(
                f"No match any file: {self.build_uri(s3_pathname)!r}"
            )

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

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
    ) -> T.AsyncContextManager:
        """Open the file with mode.

        :param path: File path to open.
        :param mode: File open mode.
        :param buffering: Buffering policy. Not used in s3.
        :param encoding: Text encoding when using text modes.
        :param errors: Error handling strategy for encoding/decoding.
        :param newline: Newline handling in text mode.
        :return: Async file context manager.
        """
        if "x" in mode:
            raise ValueError("unacceptable 'x' mode: %r" % mode)

        bucket, key = parse_s3_path(path)
        if not bucket:
            raise S3BucketNotFoundError(f"Empty bucket name: {self.build_uri(path)!r}")
        if not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(path)!r}")

        if "a" in mode or "+" in mode:
            return AioCacher(
                path,
                mode,
                download_fileobj=self._download_fileobj,
                upload_fileobj=self._upload_fileobj,
            )

        if "w" in mode:
            fileobj = AioS3BufferedWriter(
                bucket=bucket,
                key=key,
                filesystem=self,
                mode=mode,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )
        else:
            fileobj = AioS3PrefetchReader(
                bucket=bucket,
                key=key,
                filesystem=self,
                mode=mode,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )
        return fileobj

    async def upload(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """
        Upload a local file to S3.

        :param src_path: Local source file path.
        :param dst_path: S3 destination path.
        :param callback: Called periodically during upload with bytes written.
        :return: ``None``.
        """
        bucket, key = parse_s3_path(dst_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(dst_path)!r}")
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"No such file: {self.build_uri(src_path)!r}")
        if os.path.isdir(src_path):
            raise IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        async with aiofiles.open(src_path, "rb") as fileobj:
            await self._upload_fileobj(fileobj, dst_path, callback=callback)

    async def download(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """
        download file

        :param src_path: Given source path
        :param dst_path: Given destination path
        :param callback: Called periodically during download with bytes written.
        :return: ``None``.
        """
        if not await self.exists(src_path):
            raise FileNotFoundError(f"No such file: {self.build_uri(src_path)!r}")

        bucket, key = parse_s3_path(src_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        dir_path = os.path.dirname(dst_path)
        if dir_path and dir_path != ".":
            await asyncio.to_thread(os.makedirs, dir_path, exist_ok=True)

        async with aiofiles.open(dst_path, "wb") as fileobj:
            await self._download_fileobj(src_path, fileobj, callback=callback)

    async def _download_fileobj(
        self,
        src_path: str,
        fileobj,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Download S3 object to a file-like object.

        :param src_path: S3 source path (without protocol).
        :param fileobj: File-like object to write to.
        :param callback: Called periodically during download with bytes written.
        :return: ``None``.
        """
        bucket, key = parse_s3_path(src_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(src_path)!r}")

        with raise_s3_error(self.build_uri(src_path)):
            mode = "rb" if "b" in fileobj.mode else "r"
            async with AioS3PrefetchReader(
                bucket=bucket,
                key=key,
                filesystem=self,
                mode=mode,
            ) as s3_file:
                while True:
                    chunk = await s3_file.read(1024 * 1024)
                    if not chunk:
                        break
                    await fileobj.write(chunk)
                    if callback:
                        callback(len(chunk))

    async def _upload_fileobj(
        self,
        fileobj,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Upload from a file-like object to S3.

        :param fileobj: File-like object to read from.
        :param dst_path: S3 destination path (without protocol).
        :param callback: Called periodically during upload with bytes written.
        :return: ``None``.
        """
        bucket, key = parse_s3_path(dst_path)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError(f"Is a directory: {self.build_uri(dst_path)!r}")

        with raise_s3_error(self.build_uri(dst_path)):
            mode = "wb" if "b" in fileobj.mode else "w"
            async with AioS3BufferedWriter(
                bucket=bucket,
                key=key,
                filesystem=self,
                mode=mode,
            ) as s3_file:
                while True:
                    chunk = await fileobj.read(1024 * 1024)
                    if not chunk:
                        break
                    await s3_file.write(chunk)
                    if callback:
                        callback(len(chunk))

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
        max_workers = max(GLOBAL_MAX_WORKERS, 1)
        semaphore = asyncio.Semaphore(max_workers)
        max_in_flight = max_workers * 2
        copy_tasks: set[asyncio.Task[None]] = set()
        error_path = f"'{self.build_uri(src_path)}' or '{self.build_uri(dst_path)}'"

        async def _copy_object(current_src_key: str, current_dst_key: str) -> None:
            """Copy a single object to the destination path.

            :param current_src_key: Source object key.
            :param current_dst_key: Destination object key.
            """
            async with semaphore:
                with raise_s3_error(error_path):
                    await client.copy_object(
                        CopySource={
                            "Bucket": src_bucket,
                            "Key": current_src_key,
                        },
                        Bucket=dst_bucket,
                        Key=current_dst_key,
                    )

        async def _drain_copy_tasks(
            tasks: set[asyncio.Task[None]],
        ) -> set[asyncio.Task[None]]:
            """Wait for at least one copy task and propagate errors.

            :param tasks: Active copy tasks.
            :return: Remaining pending tasks.
            """
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_task in done:
                try:
                    await completed_task
                except Exception:
                    for pending_task in pending:
                        pending_task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    raise
            return set(pending)

        async for resp in self._list_objects_recursive(src_bucket, src_key):
            contents = resp.get("Contents", [])
            if not contents:
                continue

            for obj in contents:
                current_src_key = obj["Key"]
                current_dst_key = current_src_key.replace(src_key, dst_key, 1)

                copy_tasks.add(
                    asyncio.create_task(_copy_object(current_src_key, current_dst_key))
                )
                if len(copy_tasks) >= max_in_flight:
                    copy_tasks = await _drain_copy_tasks(copy_tasks)

        if copy_tasks:
            try:
                await asyncio.gather(*copy_tasks)
            except Exception:
                for pending_task in copy_tasks:
                    if not pending_task.done():
                        pending_task.cancel()
                await asyncio.gather(*copy_tasks, return_exceptions=True)
                raise

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

    async def _group_src_paths_by_block(
        self, src_paths: T.List[PathLike], block_size: int = READER_BLOCK_SIZE
    ) -> T.List[T.List[T.Tuple[PathLike, T.Optional[str]]]]:
        groups = []
        current_group, current_group_size = [], 0
        if not src_paths:
            return groups

        max_workers = max(GLOBAL_MAX_WORKERS, 1)
        semaphore = asyncio.Semaphore(max_workers)

        async def _stat_size(src_path: PathLike) -> int:
            """Return file size for a path with concurrency control.

            :param src_path: Source path to stat.
            :return: File size in bytes.
            """
            async with semaphore:
                return (await self.stat(fspath(src_path))).st_size

        tasks = [asyncio.create_task(_stat_size(src_path)) for src_path in src_paths]
        try:
            sizes = await asyncio.gather(*tasks)
        except Exception:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for src_path, current_file_size in zip(src_paths, sizes):
            if current_file_size == 0:
                continue

            if current_file_size >= block_size:
                if len(groups) == 0:
                    if current_group_size + current_file_size > 2 * block_size:
                        group_lack_size = block_size - current_group_size
                        current_group.append(
                            (src_path, f"bytes=0-{group_lack_size - 1}")
                        )
                        groups.extend(
                            [
                                current_group,
                                [
                                    (
                                        src_path,
                                        f"bytes={group_lack_size}-"
                                        f"{current_file_size - 1}",
                                    )
                                ],
                            ]
                        )
                    else:
                        current_group.append((src_path, None))
                        groups.append(current_group)
                else:
                    groups[-1].extend(current_group)
                    groups.append([(src_path, None)])
                current_group, current_group_size = [], 0
            else:
                current_group.append((src_path, None))
                current_group_size += current_file_size
                if current_group_size >= block_size:
                    groups.append(current_group)
                    current_group, current_group_size = [], 0
        if current_group:
            groups.append(current_group)
        return groups

    async def concat(
        self,
        src_paths: T.List[PathLike],
        dst_path: PathLike,
        block_size: int = READER_BLOCK_SIZE,
    ) -> None:
        """Concatenate s3 files to one file.

        :param src_paths: Given source paths
        :param dst_path: Given destination path
        """
        client = await self._get_client()
        with raise_s3_error(dst_path):
            if block_size == 0:
                groups = [[(src_path, None)] for src_path in src_paths]
            else:
                groups = await self._group_src_paths_by_block(
                    src_paths, block_size=block_size
                )

            tasks = []
            async with MultiPartWriter(client, dst_path) as writer:
                for index, group in enumerate(groups, start=1):
                    if len(group) == 1:
                        tasks.append(
                            asyncio.create_task(
                                writer.upload_part_copy(index, group[0][0], group[0][1])
                            )
                        )
                    else:
                        tasks.append(
                            asyncio.create_task(
                                writer.upload_part_by_paths(index, group)
                            )
                        )
                await asyncio.gather(*tasks, return_exceptions=True)

    async def access(self, path: str, mode: Access = Access.READ) -> bool:
        """
        Test if path has access permission described by mode

        :param mode: access mode
        :returns: bool, if the bucket of s3_url has read/write access.
        """
        if mode not in (Access.READ, Access.WRITE):
            raise TypeError("Unsupported mode: {}".format(mode))

        bucket, key = parse_s3_path(path)  # only check bucket accessibility
        if not bucket:
            raise Exception("No available bucket")
        if not isinstance(mode, Access):
            raise TypeError(
                "Unsupported mode: {} -- Mode should use one of "
                "the enums belonging to:  {}".format(
                    mode, ", ".join([str(a) for a in Access])
                )
            )
        if mode not in (Access.READ, Access.WRITE):
            raise TypeError("Unsupported mode: {}".format(mode))
        try:
            if not await self.exists(path):
                return False
        except Exception as error:
            error = translate_s3_error(error, self.build_uri(path))
            if isinstance(error, S3PermissionError):
                return False
            raise error

        if mode == Access.READ:
            return True
        try:
            if not key:
                key = "test"
            elif key.endswith("/"):
                key = key[:-1]

            client = await self._get_client()
            response = await client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = response["UploadId"]
            await client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id
            )
            return True
        except Exception as error:
            error = translate_s3_error(error, self.build_uri(path))
            if isinstance(error, S3PermissionError):
                return False
            raise error

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


class MultiPartWriter(AioClosable):
    def __init__(self, client: "S3Client", path: PathLike) -> None:
        self._client = client
        self._multipart_upload_info = []

        bucket, key = parse_s3_path(path)
        self._bucket = bucket
        self._key = key
        self._upload_id = None

    async def upload_part(self, part_num: int, file_obj: io.BytesIO) -> None:
        response = await self._client.upload_part(
            Body=file_obj,
            UploadId=self._upload_id,
            PartNumber=part_num,
            Bucket=self._bucket,
            Key=self._key,
        )
        self._multipart_upload_info.append(
            {"PartNumber": part_num, "ETag": response["ETag"]}
        )

    async def upload_part_by_paths(
        self, part_num: int, paths: T.List[T.Tuple[PathLike, str]]
    ) -> None:
        file_obj = io.BytesIO()
        if not paths:
            await self.upload_part(part_num, file_obj)
            return

        max_workers = max(GLOBAL_MAX_WORKERS, 1)
        semaphore = asyncio.Semaphore(min(max_workers, len(paths)))

        async def get_object_bytes(
            client, bucket, key, range_str: T.Optional[str] = None
        ) -> bytes:
            """Fetch object bytes with optional range and concurrency control.

            :param client: S3 client to use.
            :param bucket: Bucket name.
            :param key: Object key.
            :param range_str: Optional range header string.
            :return: Object bytes.
            """
            async with semaphore:
                if range_str:
                    response = await client.get_object(
                        Bucket=bucket, Key=key, Range=range_str
                    )
                else:
                    response = await client.get_object(Bucket=bucket, Key=key)
                return await response["Body"].read()

        tasks: T.List[asyncio.Task[bytes]] = []
        for path, bytes_range in paths:
            bucket, key = parse_s3_path(path)
            tasks.append(
                asyncio.create_task(
                    get_object_bytes(self._client, bucket, key, bytes_range)
                )
            )

        try:
            chunks = await asyncio.gather(*tasks)
        except Exception:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for data in chunks:
            file_obj.write(data)
        file_obj.seek(0, os.SEEK_SET)
        await self.upload_part(part_num, file_obj)

    async def upload_part_copy(
        self, part_num: int, path: PathLike, copy_source_range: T.Optional[str] = None
    ) -> None:
        bucket, key = parse_s3_path(path)
        params = dict(
            UploadId=self._upload_id,
            PartNumber=part_num,
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=self._bucket,
            Key=self._key,
        )
        if copy_source_range:
            params["CopySourceRange"] = copy_source_range
        response = await self._client.upload_part_copy(**params)
        self._multipart_upload_info.append(
            {"PartNumber": part_num, "ETag": response["CopyPartResult"]["ETag"]}
        )

    async def close(self):
        if self._upload_id is None:
            return
        self._multipart_upload_info.sort(key=lambda t: t["PartNumber"])
        await self._client.complete_multipart_upload(
            UploadId=self._upload_id,
            Bucket=self._bucket,
            Key=self._key,
            MultipartUpload={"Parts": self._multipart_upload_info},
        )

    async def __aenter__(self):
        self._upload_id = (
            await self._client.create_multipart_upload(
                Bucket=self._bucket, Key=self._key
            )
        )["UploadId"]
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
