import os
import re
import typing as T
from functools import cached_property, lru_cache
from logging import getLogger as get_logger

import botocore
from aiobotocore.session import AioSession, get_session

from aiomegfile.config import S3_CLIENT_CACHE_MODE
from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3ConfigError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3NameTooLongError,
    S3NotADirectoryError,
    S3NotALinkError,
    S3PermissionError,
    S3UnknownError,
    translate_s3_error,
)
from aiomegfile.interfaces import BaseFileSystem, StatResult
from aiomegfile.lib.url import get_url_scheme
from aiomegfile.smart_path import SmartPath, fspath


__all__ = [
    "S3FileSystem",
    "parse_s3_url",
    "get_endpoint_url",
    "get_s3_session",
    "is_s3",
]

_logger = get_logger(__name__)
endpoint_url = "https://s3.amazonaws.com"
max_keys = 1000


def get_s3_session(profile_name: T.Optional[str] = None) -> AioSession:
    """Get S3 session

    :returns: S3 session
    """
    return get_session()


def get_scoped_config(profile_name: T.Optional[str] = None) -> T.Dict:
    try:
        session = get_s3_session(profile_name=profile_name)
        # AioSession wraps botocore session
        if hasattr(session, "_session"):
            return session._session.get_scoped_config()  # type: ignore
        return {}
    except Exception:
        return {}


@lru_cache()
def warning_endpoint_url(key: str, endpoint_url: str):
    _logger.info("using %s: %s" % (key, endpoint_url))


def get_endpoint_url(profile_name: T.Optional[str] = None) -> str:
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
            warning_endpoint_url(environ_key, environ_endpoint_url)
            return environ_endpoint_url
    config = get_scoped_config(profile_name=profile_name)
    config_endpoint_url = config.get("s3", {}).get("endpoint_url")
    config_endpoint_url = config_endpoint_url or config.get("endpoint_url")
    if config_endpoint_url:
        warning_endpoint_url("~/.aws/config or ~/.aws/credentials", config_endpoint_url)
        return config_endpoint_url
    return endpoint_url


def is_s3(path: T.Union[str, "SmartPath", os.PathLike]) -> bool:
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


def _become_prefix(prefix: str) -> str:
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def parse_s3_url(s3_url: T.Union[str, "SmartPath", os.PathLike]) -> T.Tuple[str, str]:
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


def _make_stat(content: T.Dict[str, T.Any]) -> StatResult:
    return StatResult(
        islnk=content.get("islnk", False),
        size=content["Size"],
        mtime=content["LastModified"].timestamp(),
        extra=content,
    )


class S3FileSystem(BaseFileSystem):
    """Async S3 filesystem operations based on aiobotocore."""

    protocol = "s3"

    def __init__(self, s3_path: str, profile_name: T.Optional[str] = None):
        # Parse profile from path like s3+profile://bucket/key
        protocol = get_url_scheme(s3_path)
        self._protocol_with_profile = self.protocol
        self._profile_name = profile_name
        if protocol.startswith("s3+"):
            self._protocol_with_profile = protocol
            self._profile_name = protocol[3:]
            self._s3_path = f"s3://{s3_path[len(protocol) + 3:]}"
        elif not protocol:
            self._s3_path = f"s3://{s3_path.lstrip('/')}"
        else:
            self._s3_path = s3_path

        # Extract path_without_protocol
        if self._s3_path.startswith("s3://"):
            path_without_protocol = self._s3_path[5:]
        else:
            path_without_protocol = self._s3_path

        super().__init__(path_without_protocol, self._profile_name)

    @cached_property
    def path_with_protocol(self) -> str:
        """Return path with protocol, like s3://bucket/key"""
        return f"{self.root}{self.path_without_protocol}"

    @cached_property
    def root(self) -> str:
        """Return root of the path, like s3:// or s3+profile://"""
        return f"{self._protocol_with_profile}://"

    @property
    def name(self) -> str:
        """Return the final component of the path."""
        path = self.path_without_protocol.rstrip("/")
        if "/" in path:
            return path.rsplit("/", 1)[1]
        return path

    def _get_client(self):
        """Get an async S3 client context manager.

        Usage:
            async with self._get_client() as client:
                resp = await client.list_objects_v2(...)
        """
        session = get_s3_session(profile_name=self._profile_name)
        return session.create_client(
            "s3",
            endpoint_url=get_endpoint_url(profile_name=self._profile_name),
        )

    async def _list_objects_recursive(
        self,
        client,
        bucket: str,
        prefix: str,
        delimiter: str = "",
    ) -> T.AsyncIterator[T.Dict]:
        """Async iterator to list all objects with pagination."""
        resp = await client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=max_keys
        )

        while True:
            yield resp

            if not resp.get("IsTruncated"):
                break

            resp = await client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                Delimiter=delimiter,
                ContinuationToken=resp["NextContinuationToken"],
                MaxKeys=max_keys,
            )

    async def _s3_get_metadata(self) -> T.Dict:
        """Get object metadata."""
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return {}
        if not key or key.endswith("/"):
            return {}
        try:
            async with self._get_client() as client:
                resp = await client.head_object(Bucket=bucket, Key=key)
            return {key.lower(): value for key, value in resp.get("Metadata", {}).items()}
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return {}

    async def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if an s3 url is directory.

        :param followlinks: whether followlinks is True or False, result is the same.
            Because s3 symlink not support dir.
        :returns: True if path is s3 directory, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key
        prefix = _become_prefix(key)
        try:
            async with self._get_client() as client:
                resp = await client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
                )
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
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

    async def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if an s3_url is file.

        :returns: True if path is s3 file, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket or not key or key.endswith("/"):
            # s3://, s3:///key, s3://bucket, s3://bucket/prefix/
            return False

        if followlinks:
            try:
                link_target = await self.readlink()
                # Create a new S3FileSystem for the link target
                target_fs = S3FileSystem(link_target, self._profile_name)
                return await target_fs.is_file()
            except S3NotALinkError:
                pass

        try:
            async with self._get_client() as client:
                await client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False
        return True

    async def exists(self, followlinks: bool = False) -> bool:
        """
        Test if s3_url exists.

        :returns: True if s3_url exists, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key

        return await self.is_file(followlinks) or await self.is_dir()

    async def _get_dir_stat(self) -> StatResult:
        """
        Return StatResult of given s3_url directory.
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        prefix = _become_prefix(key)
        count, size, mtime = 0, 0, 0.0

        async with self._get_client() as client:
            async for resp in self._list_objects_recursive(client, bucket, prefix):
                for content in resp.get("Contents", []):
                    count += 1
                    size += content["Size"]
                    last_modified = content["LastModified"].timestamp()
                    if mtime < last_modified:
                        mtime = last_modified

        if count == 0:
            raise S3FileNotFoundError(
                "No such file or directory: %r" % self.path_with_protocol
            )

        return StatResult(size=size, mtime=mtime, isdir=True)

    async def stat(self, follow_symlinks: bool = True) -> StatResult:
        """
        Get StatResult of s3_url file, including file size and mtime.

        :returns: StatResult
        :raises: S3FileNotFoundError, S3BucketNotFoundError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )

        if not await self.is_file():
            return await self._get_dir_stat()

        async with self._get_client() as client:
            content = await client.head_object(Bucket=bucket, Key=key)
            islnk = False
            if "Metadata" in content:
                metadata = {
                    k.lower(): v for k, v in content["Metadata"].items()
                }
                if "symlink_to" in metadata:
                    islnk = True
                    if follow_symlinks:
                        s3_url = metadata["symlink_to"]
                        bucket, key = parse_s3_url(s3_url)
                        content = await client.head_object(Bucket=bucket, Key=key)

            return StatResult(
                islnk=islnk,
                size=content["ContentLength"],
                mtime=content["LastModified"].timestamp(),
                extra=content,
            )

    async def remove(self, missing_ok: bool = False) -> None:
        """
        Remove the file or directory on s3.

        :param missing_ok: if False and target file/directory not exists,
            raise S3FileNotFoundError
        :raises: S3PermissionError, S3FileNotFoundError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            if not key:
                raise S3IsADirectoryError("Cannot remove s3://")
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not key:
            raise S3IsADirectoryError("Cannot remove bucket: %r" % self.path_with_protocol)

        async with self._get_client() as client:
            if await self.is_file():
                try:
                    await client.delete_object(Bucket=bucket, Key=key)
                except Exception as error:
                    error = translate_s3_error(error, self.path_with_protocol)
                    if isinstance(error, S3FileNotFoundError):
                        if not missing_ok:
                            raise
                    else:
                        raise error
                return

            # Remove directory (all files with prefix)
            prefix = _become_prefix(key)
            total_count = 0
            async for resp in self._list_objects_recursive(client, bucket, prefix):
                if "Contents" in resp:
                    keys: T.List[T.Dict[str, str]] = [
                        {"Key": content["Key"]} for content in resp["Contents"]
                    ]
                    total_count += len(keys)
                    if keys:
                        await client.delete_objects(
                            Bucket=bucket, Delete={"Objects": keys}  # type: ignore
                        )

            if total_count == 0 and not missing_ok:
                raise S3FileNotFoundError(
                    "No such file or directory: %r" % self.path_with_protocol
                )

    async def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """
        Create an s3 directory.
        This function tests if the target bucket has WRITE access.

        :param mode: mode is ignored, only for compatibility
        :param parents: parents is ignored, only for compatibility
        :param exist_ok: If False and target directory exists, raise S3FileExistsError
        :raises: S3BucketNotFoundError, S3FileExistsError
        """
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )

        # Check if bucket exists
        try:
            async with self._get_client() as client:
                await client.head_bucket(Bucket=bucket)
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, S3FileNotFoundError):
                raise S3BucketNotFoundError(
                    "No such bucket: %r" % self.path_with_protocol
                )
            if isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            # S3PermissionError is ok, bucket exists but no head_bucket permission

        if exist_ok:
            return

        if await self.exists():
            raise S3FileExistsError("File exists: %r" % self.path_with_protocol)

    async def walk(
        self, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """
        Iteratively traverse the given s3 directory, in top-bottom order.

        :param followlinks: whether followlinks is True or False, result is the same.
        :raises: S3BucketNotFoundError
        :returns: A 3-tuple generator (root, dirs, files)
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError("Cannot walk s3://")

        async with self._get_client() as client:
            stack = [key]
            while stack:
                current = _become_prefix(stack.pop())
                dirs: T.List[str] = []
                files: T.List[str] = []

                async for resp in self._list_objects_recursive(
                    client, bucket, current, "/"
                ):
                    for common_prefix in resp.get("CommonPrefixes", []):
                        dirs.append(common_prefix["Prefix"][:-1])
                    for content in resp.get("Contents", []):
                        if not content["Key"].endswith("/"):
                            files.append(content["Key"])

                dirs = sorted(dirs)
                stack.extend(reversed(dirs))

                if current:
                    root = f"{self.root}{bucket}/{current}"[:-1]
                else:
                    root = f"{self.root}{bucket}"
                dir_names = [path[len(current):] for path in dirs]
                file_names = sorted(path[len(current):] for path in files)

                if file_names or dir_names or not current:
                    yield root, dir_names, file_names

    async def iglob(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> T.AsyncIterator[str]:
        """
        Return an iterator of paths matching the glob pattern.

        :param pattern: Glob pattern
        :param recursive: If False, ** will not search directory recursively
        :param missing_ok: If False and no match, raise FileNotFoundError
        :returns: An iterator of matching paths
        """
        import fnmatch

        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError("Cannot glob s3://")

        # Combine current path with pattern
        if key:
            full_pattern = f"{key.rstrip('/')}/{pattern}"
        else:
            full_pattern = pattern

        # Find the prefix without wildcards
        prefix_parts = []
        for part in full_pattern.split("/"):
            if "*" in part or "?" in part or "[" in part:
                break
            prefix_parts.append(part)
        prefix = "/".join(prefix_parts)
        if prefix:
            prefix = _become_prefix(prefix)

        found = False
        async with self._get_client() as client:
            async for resp in self._list_objects_recursive(client, bucket, prefix):
                for content in resp.get("Contents", []):
                    file_key = content["Key"]
                    if fnmatch.fnmatch(file_key, full_pattern):
                        found = True
                        yield f"{self.root}{bucket}/{file_key}"

        if not found and not missing_ok:
            raise S3FileNotFoundError(
                "No match any file: %r" % f"{self.path_with_protocol}/{pattern}"
            )

    async def iterdir(self) -> T.AsyncIterator[str]:
        """
        Get all contents of given s3_url.

        :returns: All contents have prefix of s3_url
        :raises: S3FileNotFoundError, S3NotADirectoryError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket and key:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )

        if await self.is_file():
            raise S3NotADirectoryError("Not a directory: %r" % self.path_with_protocol)

        prefix = _become_prefix(key)

        async with self._get_client() as client:
            if not bucket and not key:
                # List buckets
                response = await client.list_buckets()
                for bucket_info in response.get("Buckets", []):
                    bucket_name = bucket_info.get("Name", "")
                    if bucket_name:
                        yield f"{self.root}{bucket_name}"
                return

            async for resp in self._list_objects_recursive(
                client, bucket, prefix, "/"
            ):
                for common_prefix in resp.get("CommonPrefixes", []):
                    dir_name = common_prefix["Prefix"][len(prefix):-1]
                    yield f"{self.path_with_protocol.rstrip('/')}/{dir_name}"
                for content in resp.get("Contents", []):
                    if content["Key"].endswith("/"):
                        continue
                    file_name = content["Key"][len(prefix):]
                    yield f"{self.path_with_protocol.rstrip('/')}/{file_name}"

    async def symlink(self, dst_path: str) -> None:
        """
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Destination path
        :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError
        """
        if len(fspath(self._s3_path).encode()) > 1024:
            raise S3NameTooLongError("File name too long: %r" % dst_path)

        src_bucket, src_key = parse_s3_url(self.path_with_protocol)
        dst_bucket, dst_key = parse_s3_url(dst_path)

        if not src_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % self.path_with_protocol)
        if not dst_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % dst_path)
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % dst_path)

        # Get the actual target path (resolve existing symlinks)
        src_path = self._s3_path
        try:
            src_path = await self.readlink()
        except S3NotALinkError:
            pass

        async with self._get_client() as client:
            await client.put_object(
                Bucket=dst_bucket, Key=dst_key, Metadata={"symlink_to": src_path}
            )

    async def readlink(self) -> str:
        """
        Return the path to which the symbolic link points.

        :returns: The path the symlink points to
        :raises: S3BucketNotFoundError, S3IsADirectoryError, S3NotALinkError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % self.path_with_protocol)

        metadata = await self._s3_get_metadata()

        if "symlink_to" not in metadata:
            raise S3NotALinkError("Not a link: %r" % self.path_with_protocol)

        return metadata["symlink_to"]

    async def is_symlink(self) -> bool:
        """
        Test whether a path is link.

        :returns: True if a path is link, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return False
        if not key or key.endswith("/"):
            return False

        metadata = await self._s3_get_metadata()
        return "symlink_to" in metadata

    async def rename(self, dst_path: str, overwrite: bool = True) -> str:
        """
        Move s3 file path from src_url to dst_url.

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :returns: The destination path
        """
        dst_bucket, dst_key = parse_s3_url(dst_path)
        src_bucket, src_key = parse_s3_url(self.path_with_protocol)

        if not src_bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not dst_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % dst_path)

        # Check if destination exists
        dst_fs = S3FileSystem(dst_path, self._profile_name)
        if not overwrite and await dst_fs.is_file():
            return dst_path

        # Copy then delete
        async with self._get_client() as client:
            if await self.is_file():
                # Copy single file
                await client.copy_object(
                    CopySource={"Bucket": src_bucket, "Key": src_key},
                    Bucket=dst_bucket,
                    Key=dst_key,
                )
                await client.delete_object(Bucket=src_bucket, Key=src_key)
            else:
                # Copy directory
                src_prefix = _become_prefix(src_key)
                async for resp in self._list_objects_recursive(
                    client, src_bucket, src_prefix
                ):
                    for content in resp.get("Contents", []):
                        old_key = content["Key"]
                        new_key = dst_key + old_key[len(src_key):]
                        await client.copy_object(
                            CopySource={"Bucket": src_bucket, "Key": old_key},
                            Bucket=dst_bucket,
                            Key=new_key,
                        )
                        await client.delete_object(Bucket=src_bucket, Key=old_key)

        return dst_path

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd: bool = True,
    ) -> T.AsyncContextManager:
        """
        Open the file with mode.

        Note: This returns an async context manager that should be used with
        `async with`.
        """
        # TODO: Implement async file handlers
        raise NotImplementedError("Async file open is not yet implemented")

    async def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """S3 does not support chmod."""
        raise NotImplementedError("'chmod' is unsupported on S3")

    async def absolute(self) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        """
        return self.path_with_protocol
