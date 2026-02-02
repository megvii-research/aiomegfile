import asyncio
from io import BytesIO
from typing import TYPE_CHECKING, Optional

from aiomegfile.config import (
    READER_BLOCK_SIZE,
    READER_LAZY_PREFETCH,
    READER_MAX_BUFFER_SIZE,
    S3_MAX_RETRY_TIMES,
)
from aiomegfile.errors import (
    S3FileChangedError,
    S3InvalidRangeError,
    async_retry,
    raise_s3_error,
    s3_should_retry,
)
from aiomegfile.lib.base_prefetch_reader import (
    AsyncBasePrefetchReader,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client  # pyre-ignore[21]

__all__ = [
    "AsyncS3PrefetchReader",
]


class AsyncS3PrefetchReader(AsyncBasePrefetchReader):
    """Async reader to fast read S3 content.

    This will divide the file content into equal parts of block_size size,
    and will use LRU to cache at most blocks in max_buffer_size memory.

    open(), seek() and read() will trigger prefetch read.
    The prefetch will cached block_forward blocks of data from offset position
    (the position after reading if the called function is read).

    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :param s3_client: Async S3 client from aiobotocore.
    :param mode: File mode, either 'r' (text) or 'rb' (binary).
    :param encoding: Text encoding for 'r' mode, defaults to 'utf-8'.
    :param errors: Error handling for encoding, defaults to 'strict'.
    :param newline: Newline handling for text mode.
    :param block_size: Size of each prefetch block in bytes.
    :param max_buffer_size: Maximum total buffer size for prefetch.
    :param block_forward: Number of blocks to prefetch ahead.
    :param max_retries: Maximum retry times for fetch operations.
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        s3_client: "S3Client",
        mode: str = "rb",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: Optional[int] = None,
        max_retries: int = S3_MAX_RETRY_TIMES,
    ):
        self._bucket = bucket
        self._key = key
        self._client = s3_client
        self._content_etag = None

        super().__init__(
            mode=mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
            block_size=block_size,
            max_buffer_size=max_buffer_size,
            block_forward=block_forward,
            max_retries=max_retries,
        )

    async def _get_content_size(self) -> int:
        """Get the content size of the S3 object.

        :return: Content size in bytes.
        """
        if self._block_capacity <= 0 or READER_LAZY_PREFETCH:
            response = await self._client.head_object(
                Bucket=self._bucket, Key=self._key
            )
            self._content_etag = response.get("ETag")
            return int(response["ContentLength"])

        try:
            start, end = 0, self._block_size - 1
            first_index_response = await self._fetch_response(start=start, end=end)
            if "ContentRange" in first_index_response:
                content_size = int(first_index_response["ContentRange"].split("/")[-1])
            else:
                # usually when read a file only have one block
                content_size = int(first_index_response["ContentLength"])
        except S3InvalidRangeError:
            # usually when read a empty file
            # can use minio test empty file: https://hub.docker.com/r/minio/minio
            first_index_response = await self._fetch_response()
            content_size = int(first_index_response["ContentLength"])

        # Create a completed task for the first block
        first_task = asyncio.create_task(
            self._return_buffer(first_index_response["Body"])
        )
        self._insert_task(index=0, task=first_task)
        self._content_etag = first_index_response.get("ETag")
        return content_size

    async def _return_buffer(self, buffer: BytesIO) -> BytesIO:
        """Helper to return a buffer directly (for completed tasks).

        :param buffer: Buffer to return.
        :return: The same buffer.
        """
        return buffer

    @property
    def name(self) -> str:
        """Return the path of the file."""
        # TODO: support URI with alias
        return f"{self._bucket}/{self._key}"

    async def _fetch_response(
        self, start: Optional[int] = None, end: Optional[int] = None
    ) -> dict:
        """Fetch response from S3.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with 'Body' key.
        """

        @async_retry(should_retry=s3_should_retry, max_retries=self._max_retries)
        async def fetch_response() -> dict:
            if start is None or end is None:
                response = await self._client.get_object(
                    Bucket=self._bucket, Key=self._key
                )
                # Read the streaming body into BytesIO
                body_bytes = await response["Body"].read()
                response["Body"] = BytesIO(body_bytes)
                return response

            range_str = f"bytes={start}-{end}"
            response = await self._client.get_object(
                Bucket=self._bucket, Key=self._key, Range=range_str
            )
            # Read the streaming body into BytesIO
            body_bytes = await response["Body"].read()
            response["Body"] = BytesIO(body_bytes)
            return response

        with raise_s3_error(self.name):
            return await fetch_response()

    async def _fetch_buffer(self, index: int) -> BytesIO:
        """Fetch a single block buffer from S3.

        :param index: Block index to fetch.
        :return: BytesIO buffer.
        """
        start = index * self._block_size
        end = min((index + 1) * self._block_size - 1, self._content_size - 1)
        response = await self._fetch_response(start=start, end=end)
        etag = response.get("ETag", None)
        if self._content_etag and etag and etag != self._content_etag:
            raise S3FileChangedError(
                "File changed: %r, etag before: %s, after: %s"
                % (self.name, self._content_etag, etag)
            )

        return response["Body"]
