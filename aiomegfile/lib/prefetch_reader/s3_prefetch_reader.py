import asyncio
from io import BytesIO
from typing import Optional

from aiomegfile.config import (
    READER_BLOCK_SIZE,
    READER_LAZY_PREFETCH,
    READER_MAX_BUFFER_SIZE,
    S3_MAX_RETRY_TIMES,
)
from aiomegfile.errors import (
    S3FileChangedError,
    S3InvalidRangeError,
    aioretry,
    raise_s3_error,
)
from aiomegfile.errors.s3 import s3_should_retry
from aiomegfile.lib.prefetch_reader.base_prefetch_reader import (
    AioBasePrefetchReader,
)

__all__ = [
    "AioS3PrefetchReader",
]


class AioS3PrefetchReader(AioBasePrefetchReader):
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
        filesystem,
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
        self._filesystem = filesystem
        self._client = None
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

    async def __aenter__(self):
        self._client = await self._filesystem._get_client()
        return await super().__aenter__()

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
        return self._filesystem.build_uri(f"{self._bucket}/{self._key}")

    async def _fetch_response(
        self, start: Optional[int] = None, end: Optional[int] = None
    ) -> dict:
        """Fetch response from S3.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with 'Body' key.
        """

        @aioretry(should_retry=s3_should_retry, max_retries=self._max_retries)
        async def fetch_response() -> dict:
            if start is None or end is None:
                with raise_s3_error(self.name):
                    # The client has a retry mechanism;
                    # raise_s3_error prevents nested retries.
                    response = await self._client.get_object(
                        Bucket=self._bucket, Key=self._key
                    )
                body_bytes = await response["Body"].read()
                response["Body"] = BytesIO(body_bytes)
                return response

            range_str = f"bytes={start}-{end}"
            with raise_s3_error(self.name):
                # The client has a retry mechanism;
                # raise_s3_error prevents nested retries.
                response = await self._client.get_object(
                    Bucket=self._bucket, Key=self._key, Range=range_str
                )
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
