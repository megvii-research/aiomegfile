import asyncio
import os
from collections import OrderedDict
from io import BytesIO, TextIOWrapper
from logging import getLogger as get_logger
from typing import NamedTuple, Optional

from aiomegfile.config import (
    DEFAULT_WRITER_BLOCK_AUTOSCALE,
    WRITER_BLOCK_SIZE,
    WRITER_MAX_BUFFER_SIZE,
)
from aiomegfile.errors import translate_s3_error
from aiomegfile.interfaces import AioWritable

_logger = get_logger(__name__)


class PartResult(NamedTuple):
    """Result of uploading a single part in multipart upload."""

    etag: str
    part_number: int
    content_size: int

    def asdict(self):
        """Convert to dictionary format for S3 API."""
        return {"PartNumber": self.part_number, "ETag": self.etag}


class AioS3BufferedWriter(AioWritable):
    """Async buffered writer for S3 using asyncio tasks.

    This class provides async write operations to S3 with automatic multipart
    upload handling. It uses asyncio tasks for concurrent uploads while
    respecting the max_buffer_size limit.

    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :param s3_client: Async S3 client from aiobotocore.
    :param mode: File mode, either 'w' (text) or 'wb' (binary).
    :param encoding: Text encoding for 'w' mode, defaults to 'utf-8'.
    :param errors: Error handling for encoding, defaults to 'strict'.
    :param newline: Newline handling for text mode.
    :param block_size: Size of each upload part in bytes.
    :param block_autoscale: Whether to auto-scale block size based on part number.
    :param max_buffer_size: Maximum total buffer size for pending uploads.
    """

    # Multi-upload part size must be between 5 MiB and 5 GiB.
    # There is no minimum size limit on the last part of your multipart upload.
    MIN_BLOCK_SIZE = 8 * 2**20

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        filesystem,
        mode: str = "wb",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        block_size: int = WRITER_BLOCK_SIZE,
        block_autoscale: bool = DEFAULT_WRITER_BLOCK_AUTOSCALE,
        max_buffer_size: int = WRITER_MAX_BUFFER_SIZE,
    ):
        # Set basic attributes first for __repr__ in case of error
        self._bucket = bucket
        self._key = key
        self._mode = mode

        if mode not in ("w", "wb"):
            raise ValueError(f"Invalid mode: {mode!r}, must be 'w' or 'wb'")

        self._client = None
        self._filesystem = filesystem

        # Text mode settings
        self._is_text_mode = "b" not in mode
        self._encoding = encoding or "utf-8" if self._is_text_mode else None

        if errors not in (None, "strict", "ignore", "replace", "backslashreplace"):
            raise ValueError(
                f"Invalid errors value: {errors!r}, must be "
                "'strict', 'ignore', 'replace', or 'backslashreplace'"
            )
        self._errors = errors or "backslashreplace" if self._is_text_mode else None

        if newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError(
                f"Invalid newline value: {newline!r}, must be "
                "None, '', '\\n', '\\r', or '\\r\\n'"
            )
        self._newline = newline

        # user maybe put block_size with 'numpy.uint64' type
        self._base_block_size = int(block_size)
        self._block_autoscale = block_autoscale

        self._max_buffer_size = max_buffer_size
        self._total_buffer_size = 0
        self._offset = 0
        self._buffer = BytesIO()
        if self._is_text_mode:
            self._writer = TextIOWrapper(
                self._buffer,
                encoding=self._encoding,
                errors=self._errors,
                newline=self._newline,
                write_through=True,
            )
        else:
            self._writer = self._buffer

        self._tasks_result: OrderedDict[int, dict] = OrderedDict()
        self._uploading_tasks: set[asyncio.Task[PartResult]] = set()

        self._part_number = 0
        self.__upload_id: Optional[str] = None
        self.__upload_id_lock = asyncio.Lock()

        _logger.debug("open file: %r, mode: %s" % (self.name, self.mode))

    async def __aenter__(self):
        self._client = await self._filesystem._get_client()
        return self

    @property
    def name(self) -> str:
        """Return the path of the file."""
        return self._filesystem.build_uri(f"{self._bucket}/{self._key}")

    @property
    def mode(self) -> str:
        """Return the file mode."""
        return self._mode

    async def tell(self) -> int:
        """Return the current stream position."""
        return self._offset

    @property
    def _block_size(self) -> int:
        """Calculate the current block size based on part number."""
        if self._block_autoscale:
            if self._part_number < 10:
                return self._base_block_size
            elif self._part_number < 100:
                return min(self._base_block_size * 2, self._max_buffer_size)
            elif self._part_number < 1000:
                return min(self._base_block_size * 4, self._max_buffer_size)
            elif self._part_number < 10000:
                return min(self._base_block_size * 8, self._max_buffer_size)
            return min(self._base_block_size * 16, self._max_buffer_size)  # unreachable
        return self._base_block_size

    @property
    def _is_multipart(self) -> bool:
        """Return True if multipart upload has been initiated."""
        return len(self._tasks_result) > 0 or len(self._uploading_tasks) > 0

    async def _get_upload_id(self) -> str:
        """Get or create the multipart upload ID.

        :return: The upload ID for the multipart upload.
        """
        if self.__upload_id is None:
            async with self.__upload_id_lock:
                if self.__upload_id is None:
                    try:
                        resp = await self._client.create_multipart_upload(
                            Bucket=self._bucket, Key=self._key
                        )
                        self.__upload_id = resp["UploadId"]
                    except Exception as e:
                        raise translate_s3_error(e, self.name) from e
        return self.__upload_id

    async def _collect_multipart_upload(self) -> dict:
        """Collect all completed upload parts.

        :return: Dictionary with Parts list for complete_multipart_upload.
        """
        for task in self._uploading_tasks:
            result = await task
            self._total_buffer_size -= result.content_size
            self._tasks_result[result.part_number] = result.asdict()
        self._uploading_tasks = set()
        return {"Parts": [result for _, result in sorted(self._tasks_result.items())]}

    async def _upload_buffer(self, part_number: int, content: bytes) -> PartResult:
        """Upload a single buffer to S3.

        :param part_number: The part number for this upload.
        :param content: The content to upload.
        :return: PartResult with upload details.
        """
        try:
            upload_id = await self._get_upload_id()
            resp = await self._client.upload_part(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=content,
            )
            return PartResult(
                resp["ETag"],
                part_number,
                len(content),
            )
        except Exception as e:
            raise translate_s3_error(e, self.name) from e

    async def _submit_upload_buffer(self, part_number: int, content: bytes) -> None:
        """Submit a buffer for upload as an async task.

        :param part_number: The part number for this upload.
        :param content: The content to upload.
        """
        task = asyncio.create_task(self._upload_buffer(part_number, content))
        self._uploading_tasks.add(task)
        self._total_buffer_size += len(content)

        # Wait for tasks to complete if buffer is full
        while (
            self._uploading_tasks and self._total_buffer_size >= self._max_buffer_size
        ):
            done, pending = await asyncio.wait(
                self._uploading_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_task in done:
                result = await completed_task
                self._total_buffer_size -= result.content_size
                self._tasks_result[result.part_number] = result.asdict()
            self._uploading_tasks = pending

    async def _submit_upload_content(self, content: bytes) -> None:
        """Split content into parts and submit for upload.

        :param content: The content to split and upload.
        """
        # s3 part needs at least 5MB,
        # so we need to divide content into equal-size parts,
        # and give last part more size.
        # e.g. 257MB can be divided into 2 parts, 128MB and 129MB
        block_size = self._block_size
        while len(content) - block_size > self.MIN_BLOCK_SIZE:
            self._part_number += 1
            current_content, content = (
                content[:block_size],
                content[block_size:],
            )
            await self._submit_upload_buffer(self._part_number, current_content)
            block_size = self._block_size

        if content:
            self._part_number += 1
            await self._submit_upload_buffer(self._part_number, content)

    async def _submit_futures(self) -> None:
        """Submit the current buffer for upload."""
        content = self._buffer.getvalue()
        if len(content) == 0:
            return
        self._buffer.seek(0, os.SEEK_SET)
        self._buffer.truncate()
        await self._submit_upload_content(content)

    async def write(self, data: bytes | str) -> int:
        """Write data to the buffer.

        :param data: Data to write (bytes for 'wb' mode, str for 'w' mode).
        :raises IOError: If the file is already closed.
        :raises TypeError: If data type doesn't match the mode.
        :return: Number of bytes/characters written.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if self._is_text_mode:
            if not isinstance(data, str):
                raise TypeError(
                    f"write() argument must be str, not {type(data).__name__}"
                )
        else:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError(
                    f"a bytes-like object is required, not '{type(data).__name__}'"
                )

        self._writer.write(data)
        if self._buffer.tell() >= self._block_size:
            await self._submit_futures()
        self._offset += len(data)
        return len(data)

    async def close(self) -> None:
        """Close the writer and complete the upload."""
        _logger.debug("close file: %r" % self.name)

        if not self._is_multipart:
            try:
                await self._client.put_object(
                    Bucket=self._bucket, Key=self._key, Body=self._buffer.getvalue()
                )
            except Exception as e:
                raise translate_s3_error(e, self.name) from e
            return

        await self._submit_futures()

        try:
            upload_id = await self._get_upload_id()
            multipart_upload = await self._collect_multipart_upload()
            await self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                MultipartUpload=multipart_upload,  # pyre-ignore[6]
                UploadId=upload_id,
            )
        except Exception as e:
            raise translate_s3_error(e, self.name) from e
