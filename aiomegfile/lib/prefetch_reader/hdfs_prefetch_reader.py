"""Async prefetch reader for HDFS content."""

from __future__ import annotations

import asyncio
import typing as T
from io import BytesIO

from aiomegfile.config import (
    HDFS_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.errors.hdfs import HdfsIsADirectoryError, translate_hdfs_error
from aiomegfile.lib.prefetch_reader.base_prefetch_reader import AioBasePrefetchReader

if T.TYPE_CHECKING:
    from aiomegfile.filesystem.hdfs import HdfsFileSystem

__all__ = [
    "AioHdfsPrefetchReader",
]


class AioHdfsPrefetchReader(AioBasePrefetchReader):
    """Async prefetch reader for HDFS content backed by the sync client.

    :param path: HDFS path without protocol.
    :param filesystem: HDFS filesystem instance.
    :param mode: File mode, either ``r``/``rt`` or ``rb``.
    :param encoding: Text encoding for text mode.
    :param errors: Error handling for text decoding.
    :param newline: Newline handling for text mode.
    :param block_size: Prefetch block size in bytes.
    :param max_buffer_size: Maximum prefetch buffer size.
    :param block_forward: Number of prefetched blocks ahead.
    :param max_retries: Maximum retry times for fetch operations.
    """

    def __init__(
        self,
        path: str,
        *,
        filesystem: "HdfsFileSystem",
        mode: str = "rb",
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: T.Optional[int] = None,
        max_retries: int = HDFS_MAX_RETRY_TIMES,
    ) -> None:
        """Initialize the HDFS prefetch reader.

        :param path: HDFS path without protocol.
        :param filesystem: HDFS filesystem instance.
        :param mode: File mode.
        :param encoding: Text encoding for text mode.
        :param errors: Text decoding error handling strategy.
        :param newline: Newline handling for text mode.
        :param block_size: Prefetch block size in bytes.
        :param max_buffer_size: Maximum prefetch buffer size.
        :param block_forward: Number of prefetched blocks ahead.
        :param max_retries: Maximum retry times for fetch operations.
        """
        self._path = path
        self._filesystem = filesystem

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

    @property
    def name(self) -> str:
        """Return full HDFS URI of the target file.

        :return: Full HDFS URI.
        :rtype: str
        """
        return self._filesystem.build_uri(self._path)

    async def _get_content_size(self) -> int:
        """Get file size from HDFS metadata.

        :return: Content size in bytes.
        :rtype: int
        """

        def _status() -> T.Mapping[str, T.Any]:
            """Read file status from HDFS synchronously.

            :return: HDFS status mapping.
            :rtype: typing.Mapping[str, typing.Any]
            """
            return self._filesystem._client.status(self._path)

        try:
            stat_data = await asyncio.to_thread(_status)
        except Exception as error:
            translated = translate_hdfs_error(error, self.name)
            raise translated from error

        if str(stat_data.get("type", "")).upper() == "DIRECTORY":
            raise HdfsIsADirectoryError(f"Is a directory: {self.name!r}")
        return int(stat_data.get("length", 0) or 0)

    async def _fetch_response(
        self,
        start: T.Optional[int] = None,
        end: T.Optional[int] = None,
    ) -> dict:
        """Fetch response bytes from HDFS by range.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with ``Body`` and ``ContentLength``.
        :rtype: dict
        """
        if self._content_size <= 0:
            return {
                "Body": BytesIO(b""),
                "ContentLength": 0,
            }

        if start is None:
            start = 0
        if end is None:
            end = self._content_size - 1

        if end < start:
            return {
                "Body": BytesIO(b""),
                "ContentLength": 0,
            }

        length = end - start + 1

        def _read_range() -> bytes:
            """Read a byte range from HDFS synchronously.

            :return: Retrieved bytes.
            :rtype: bytes
            """
            with self._filesystem._client.read(
                self._path,
                offset=start,
                length=length,
            ) as file_obj:
                data = file_obj.read()
                if isinstance(data, str):
                    return data.encode("utf-8")
                return bytes(data or b"")

        try:
            data = await asyncio.to_thread(_read_range)
        except Exception as error:
            translated = translate_hdfs_error(error, self.name)
            raise translated from error

        return {
            "Body": BytesIO(data),
            "ContentLength": len(data),
        }
