import typing as T
from io import BytesIO

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.lib.prefetch_reader.base_prefetch_reader import AioBasePrefetchReader
from aiomegfile.utils.retry.sftp import translate_sftp_error

if T.TYPE_CHECKING:
    from aiomegfile.filesystem.sftp import SftpFileSystem

__all__ = [
    "AioSftpPrefetchReader",
]


class AioSftpPrefetchReader(AioBasePrefetchReader):
    """Async prefetch reader for SFTP content powered by asyncssh.

    :param path: Path without protocol for the current SFTP filesystem.
    :param filesystem: SFTP filesystem instance used to open connections.
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
        filesystem: "SftpFileSystem",
        mode: str = "rb",
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: T.Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    ) -> None:
        """Initialize the SFTP prefetch reader.

        :param path: Path without protocol for the current SFTP filesystem.
        :param filesystem: SFTP filesystem instance.
        :param mode: File mode, either ``r``/``rt`` or ``rb``.
        :param encoding: Text encoding for text mode.
        :param errors: Error handling for text decoding.
        :param newline: Newline handling for text mode.
        :param block_size: Prefetch block size in bytes.
        :param max_buffer_size: Maximum prefetch buffer size.
        :param block_forward: Number of prefetched blocks ahead.
        :param max_retries: Maximum retry times for fetch operations.
        """
        self._path = path
        self._filesystem = filesystem

        self._connection = None
        self._client = None
        self._remote_path: T.Optional[str] = None

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
        """Enter async context and initialize SFTP connection.

        :return: Initialized reader.
        :rtype: AioSftpPrefetchReader
        """
        self._connection, self._client = await self._filesystem._open_client()
        try:
            self._remote_path = await self._filesystem._resolve_remote_path(
                self._client,
                self._path,
            )
        except Exception as error:
            await self._filesystem._close_client(self._connection, self._client)
            translated = translate_sftp_error(error, self.name)
            raise translated from error
        return await super().__aenter__()

    @property
    def name(self) -> str:
        """Return full SFTP URI of the target file.

        :return: Full SFTP URI.
        :rtype: str
        """
        return self._filesystem.build_uri(self._path)

    async def _get_content_size(self) -> int:
        """Get content size from remote SFTP metadata.

        :return: Content size in bytes.
        :rtype: int
        """
        if self._client is None or self._remote_path is None:
            raise RuntimeError("SFTP reader is not initialized")

        try:
            attrs = await self._client.stat(self._remote_path, follow_symlinks=True)
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error

        size = getattr(attrs, "size", 0)
        return int(size or 0)

    async def _fetch_response(
        self,
        start: T.Optional[int] = None,
        end: T.Optional[int] = None,
    ) -> dict:
        """Fetch response bytes from SFTP by range.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with ``Body`` and ``ContentLength``.
        :rtype: dict
        """
        if self._client is None or self._remote_path is None:
            raise RuntimeError("SFTP reader is not initialized")

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

        byte_count = end - start + 1

        file_obj = None
        try:
            async with self._client.open(
                self._remote_path,
                "rb",
                encoding=None,
            ) as file_obj:
                data = await file_obj.read(byte_count, offset=start)
                if data is None:
                    data = b""
                elif isinstance(data, str):
                    data = data.encode("utf-8")

                data = T.cast(bytes, data)
                return {
                    "Body": BytesIO(data),
                    "ContentLength": len(data),
                }
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error

    async def close(self) -> None:
        """Close reader and release SFTP connection resources."""
        await super().close()
        self._connection = None
        self._client = None
