import asyncio
import codecs
import os
import typing as T
from abc import ABC, abstractmethod
from collections import OrderedDict
from io import BytesIO, StringIO
from logging import getLogger as get_logger
from math import ceil

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.interfaces import AsyncReadable, AsyncSeekable

_logger = get_logger(__name__)

# Newline byte value
NEWLINE = b"\n"


class SeekRecord:
    def __init__(self, seek_index: int):
        self.seek_index = seek_index
        self.seek_count = 0
        self.read_count = 0


# A tricky aspect of TextIOWrapper is that its offset-related methods work with
# bytes (e.g., seek(1) moves 1 byte), while its read methods work with characters
# (e.g., read(1) reads 1 char). To ensure consistency,
# the following Reader must observe this rule.
class AsyncBasePrefetchReader(AsyncReadable[T.AnyStr], AsyncSeekable[T.AnyStr], ABC):
    """Base class for async prefetch readers.

    This class provides async read operations with automatic prefetching
    using asyncio tasks instead of threads.

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
        *,
        mode: str = "rb",
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: T.Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
        **kwargs,
    ) -> None:
        self._mode = mode

        if mode not in ("r", "rb"):
            raise ValueError(f"Invalid mode: {mode!r}, must be 'r' or 'rb'")

        # Text mode settings
        self._is_text_mode = "b" not in mode
        self._encoding = encoding or "utf-8" if self._is_text_mode else None

        if errors not in (None, "strict", "ignore", "replace", "backslashreplace"):
            raise ValueError(
                f"Invalid errors value: {errors!r}, must be "
                "'strict', 'ignore', 'replace', or 'backslashreplace'"
            )
        self._errors = errors or "backslashreplace" if self._is_text_mode else None

        if newline and not self._is_text_mode:
            raise ValueError("newline parameter only supported in text mode")
        elif newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError(
                f"Invalid newline value: {newline!r}, must be "
                "None, '', '\\n', '\\r', or '\\r\\n'"
            )
        if not self._is_text_mode:
            self._newline = NEWLINE
        elif not newline:
            self._newline = NEWLINE.decode(self._encoding)  # pyre-ignore[6]
        else:
            self._newline = newline

        if max_buffer_size == 0:
            block_capacity = block_forward = 0
        else:
            block_capacity = max(max_buffer_size // block_size, 1)

        self._is_auto_scaling = False
        if block_forward is None:
            block_forward = max(block_capacity - 1, 0)
            self._is_auto_scaling = block_forward > 0

        if 0 < block_capacity <= block_forward:
            raise ValueError(
                "max_buffer_size should greater than block_forward * block_size, "
                "got: max_buffer_size=%s, block_size=%s, block_forward=%s"
                % (max_buffer_size, block_size, block_forward)
            )

        # user maybe put block_size with 'numpy.uint64' type
        block_size = int(block_size)

        self._max_retries = max_retries
        self._block_size = block_size
        self._block_capacity = block_capacity  # Max number of blocks

        # Number of blocks every prefetch, which should be smaller than block_capacity
        self._block_forward = block_forward

        self._offset = 0
        self._cached_buffer = None
        self._block_index = 0  # Current block index
        self._cached_offset: T.Optional[int] = 0  # Current offset in the current block
        self._seek_history = []

        # Initialize tasks manager
        self._tasks = AsyncLRUCacheTaskManager(self.name)

        _logger.debug("open file: %r, mode: %s" % (self.name, self.mode))

    @abstractmethod
    async def _get_content_size(self) -> int:
        """Get the content size of the file.

        :return: Content size in bytes.
        """
        pass  # pragma: no cover

    @property
    def _content_size(self) -> int:
        """Return the cached content size.

        Note: This property assumes _get_content_size() has been called during init.
        For async initialization, use _init_content_size() separately.
        """
        if not hasattr(self, "_cached_content_size"):
            raise RuntimeError(
                "Content size not initialized. Call _init_content_size() first."
            )
        return self._cached_content_size  # pyre-ignore[16]

    @property
    def _block_stop(self) -> int:
        """Return the total number of blocks."""
        return ceil(self._content_size / self._block_size)

    async def _init_content_size(self) -> None:
        """Initialize content size asynchronously."""
        self._cached_content_size = await self._get_content_size()  # pyre-ignore[16]

    async def __aenter__(self):
        await self._init_content_size()
        return self

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the file."""
        pass  # pragma: no cover

    @property
    def mode(self) -> str:
        """Return the file mode.

        :return: File mode string.
        """
        return self._mode

    async def tell(self) -> int:
        """Return the current stream position."""
        return self._offset

    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte offset pos relative to position indicated by whence:

            0  Start of stream (the default).  pos should be >= 0;
            1  Current position - pos may be negative;
            2  End of stream - pos usually negative.

        Returns the new absolute position.
        """
        offset = int(offset)  # user maybe put offset with 'numpy.uint64' type
        if self.closed:
            raise IOError("file already closed: %r" % self.name)
        if whence == os.SEEK_CUR:
            target_offset = self._offset + offset
        elif whence == os.SEEK_END:
            target_offset = self._content_size + offset
        elif whence == os.SEEK_SET:
            target_offset = offset
        else:
            raise ValueError("invalid whence: %r" % whence)

        if target_offset == self._offset:
            return target_offset

        self._offset = max(min(target_offset, self._content_size), 0)
        block_index = self._offset // self._block_size
        block_offset = self._offset % self._block_size
        self._seek_buffer(block_index, block_offset)
        return self._offset

    async def _async_iterator_byte_blocks(self):
        """Async generator for byte blocks."""
        buffer = await self._get_buffer()
        yield buffer.read()

        while self._block_index < self._block_stop - 1:
            buffer = await self._get_next_buffer()
            yield buffer.read()

    def _empty_value(self) -> T.AnyStr:
        """Return the empty value for the current mode.

        :return: Empty string in text mode or empty bytes in binary mode.
        """
        return T.cast(T.AnyStr, "" if self._is_text_mode else b"")

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read at most size bytes/characters, returned as bytes or str.

        If the size argument is negative, read until EOF is reached.
        Return an empty bytes/str object at EOF.

        :param size: Number of bytes (binary mode) or characters (text mode) to read.
        :return: Bytes in binary mode, str in text mode.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if self._offset >= self._content_size:
            return self._empty_value()
        if size == 0:
            return self._empty_value()

        if not self._is_text_mode:
            if size is None or size < 0:
                size = self._content_size - self._offset
            else:
                size = min(size, self._content_size - self._offset)

            buffer = bytearray(size)
            await self.readinto(buffer)
            return bytes(buffer)

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1

        # Text mode: manually decode bytes
        str_buffer = StringIO()
        bytes_offset = 0
        decoder = codecs.getincrementaldecoder(self._encoding)(errors=self._errors)

        async for chunk in self._async_iterator_byte_blocks():
            total_bytes = len(chunk)
            decoded = decoder.decode(chunk, False)
            if size is not None and size > 0:
                chars_needed = size - str_buffer.tell()
                if len(decoded) >= chars_needed:
                    str_buffer.write(decoded[:chars_needed])

                    # Calculate bytes consumed
                    current_bytes_offset = len(
                        decoded[:chars_needed].encode(
                            self._encoding, errors=self._errors
                        )
                    )
                    if self._cached_buffer:
                        self._cached_buffer.seek(
                            current_bytes_offset - total_bytes,
                            os.SEEK_END,
                        )
                    bytes_offset += current_bytes_offset

                    self._offset += bytes_offset
                    return str_buffer.getvalue()
            str_buffer.write(decoded)
            bytes_offset += len(chunk)
        else:
            # Decode any remaining bytes
            decoded = decoder.decode(b"", True)
            str_buffer.write(decoded)
            bytes_offset = self._content_size - self._offset

        self._offset += bytes_offset
        return str_buffer.getvalue()

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Next line from the file, as bytes or str.

        Retain newline. A non-negative size argument limits the maximum
        number of bytes/characters to return (an incomplete line may be returned then).
        If the size argument is negative, read until EOF is reached.
        Return an empty bytes/str object at EOF.

        Note: In text mode, size refers to bytes before decoding, not characters.

        :param size: Maximum number of bytes to return.
        :return: Bytes in binary mode, str in text mode.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if self._offset >= self._content_size:
            return self._empty_value()
        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1
        if size == 0:
            return self._empty_value()

        bytes_offset = 0
        if self._is_text_mode:
            _buffer = StringIO()
            decoder = codecs.getincrementaldecoder(self._encoding)(errors=self._errors)
        else:
            _buffer = BytesIO()
            decoder = None
        async for chunk in self._async_iterator_byte_blocks():
            total_bytes = len(chunk)
            if decoder:
                chunk = decoder.decode(chunk, False)
            if size is not None and size > 0 and len(chunk) + _buffer.tell() >= size:
                latest_chars = chunk[: size - _buffer.tell()]
                _buffer.write(latest_chars)

                if self._is_text_mode:
                    current_bytes_offset = len(
                        latest_chars.encode(  # pytype: disable=attribute-error
                            self._encoding, errors=self._errors
                        )
                    )
                else:
                    current_bytes_offset = len(latest_chars)
                if self._cached_buffer:
                    self._cached_buffer.seek(
                        current_bytes_offset - total_bytes,
                        os.SEEK_END,
                    )
                bytes_offset += current_bytes_offset
                self._offset += bytes_offset
                return _buffer.getvalue()

            newline_index = chunk.find(self._newline)
            if newline_index >= 0:
                latest_chars = chunk[: newline_index + len(self._newline)]
                _buffer.write(latest_chars)
                if self._is_text_mode:
                    current_bytes_offset = len(
                        latest_chars.encode(  # pytype: disable=attribute-error
                            self._encoding, errors=self._errors
                        )
                    )
                else:
                    current_bytes_offset = len(latest_chars)
                if self._cached_buffer:
                    self._cached_buffer.seek(
                        current_bytes_offset - total_bytes,
                        os.SEEK_END,
                    )
                bytes_offset += current_bytes_offset
                self._offset += bytes_offset
                print(f"{self._offset} {current_bytes_offset}")
                return _buffer.getvalue()

            _buffer.write(chunk)
            bytes_offset += self._block_size
        else:
            if decoder:
                decoded = decoder.decode(b"", True)
                _buffer.write(decoded)  # pyre-ignore[6]
            bytes_offset = self._content_size - self._offset

        self._offset += bytes_offset
        return _buffer.getvalue()

    async def _read(self, size: int) -> bytes:
        """Read data without prefetch buffer.

        :param size: Number of bytes to read.
        :return: Bytes read.
        """
        if size == 0 or self._offset >= self._content_size:
            return b""

        resp = await self._fetch_response(
            start=self._offset, end=self._offset + size - 1
        )
        data = resp["Body"].read()
        await self.seek(size, os.SEEK_CUR)
        return data

    async def readinto(self, buffer: bytearray) -> int:
        """Read bytes into buffer.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)
        if self._is_text_mode:
            raise IOError("readinto() not supported in text mode")

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1

        if self._offset >= self._content_size:
            return 0

        size = len(buffer)
        size = min(size, self._content_size - self._offset)

        if self._block_capacity == 0:
            buffer[:size] = await self._read(size)
            return size

        if self._block_forward == 0:
            block_index = self._offset // self._block_size
            if block_index not in self._tasks:
                buffer[:size] = await self._read(size)
                return size

        buffer_obj = await self._get_buffer()
        data = buffer_obj.read(size)
        buffer[: len(data)] = data
        if len(data) == size:
            self._offset += len(data)
            return size

        offset = len(data)
        while offset < size:
            remain_size = size - offset
            next_buffer = await self._get_next_buffer()
            data = next_buffer.read(remain_size)
            buffer[offset : offset + len(data)] = data
            offset += len(data)

        self._offset += offset
        return size

    @property
    def _cached_blocks(self) -> list:
        """Return list of cached block indices."""
        return list(self._tasks.keys())

    async def _get_buffer(self) -> BytesIO:
        """Get the current buffer.

        :return: BytesIO buffer for current block.
        """
        if self._block_capacity == 0:
            buffer = await self._fetch_buffer(index=self._block_index)
            if self._cached_offset is not None:
                # seek when change self._cached_offset
                offset = self._cached_offset
            else:
                offset = self._offset % self._block_size
            buffer.seek(offset)
            self._cached_offset = None
            return buffer

        if self._cached_offset is not None:
            if self._block_forward > 0:  # pyre-ignore[58]
                start = self._block_index
                stop = min(
                    start + self._block_forward,  # pyre-ignore[58]
                    self._block_stop,
                )

                for index in range(start, stop + 1):
                    await self._submit_task(index)
                # for lru cache
                for index in range(stop, start - 1, -1):
                    await self._submit_task(index)
            else:
                await self._submit_task(self._block_index)
            await self._cleanup_tasks()

            self._cached_buffer = await self._fetch_task_result(self._block_index)
            self._cached_buffer.seek(self._cached_offset)
            self._cached_offset = None

        return self._cached_buffer

    async def _get_next_buffer(self) -> BytesIO:
        """Get next buffer by this function when finished reading current buffer.

        Make sure that _get_buffer is used before using _get_next_buffer(), or will make
        _cached_offset invalid.

        :return: BytesIO buffer for next block.
        """
        self._block_index += 1
        self._cached_offset = 0
        return await self._get_buffer()

    def _seek_buffer(self, index: int, offset: int = 0) -> None:
        """Seek to a specific block and offset.

        The corresponding block is probably not downloaded when seek to a new position
        So record the offset first, set it when it is accessed.

        :param index: Block index to seek to.
        :param offset: Offset within the block.
        """
        if self._is_auto_scaling:
            history = []
            for item in self._seek_history:
                if item.seek_count > self._block_capacity * 2:
                    # seek interval is bigger than self._block_capacity * 2, drop it
                    # from self._seek_history
                    continue
                if index - 1 < item.seek_index < index + 2:
                    continue
                item.seek_count += 1
                history.append(item)
            history.append(SeekRecord(index))
            self._seek_history = history
            self._block_forward = min(
                max(self._block_capacity // len(self._seek_history), 0),
                self._block_capacity - 1,
            )
            if self._block_forward == 0:
                self._is_auto_scaling = False
                self._seek_history = []

        self._cached_offset = offset
        self._block_index = index

    @abstractmethod
    async def _fetch_response(
        self, start: T.Optional[int] = None, end: T.Optional[int] = None
    ) -> dict:
        """Fetch response from storage.

        :param start: Start byte position.
        :param end: End byte position.
        :return: Response dict with 'Body' key.
        """
        pass  # pragma: no cover

    async def _fetch_buffer(self, index: int) -> BytesIO:
        """Fetch a single block buffer.

        :param index: Block index to fetch.
        :return: BytesIO buffer.
        """
        start, end = index * self._block_size, (index + 1) * self._block_size - 1
        response = await self._fetch_response(start=start, end=end)
        return response["Body"]

    async def _submit_task(self, index: int) -> None:
        """Submit a prefetch task for a block.

        :param index: Block index to prefetch.
        """
        if index < 0 or index >= self._block_stop:
            return
        await self._tasks.submit(index, self._fetch_buffer, index)

    def _insert_task(self, index: int, task: asyncio.Task) -> None:
        """Insert a task directly into the task manager.

        :param index: Block index.
        :param task: Asyncio task.
        """
        self._tasks[index] = task

    async def _fetch_task_result(self, index: int) -> BytesIO:
        """Fetch and return the result of a task.

        :param index: Block index.
        :return: BytesIO buffer.
        """
        return await self._tasks.result(index)

    async def _cleanup_tasks(self) -> None:
        """Cleanup old tasks to maintain memory limits."""
        await self._tasks.cleanup(self._block_capacity)

    async def close(self) -> None:
        """Close the reader and cleanup resources."""
        _logger.debug("close file: %r" % self.name)
        await self._tasks.aclear()  # clean memory


class AsyncLRUCacheTaskManager(OrderedDict):
    """LRU cache manager for asyncio tasks.

    Similar to LRUCacheFutureManager but uses asyncio.Task instead of
    concurrent.futures.Future.
    :param name: Name of the task manager (for logging).
    :type name: str
    :param block_capacity: Maximum number of tasks to keep.
    :type block_capacity: int
    """

    def __init__(self, name: str):
        super().__init__()
        self._name = name

    async def submit(self, key: int, coro_func, *args, **kwargs) -> None:
        """Submit a coroutine as a task.

        :param key: Cache key (block index).
        :param coro_func: Async function to execute.
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        """
        if key in self:
            self.move_to_end(key, last=True)
            return

        self[key] = asyncio.create_task(coro_func(*args, **kwargs))
        _logger.debug("submit task: %r, key: %r" % (self._name, key))

    @property
    def finished(self) -> bool:
        """Return True if all tasks are done.

        :return: True if all tasks completed.
        """
        return all(task.done() for task in self.values())

    async def result(self, key: int):
        """Get the result of a task.

        :param key: Cache key (block index).
        :return: Task result.
        """
        self.move_to_end(key, last=True)
        return await self[key]

    async def cleanup(self, block_capacity: int) -> list:
        """Remove old tasks to maintain capacity.

        :return: List of removed keys.
        """
        keys = []
        while len(self) > block_capacity:
            key, task = self.popitem(last=False)
            keys.append(key)
            if not task.done():
                task.cancel()
        if keys:
            _logger.debug("cleanup tasks: %r, keys: %s" % (self._name, keys))
        return keys

    async def aclear(self) -> None:
        """Clear all tasks and cancel pending ones."""
        await self.cleanup(0)
        self.clear()
