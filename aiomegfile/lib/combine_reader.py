import asyncio
import inspect
import os
import typing as T
from io import BytesIO, StringIO

from aiomegfile.interfaces import AioReadable, AioSeekable

CloseAction = T.Callable[[], T.Awaitable[None]]


async def _maybe_await(value: T.Any) -> T.Any:
    """Return the awaited value when value is awaitable.

    :param value: Value or awaitable to resolve.
    :return: Resolved value.
    """
    if inspect.isawaitable(value):
        return await value
    return value


async def _call_method(fileobj: T.Any, method_name: str, *args: T.Any) -> T.Any:
    """Call a method on a file-like object and await if needed.

    :param fileobj: Target file-like object.
    :param method_name: Name of method to invoke.
    :param args: Positional arguments for the method.
    :return: Method result.
    """
    method = getattr(fileobj, method_name)
    return await _maybe_await(method(*args))


def _get_name(fileobj: T.Any) -> str:
    """Return a friendly name for a file-like object.

    :param fileobj: File-like object.
    :return: Display name for error messages.
    :rtype: str
    """
    name = getattr(fileobj, "name", None)
    if name is None:
        return repr(fileobj)
    return name


def _get_mode(fileobj: T.Any, fallback: str) -> str:
    """Return mode for a file-like object.

    :param fileobj: File-like object.
    :param fallback: Fallback mode when object lacks a mode attribute.
    :return: Mode string.
    :rtype: str
    """
    mode = getattr(fileobj, "mode", None)
    if mode is None:
        return fallback
    return mode


async def _is_readable(fileobj: T.Any) -> bool:
    """Return True if file-like object is readable.

    :param fileobj: File-like object.
    :return: True if readable, else False.
    :rtype: bool
    """
    readable = getattr(fileobj, "readable", None)
    if readable is None:
        return hasattr(fileobj, "read")
    if callable(readable):
        return bool(await _maybe_await(readable()))
    return bool(readable)


async def _is_seekable(fileobj: T.Any) -> bool:
    """Return True if file-like object is seekable.

    :param fileobj: File-like object.
    :return: True if seekable, else False.
    :rtype: bool
    """
    seekable = getattr(fileobj, "seekable", None)
    if seekable is None:
        return hasattr(fileobj, "seek")
    if callable(seekable):
        return bool(await _maybe_await(seekable()))
    return bool(seekable)


async def _is_writable(fileobj: T.Any) -> bool:
    """Return True if file-like object is writable.

    :param fileobj: File-like object.
    :return: True if writable, else False.
    :rtype: bool
    """
    writable = getattr(fileobj, "writable", None)
    if writable is None:
        return hasattr(fileobj, "write")
    if callable(writable):
        return bool(await _maybe_await(writable()))
    return bool(writable)


async def _get_content_size(fileobj: T.Any, *, intrusive: bool = False) -> int:
    """Return size of a file-like object.

    :param fileobj: File-like object to inspect.
    :param intrusive: If True, do not restore the original position.
    :return: Content size.
    :rtype: int
    :raises IOError: When the size cannot be determined.
    """
    if isinstance(fileobj, (BytesIO, StringIO)):
        return len(fileobj.getvalue())

    if hasattr(fileobj, "_content_size"):
        try:
            size = getattr(fileobj, "_content_size")
            size = await _maybe_await(size)
            if isinstance(size, int):
                return size
        except Exception:
            pass

    if not hasattr(fileobj, "tell"):
        raise IOError("not seekable: %r" % _get_name(fileobj))

    offset = await _call_method(fileobj, "tell")
    if not await _is_seekable(fileobj) and await _is_writable(fileobj):
        return int(offset)

    if not hasattr(fileobj, "seek"):
        raise IOError("not seekable: %r" % _get_name(fileobj))

    await _call_method(fileobj, "seek", 0, os.SEEK_END)
    size = await _call_method(fileobj, "tell")
    if not intrusive:
        await _call_method(fileobj, "seek", offset)
    return int(size)


def _endswith_newline(data: T.AnyStr) -> bool:
    """Return True if data ends with a newline.

    :param data: Data chunk to inspect.
    :return: True if ending with newline, else False.
    :rtype: bool
    """
    if not data:
        return False
    if isinstance(data, bytes):
        return data.endswith(b"\n")
    return T.cast(str, data).endswith("\n")


def _make_close_action(
    fileobj: T.Any, context: T.Optional[T.Any] = None
) -> CloseAction:
    """Create an async close action for the given file or context.

    :param fileobj: Opened file object.
    :param context: Optional context manager that opened the file.
    :return: Async callable that closes the resource.
    :rtype: CloseAction
    """

    async def _close_context() -> None:
        if context is not None and hasattr(context, "__aexit__"):
            await _maybe_await(context.__aexit__(None, None, None))
            return
        if hasattr(fileobj, "close"):
            await _maybe_await(fileobj.close())

    return _close_context


class AioCombineReader(AioReadable[T.AnyStr], AioSeekable[T.AnyStr]):
    """Async reader combining multiple files into a single stream.

    :param path_glob: Glob pattern for source files.
    :param mode: File open mode.
    :param open_func: Callable used to open each file.
    :param glob_func: Async callable returning matching paths.
    """

    def __init__(
        self,
        path_glob: str,
        *,
        mode: str,
        open_func: T.Callable[[str, str], T.Any],
        glob_func: T.Callable[[str], T.Awaitable[T.List[str]]],
    ) -> None:
        """Initialize the combined reader configuration.

        :param path_glob: Glob pattern for source files.
        :param mode: File open mode.
        :param open_func: Callable used to open each file.
        :param glob_func: Async callable returning matching paths.
        """
        self._path_glob = path_glob
        self._requested_mode = mode
        self._mode = mode
        self._open_func = open_func
        self._glob_func = glob_func
        self._file_objects: T.List[T.Any] = []
        self._close_actions: T.List[CloseAction] = []
        self._blocks_sizes: T.List[int] = []
        self._content_size = 0
        self._offset = 0
        self._initialized = False
        self._init_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """Return the combined reader name.

        :return: Reader name.
        :rtype: str
        """
        return self._path_glob

    @property
    def mode(self) -> str:
        """Return the mode of the combined reader.

        :return: Mode string.
        :rtype: str
        """
        return self._mode

    @property
    def _block_index_and_offset(self) -> T.Tuple[int, int]:
        """Return the block index and offset within that block.

        :return: Tuple of (block_index, block_offset).
        :rtype: T.Tuple[int, int]
        """
        for index, size in enumerate(self._blocks_sizes):
            if self._offset < size:
                return index - 1, self._offset - self._blocks_sizes[index - 1]
        raise IOError("offset out of range: %d" % self._offset)

    def _empty_value(self) -> T.AnyStr:
        """Return empty value for the current mode.

        :return: Empty bytes or string.
        :rtype: T.AnyStr
        """
        if "b" in self._mode:
            return T.cast(T.AnyStr, b"")
        return T.cast(T.AnyStr, "")

    def _empty_buffer(self) -> T.Union[BytesIO, StringIO]:
        """Return a buffer suitable for the current mode.

        :return: BytesIO for binary, StringIO for text.
        :rtype: T.Union[BytesIO, StringIO]
        """
        if "b" in self._mode:
            return BytesIO()
        return StringIO()

    async def __aenter__(self) -> "AioCombineReader":
        """Initialize the combined reader.

        :return: Self after initialization.
        :rtype: AioCombineReader
        """
        await self._ensure_initialized()
        return self

    async def _open_file(self, path: str) -> T.Tuple[T.Any, CloseAction]:
        """Open a file and return its object and close action.

        :param path: Path to open.
        :return: Tuple of opened file object and close action.
        :rtype: T.Tuple[T.Any, CloseAction]
        """
        opener = self._open_func(path, self._requested_mode)
        if inspect.isawaitable(opener):
            fileobj = await opener
            return fileobj, _make_close_action(fileobj)
        if hasattr(opener, "__aenter__"):
            fileobj = await opener.__aenter__()
            return fileobj, _make_close_action(fileobj, context=opener)
        return opener, _make_close_action(opener)

    async def _ensure_initialized(self) -> None:
        """Initialize file objects and metadata once.

        :return: None
        :rtype: None
        """
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            await self._initialize()
            self._initialized = True

    async def _initialize(self) -> None:
        """Open files and compute combined metadata.

        :return: None
        :rtype: None
        """
        paths = sorted(await self._glob_func(self._path_glob))
        file_objects: T.List[T.Any] = []
        close_actions: T.List[CloseAction] = []
        blocks_sizes: T.List[int] = []
        content_size = 0
        mode: T.Optional[str] = None
        try:
            for path in paths:
                fileobj, close_action = await self._open_file(path)
                file_objects.append(fileobj)
                close_actions.append(close_action)
                if not await _is_readable(fileobj):
                    raise IOError("not readable: %r" % _get_name(fileobj))
                file_mode = _get_mode(fileobj, self._requested_mode)
                if mode is None:
                    mode = file_mode
                elif mode != file_mode:
                    raise IOError(
                        "inconsistent mode: %r, expected: %r, got: %r"
                        % (_get_name(fileobj), mode, file_mode)
                    )
                blocks_sizes.append(content_size)
                content_size += await _get_content_size(fileobj)
        except Exception:
            await self._close_actions_list(close_actions)
            raise

        blocks_sizes.append(content_size)
        self._file_objects = file_objects
        self._close_actions = close_actions
        self._blocks_sizes = blocks_sizes
        self._content_size = content_size
        self._mode = mode or self._requested_mode
        self._offset = 0

    async def _close_actions_list(self, actions: T.Iterable[CloseAction]) -> None:
        """Close all actions, ignoring subsequent errors.

        :param actions: Close actions to execute.
        :return: None
        :rtype: None
        """
        for action in actions:
            try:
                await action()
            except Exception:
                pass

    async def tell(self) -> int:
        """Return the current stream position.

        :return: Current offset.
        :rtype: int
        """
        await self._ensure_initialized()
        return self._offset

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read bytes from the combined stream.

        :param size: Maximum bytes to read, or read to EOF if negative/None.
        :return: Read data.
        :rtype: T.AnyStr
        """
        await self._ensure_initialized()
        if self._offset >= self._content_size:
            return self._empty_value()
        if size is None or size < 0:
            size = self._content_size - self._offset
        buffer = self._empty_buffer()
        while size > 0 and self._offset < self._content_size:
            block_index, block_offset = self._block_index_and_offset
            fileobj = self._file_objects[block_index]
            await _call_method(fileobj, "seek", block_offset)
            data = await _call_method(fileobj, "read", size)
            if not data:
                break
            buffer.write(data)
            data_len = len(data)
            size -= data_len
            self._offset += data_len
        return T.cast(T.AnyStr, buffer.getvalue())

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read one line from the combined stream.

        :param size: Maximum bytes to read.
        :return: Read line.
        :rtype: T.AnyStr
        """
        await self._ensure_initialized()
        if self._offset >= self._content_size:
            return self._empty_value()
        if size is None or size < 0:
            size = self._content_size - self._offset
        block_index, block_offset = self._block_index_and_offset
        fileobj = self._file_objects[block_index]
        await _call_method(fileobj, "seek", block_offset)
        data = await _call_method(fileobj, "readline", size)
        self._offset += len(data)
        if len(data) == size or _endswith_newline(data):
            return T.cast(T.AnyStr, data)
        buffer = self._empty_buffer()
        if data:
            buffer.write(data)
        while True:
            remain_size = size - buffer.tell()
            if remain_size <= 0:
                break
            block_index, block_offset = self._block_index_and_offset
            fileobj = self._file_objects[block_index]
            await _call_method(fileobj, "seek", block_offset)
            data = await _call_method(fileobj, "readline", remain_size)
            if not data:
                break
            buffer.write(data)
            self._offset += len(data)
            if buffer.tell() == size or _endswith_newline(data):
                break
        return T.cast(T.AnyStr, buffer.getvalue())

    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        :param offset: Byte offset.
        :param whence: Seek reference point.
        :return: New absolute position.
        :rtype: int
        """
        await self._ensure_initialized()
        offset = int(offset)
        if whence == os.SEEK_SET:
            target_offset = offset
        elif whence == os.SEEK_CUR:
            target_offset = self._offset + offset
        elif whence == os.SEEK_END:
            target_offset = self._content_size + offset
        else:
            raise ValueError("invalid whence: %r" % whence)

        if target_offset < 0:
            raise ValueError("negative seek value %r" % target_offset)

        self._offset = target_offset
        return self._offset

    async def close(self) -> None:
        """Close the combined reader and underlying file objects.

        :return: None
        :rtype: None
        """
        if not self._initialized:
            return
        await self._close_actions_list(self._close_actions)
        self._file_objects = []
        self._close_actions = []
        self._blocks_sizes = []
        self._content_size = 0
