import os
import typing as T

import aiofiles

from aiomegfile.interfaces import (
    AioReadable,
    AioSeekable,
    AioWritable,
)
from aiomegfile.utils.path import generate_cache_path


class AioCacher(AioReadable[T.AnyStr], AioWritable[T.AnyStr], AioSeekable[T.AnyStr]):
    """Async cacher file-like base class."""

    def __init__(
        self,
        path: str,
        mode: str,
        *,
        download_fileobj: T.Callable[[str, AioWritable], T.Awaitable[None]],
        upload_fileobj: T.Callable[[AioReadable, str], T.Awaitable[None]],
        cache_dir: T.Optional[str] = None,
    ):
        self._mode = mode
        self._fileobj = None
        self._path = path
        self._cache_dir = cache_dir or "/tmp"
        self._download_fileobj = download_fileobj
        self._upload_fileobj = upload_fileobj

    @property
    def name(self) -> str:
        return self._path

    @property
    def mode(self) -> str:
        return self._mode

    async def readable(self) -> bool:
        return "r" in self._mode or "+" in self._mode

    async def writable(self) -> bool:
        return "w" in self._mode or "a" in self._mode or "+" in self._mode

    async def read(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read bytes from cache file.

        :param size: Maximum bytes to read.
        :type size: int, optional
        :return: Read data.
        :rtype: T.AnyStr
        """
        await self._read_check()
        return await self._fileobj.read(size)

    async def readline(self, size: T.Optional[int] = None) -> T.AnyStr:
        """Read a single line from cache file.

        :param size: Maximum bytes to read.
        :type size: int, optional
        :return: Read line.
        :rtype: T.AnyStr
        """
        await self._read_check()
        return await self._fileobj.readline(size)

    async def readlines(self, hint: T.Optional[int] = None) -> T.List[T.AnyStr]:
        """Read all lines from cache file.

        :param hint: Maximum bytes to read.
        :type hint: int, optional
        :return: List of lines.
        :rtype: T.List[T.AnyStr]
        """
        await self._read_check()
        return await self._fileobj.readlines(hint)

    async def readinto(self, buffer: bytearray):
        """Read bytes into buffer.

        :param buffer: Buffer to read into.
        :type buffer: bytearray
        :return: Bytes read.
        :rtype: int
        """
        await self._read_check()
        return await self._fileobj.readinto(buffer)

    async def write(self, data: T.AnyStr) -> int:
        """Write data to cache file.

        :param data: Data to write.
        :type data: T.AnyStr
        :return: Bytes written.
        :rtype: int
        """
        await self._write_check()
        return await self._fileobj.write(data)

    async def writelines(self, lines: T.Iterable[T.AnyStr]) -> None:
        """Write multiple lines to cache file.

        :param lines: Lines to write.
        :type lines: T.Iterable[T.AnyStr]
        """
        await self._write_check()
        await self._fileobj.writelines(lines)

    async def truncate(self, size: T.Optional[int] = None) -> int:
        """Truncate cache file to size.

        :param size: Size to truncate to.
        :type size: int, optional
        :return: New size.
        :rtype: int
        """
        await self._write_check()
        return await self._fileobj.truncate(size)

    async def tell(self) -> int:
        """Return the current stream position.

        :return: Current position.
        :rtype: int
        """
        if self._fileobj is None:
            raise RuntimeError("file not opened")
        return await self._fileobj.tell()

    async def flush(self) -> None:
        """Flush the cache file buffer."""
        if self._fileobj is None:
            return
        await self._fileobj.flush()

    async def __aenter__(self) -> "AioCacher":
        """Open cache file and initialize handlers.

        :return: Current cacher instance.
        :rtype: AioCacher
        """
        await aiofiles.os.makedirs(self._cache_dir, exist_ok=True)
        cache_path = generate_cache_path(self._path, self._cache_dir)
        self._fileobj = await aiofiles.open(cache_path, mode="wb+")
        await aiofiles.os.unlink(cache_path)
        if "w" not in self._mode:
            try:
                await self._download_fileobj(  # pytype: disable=wrong-arg-types
                    self._path,
                    self._fileobj,
                )
                if "a" not in self._mode:
                    await self._fileobj.seek(0)
            except FileNotFoundError:
                pass
            except Exception:
                await self._fileobj.close()
                raise

        return self

    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        :param offset: Byte offset.
        :type offset: int
        :param whence: Seek reference point.
        :type whence: int
        :return: New absolute position.
        :rtype: int
        """
        if self._fileobj is None:
            raise RuntimeError("file not opened")
        if "a" in self._mode and "+" not in self._mode:
            return await self._fileobj.seek(0, os.SEEK_END)
        return await self._fileobj.seek(offset, whence)

    async def close(self) -> None:
        """Flush, upload if needed, and close cache file."""
        if self._fileobj is None:
            return
        if await self.writable():
            await self._fileobj.flush()
            await self._fileobj.seek(0)
            await self._upload_fileobj(  # pytype: disable=wrong-arg-types
                self._fileobj,
                self._path,
            )
        await self._fileobj.close()

    async def _read_check(self) -> None:
        """Validate readable state.

        :raises RuntimeError: When file is not opened.
        :raises IOError: When file is not open for reading.
        """
        if self._fileobj is None:
            raise RuntimeError("file not opened")
        if not await self.readable():
            raise IOError("file not open for reading")

    async def _write_check(self) -> None:
        """Validate writable state.

        :raises RuntimeError: When file is not opened.
        :raises IOError: When file is not open for writing.
        """
        if self._fileobj is None:
            raise RuntimeError("file not opened")
        if not await self.writable():
            raise IOError("file not open for writing")
