import os
import typing as T

import aiofiles
import aiofiles.ospath

from aiomegfile.interfaces import (
    AioClosable,
    AioReadable,
    AioSeekable,
    AioWritable,
)
from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.path import fspath, generate_cache_path


class AioFileCacher(
    AioReadable[T.AnyStr], AioWritable[T.AnyStr], AioSeekable[T.AnyStr]
):
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
        return await self._fileobj.readinto(buffer)  # pytype: disable=attribute-error

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

    async def __aenter__(self) -> "AioFileCacher":
        """Open cache file and initialize handlers.

        :return: Current cacher instance.
        :rtype: AioFileCacher
        """
        await aiofiles.os.makedirs(self._cache_dir, exist_ok=True)
        cache_path = generate_cache_path(self._path, self._cache_dir)
        mode = "wb+" if "b" in self._mode else "w+"
        self._fileobj = await aiofiles.open(cache_path, mode=mode)  # pyre-ignore[6]
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


class SmartCacher(AioClosable):
    """Async smart cache files in local filesystem."""

    cache_path = None

    def __init__(self, path: str, cache_path: T.Optional[str] = None, mode: str = "r"):
        """
        :param path: Path to cache.
        :type path: str
        :param cache_path: Path to cache file, defaults to None, will use ``/tmp``.
        :type cache_path: T.Optional[str], optional
        :param mode: Mode to open cache file, defaults to "r".
        :type mode: str, optional
        :raises ValueError: If mode is not one of "r", "w", "a".
        """
        if mode not in ("r", "w", "a"):
            raise ValueError("unacceptable mode: %r" % mode)
        self._path = fspath(path)
        self._mode = mode
        self.cache_path = cache_path or generate_cache_path(self._path)
        self._prepared = False

    @property
    def name(self) -> str:
        """Return the original path."""
        return self._path

    @property
    def mode(self) -> str:
        """Return the cache mode."""
        return self._mode

    async def __aenter__(self) -> str:
        """Prepare the cache and return the local cache path."""
        if not self._prepared:
            await self._prepare_cache()
        return self.cache_path

    async def close(self) -> None:
        """Upload cached content if needed and remove cache file."""
        if not self.cache_path:
            return
        if await aiofiles.ospath.exists(self.cache_path):
            if self._mode in ("w", "a"):
                await SmartPath(self.cache_path).copy_file(self._path)
            await aiofiles.os.unlink(self.cache_path)

    async def _prepare_cache(self) -> None:
        """Ensure cache directory exists and download source when needed."""
        cache_dir = os.path.dirname(self.cache_path)
        if cache_dir and cache_dir != ".":
            await aiofiles.os.makedirs(cache_dir, exist_ok=True)
        if self._mode in ("r", "a"):
            await SmartPath(self._path).copy_file(self.cache_path)
        self._prepared = True


class NullCacher(AioClosable):
    """Async no-op cacher for local paths."""

    cache_path = None

    def __init__(self, path: str):
        """
        :param path: Local path to return.
        :type path: str
        """
        self.cache_path = fspath(path)

    async def __aenter__(self) -> str:
        """Return the local path."""
        return self.cache_path

    async def close(self) -> None:
        """No-op close for local paths."""
        return None
