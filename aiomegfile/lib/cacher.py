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
        self._cache_dir = cache_dir
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

    async def __aenter__(self) -> "AioCacher":
        cache_path = generate_cache_path(self._path, self._cache_dir)
        self._fileobj = await aiofiles.open(cache_path, mode="wb+")
        await aiofiles.os.unlink(cache_path)
        if "w" not in self._mode:
            try:
                await self._download_fileobj(self._path, self._fileobj)
                if "a" not in self._mode:
                    await self._fileobj.seek(0)
            except FileNotFoundError:
                pass

        self.read = self._fileobj.read
        self.readline = self._fileobj.readline
        self.readlines = self._fileobj.readlines
        self.write = self._fileobj.write
        self.writelines = self._fileobj.writelines
        self.tell = self._fileobj.tell
        self.flush = self._fileobj.flush

        return self

    async def seek(self, offset, whence=os.SEEK_SET):
        if "a" in self._mode:
            return
        return await self._fileobj.seek(offset, whence)

    async def close(self):
        if hasattr(self, "_fileobj"):
            if await self.writable():
                await self._fileobj.seek(0)
                await self._upload_fileobj(self._fileobj, self._path)
            await self._fileobj.close()
