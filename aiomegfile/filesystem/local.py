import asyncio
import os
import stat
import typing as T

import aiofiles
import aiofiles.os
import aiofiles.ospath

from aiomegfile.interfaces import BaseFileSystem, StatResult
from aiomegfile.lib.url import split_uri


class LocalFileSystem(BaseFileSystem):
    """
    Protocol for local filesystem operations.
    """

    protocol = "file"

    def __init__(self, protocol_in_path: bool):
        self.protocol_in_path = protocol_in_path

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a directory, otherwise False.
        """
        try:
            if followlinks:
                return await aiofiles.ospath.isdir(path)
            stat_result = await asyncio.to_thread(os.lstat, path)
            return stat.S_ISDIR(stat_result.st_mode)
        except OSError:
            return False

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path is a regular file, otherwise False.
        """
        try:
            if followlinks:
                return await aiofiles.ospath.isfile(path)
            stat_result = await asyncio.to_thread(os.lstat, path)
            return stat.S_ISREG(stat_result.st_mode)
        except OSError:
            return False

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing file or directory.

        :param followlinks: Whether to follow symbolic links.
        :return: True if the path exists, otherwise False.
        """
        try:
            if followlinks:
                return await aiofiles.ospath.exists(path)
            await asyncio.to_thread(os.lstat, path)
            return True
        except OSError:
            return False

    async def stat(self, path: str, follow_symlinks: bool = True) -> StatResult:
        """Get the status of the path.

        :param follow_symlinks: Whether to follow symbolic links.
        :return: Populated StatResult for the path.
        """
        stat_result = await aiofiles.os.stat(path, follow_symlinks=follow_symlinks)

        return StatResult(
            size=stat_result.st_size,
            ctime=stat_result.st_ctime,
            mtime=stat_result.st_mtime,
            isdir=stat.S_ISDIR(stat_result.st_mode),
            islnk=stat.S_ISLNK(stat_result.st_mode),
            extra=stat_result,
        )

    async def unlink(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file.

        :param missing_ok: If False, raise when the file does not exist.
        :raises FileNotFoundError: When missing_ok is False and the file is absent.
        """
        try:
            await aiofiles.os.unlink(path)
        except FileNotFoundError:
            if not missing_ok:
                raise

    async def rmdir(self, path: str) -> None:
        """Remove (delete) the directory."""
        await aiofiles.os.rmdir(path)

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param mode: Permission bits for the new directory.
        :param parents: Whether to create missing parents.
        :param exist_ok: Whether to ignore if the directory exists.
        :raises FileExistsError: When directory exists and exist_ok is False.
        """
        try:
            if parents:
                await aiofiles.os.makedirs(path, mode=mode, exist_ok=exist_ok)
            else:
                await aiofiles.os.mkdir(path, mode=mode)
        except FileExistsError:
            if not exist_ok:
                raise

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
    ) -> T.AsyncContextManager:
        """Open the file with mode.

        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding when using text modes.
        :param errors: Error handling strategy for encoding/decoding.
        :param newline: Newline handling in text mode.
        :return: Async file context manager.
        """
        return aiofiles.open(  # pytype: disable=wrong-arg-types
            path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    async def walk(
        self, path: str, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree.

        :param followlinks: Whether to traverse symbolic links to directories.
        :return: Async iterator of (root, dirs, files).
        """
        for root, dirs, files in os.walk(path, followlinks=followlinks):
            yield root, dirs, files

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        Move file.

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :return: The destination path
        """
        if not overwrite and await aiofiles.ospath.exists(dst_path):
            raise FileExistsError(f"Destination path already exists: {dst_path}")
        await aiofiles.os.rename(src_path, dst_path)
        return dst_path

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to self named dst_path.

        :param dst_path: The symbolic link path.
        """
        await aiofiles.os.symlink(src_path, dst_path)

    async def readlink(self, path: str) -> str:
        """Return a new path representing the symbolic link's target."""
        return await aiofiles.os.readlink(path)

    async def is_symlink(self, path: str) -> bool:
        """Return True if the path points to a symbolic link."""
        return await aiofiles.ospath.islink(path)

    async def iterdir(self, path: str) -> T.AsyncIterator[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :return: All contents have in the path in ascending alphabetical order
        """
        files = await aiofiles.os.listdir(path)
        for filename in sorted(files):
            yield os.path.join(path, filename)

    async def absolute(self, path: str) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object.
        """
        return os.path.abspath(path)

    async def samefile(self, path: str, other_path: str) -> bool:
        """
        Return True if the path points to the same file as other_path.

        :param path: First path to compare.
        :param other_path: Other path to compare.
        :return: True if both paths point to the same file, otherwise False.
        """
        try:
            return await asyncio.to_thread(os.path.samefile, path, other_path)
        except FileNotFoundError:
            return False

    def same_endpoint(self, other_filesystem: "LocalFileSystem") -> bool:
        """
        Local filesystem endpoints match when protocols match.

        :param other_filesystem: Filesystem to compare.
        :return: True if both represent the same endpoint.
        """
        if isinstance(other_filesystem, LocalFileSystem):
            return True
        return False

    def get_path_from_uri(self, uri: str) -> str:
        """
        Extract path component from uri.

        :param uri: URI string.
        :return: Path part string.
        """
        _, path, _ = split_uri(uri)
        return path

    def generate_uri(self, path: str) -> str:
        """
        Generate file URI from path without protocol.

        :param path: Path without protocol.
        :return: URI string.
        """
        if not self.protocol_in_path:
            return path
        return f"{self.protocol}://{path}"

    @classmethod
    def from_uri(cls, uri: str) -> "LocalFileSystem":
        """
        Create LocalFileSystem from uri string.

        :param uri: URI string.
        :return: LocalFileSystem instance.
        """
        return cls(protocol_in_path="file://" in uri)
