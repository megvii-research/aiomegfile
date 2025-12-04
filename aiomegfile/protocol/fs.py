import asyncio
import glob
import os
import stat
import typing as T

import aiofiles
import aiofiles.os
import aiofiles.ospath

from aiomegfile.interfaces import BaseProtocol, StatResult


class FSProtocol(BaseProtocol):
    """
    Protocol for local filesystem operations.
    """

    protocol_name = "file"

    async def is_dir(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory."""
        try:
            if followlinks:
                return await aiofiles.ospath.isdir(self.path_without_protocol)
            stat_result = await asyncio.to_thread(os.lstat, self.path_without_protocol)
            return stat.S_ISDIR(stat_result.st_mode)
        except OSError:
            return False

    async def is_file(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file."""
        try:
            if followlinks:
                return await aiofiles.ospath.isfile(self.path_without_protocol)
            stat_result = await asyncio.to_thread(os.lstat, self.path_without_protocol)
            return stat.S_ISREG(stat_result.st_mode)
        except OSError:
            return False

    async def exists(self, followlinks: bool = False) -> bool:
        """Whether the path points to an existing file or directory."""
        try:
            if followlinks:
                return await aiofiles.ospath.exists(self.path_without_protocol)
            await asyncio.to_thread(os.lstat, self.path_without_protocol)
            return True
        except OSError:
            return False

    async def stat(self, follow_symlinks: bool = True) -> StatResult:
        """Get the status of the path."""
        stat_result = await aiofiles.os.stat(
            self.path_without_protocol, follow_symlinks=follow_symlinks
        )

        return StatResult(
            size=stat_result.st_size,
            ctime=stat_result.st_ctime,
            mtime=stat_result.st_mtime,
            isdir=stat.S_ISDIR(stat_result.st_mode),
            islnk=stat.S_ISLNK(stat_result.st_mode),
            extra=stat_result,
        )

    async def remove(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""
        try:
            await aiofiles.os.remove(self.path_without_protocol)
        except FileNotFoundError:
            if not missing_ok:
                raise

    async def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a directory."""
        try:
            if parents:
                await aiofiles.os.makedirs(
                    self.path_without_protocol, mode=mode, exist_ok=exist_ok
                )
            else:
                await aiofiles.os.mkdir(self.path_without_protocol, mode=mode)
        except FileExistsError:
            if not exist_ok:
                raise

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd: bool = True,
    ) -> T.AsyncContextManager:
        """Open the file with mode."""
        return aiofiles.open(  # pytype: disable=wrong-arg-types
            self.path_without_protocol,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
        )

    async def walk(
        self, followlinks: bool = False
    ) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
        """Generate the file names in a directory tree by walking the tree."""
        for root, dirs, files in os.walk(
            self.path_without_protocol, followlinks=followlinks
        ):
            yield root, dirs, files

    async def iglob(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> T.AsyncIterator[str]:
        """Return an iterator of files whose paths match the glob pattern."""

        # FIXME: support more glob features like [] and {}
        for path in glob.iglob(
            os.path.join(self.path_without_protocol, pattern), recursive=recursive
        ):
            yield path

    async def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """Change the access permissions of a file."""
        await asyncio.to_thread(
            os.chmod, self.path_without_protocol, mode, follow_symlinks=follow_symlinks
        )

    async def rename(self, dst_path: str, overwrite: bool = True) -> str:
        """
        Rename file.

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :returns: The destination path
        """
        if not overwrite and await aiofiles.ospath.exists(dst_path):
            raise FileExistsError(f"Destination path already exists: {dst_path}")
        await aiofiles.os.rename(self.path_without_protocol, dst_path)
        return dst_path

    async def symlink(self, dst_path: str) -> None:
        """Create a symbolic link pointing to dst_path."""
        await aiofiles.os.symlink(self.path_without_protocol, dst_path)

    async def readlink(self) -> str:
        """Return a new path representing the symbolic link's target."""
        return await aiofiles.os.readlink(self.path_without_protocol)

    async def is_symlink(self) -> bool:
        """Return True if the path points to a symbolic link."""
        return await aiofiles.ospath.islink(self.path_without_protocol)

    async def iterdir(self) -> T.AsyncIterator[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        files = await aiofiles.os.listdir(self.path_without_protocol)
        for filename in sorted(files):
            yield os.path.join(self.path_without_protocol, filename)

    async def absolute(self) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object.
        """
        return os.path.abspath(self.path_without_protocol)

    def __eq__(self, other: BaseProtocol) -> bool:
        """Return True if the path points to the same file as other_path."""
        if other.protocol_name != self.protocol_name:
            return False
        return os.path.samefile(self.path_without_protocol, other.path_without_protocol)
