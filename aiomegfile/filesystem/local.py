import asyncio
import os
import shutil
import stat
import typing as T

import aiofiles
import aiofiles.os
import aiofiles.ospath

from aiomegfile.interfaces import (
    AioScannableManager,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.utils.path import split_uri


class LocalFileSystem(BaseFileSystem):
    """
    Protocol for local filesystem operations.
    """

    protocol = "file"

    def __init__(self, protocol_in_path: bool):
        """Create a LocalFileSystem instance.

        :param protocol_in_path: Whether incoming paths include the ``file://`` prefix.
        """
        self.protocol_in_path = protocol_in_path

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param path: Path to check.
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

        :param path: Path to check.
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

        :param path: Path to check.
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

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Get the status of the path.

        :param path: Path to stat.
        :param followlinks: Whether to follow symbolic links.
        :raises FileNotFoundError: If the path does not exist.
        :return: Populated StatResult for the path.
        """
        stat_result = await aiofiles.os.stat(path, follow_symlinks=followlinks)

        return StatResult(
            st_size=stat_result.st_size,
            st_ctime=stat_result.st_ctime,  # pyre-ignore[16]
            st_mtime=stat_result.st_mtime,
            isdir=stat.S_ISDIR(stat_result.st_mode),
            islnk=stat.S_ISLNK(stat_result.st_mode),
            extra=stat_result,
        )

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) the file or directory.

        If path is a file, remove it directly.
        If path is a directory, remove it and all its contents recursively.

        :param path: Path to remove.
        :param missing_ok: If False, raise when the path does not exist.
        :raises FileNotFoundError: When missing_ok is False and the path is absent.
        """
        try:
            if await self.is_dir(path):
                await asyncio.to_thread(shutil.rmtree, path)
            else:
                await aiofiles.os.unlink(path)
        except FileNotFoundError:
            if not missing_ok:
                raise

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param path: Directory path to create.
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

        :param path: File path to open.
        :param mode: File open mode.
        :param buffering: Buffering policy.
        :param encoding: Text encoding when using text modes.
        :param errors: Error handling strategy for encoding/decoding.
        :param newline: Newline handling in text mode.
        :return: Async file context manager.
        """
        dir_path = os.path.dirname(path)
        if dir_path and dir_path != ".":
            os.makedirs(os.path.dirname(path), exist_ok=True)

        return aiofiles.open(  # pytype: disable=wrong-arg-types
            path,
            mode=mode,  # pyre-ignore[6]
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def scandir(self, path) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return an async context manager for iterating directory entries.

        :param path: Directory to scan.
        :return: Async context manager producing ``FileEntry`` items.
        """
        sync_scandir = os.scandir(path)

        async def aiterator():
            for entry in sync_scandir:
                stat_result = entry.stat()
                yield FileEntry(
                    name=entry.name,
                    path=entry.path,
                    stat=StatResult(
                        st_size=stat_result.st_size,
                        st_ctime=stat_result.st_ctime,
                        st_mtime=stat_result.st_mtime,
                        isdir=stat.S_ISDIR(stat_result.st_mode),
                        islnk=stat.S_ISLNK(stat_result.st_mode),
                        extra=stat_result,
                    ),
                )

        async def aexit(exc_type, exc_value, traceback) -> None:
            sync_scandir.close()

        return AioScannableManager(aiterator(), aexit)

    def scanfile(
        self,
        path,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """
        Iteratively traverse only files in given directory.
        Every iteration on generator yields FileEntry object.

        :returns: Async context manager yielding an async iterator of FileEntry objects.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """

        def _normalize_sort(name: str) -> str:
            """Normalize path separators for consistent sorting.

            :param name: Path or entry name to normalize.
            :return: Normalized name used for sorting.
            """
            return name.replace(os.path.sep, "/")

        async def _iter_dir(dir_path: str) -> T.AsyncIterator[FileEntry]:
            """Yield FileEntry objects in sorted order under dir_path.

            :param dir_path: Directory path to traverse.
            :return: Async iterator yielding FileEntry objects.
            """
            try:
                entries = await asyncio.to_thread(list, os.scandir(dir_path))
            except OSError:
                return

            names: T.List[str] = []
            entry_map: T.Dict[str, T.Tuple[os.DirEntry, bool]] = {}
            for entry in entries:
                name = entry.name
                is_dir = entry.is_dir(follow_symlinks=False)
                is_symlink_dir = False
                if not is_dir and entry.is_symlink():
                    try:
                        is_symlink_dir = entry.is_dir(follow_symlinks=True)
                    except OSError:
                        is_symlink_dir = False
                if is_dir or is_symlink_dir:
                    names.append(name + os.path.sep)
                else:
                    names.append(name)
                entry_map[name] = (entry, is_symlink_dir)

            names.sort(key=_normalize_sort)
            for name in names:
                is_dir_entry = name.endswith(os.path.sep)
                raw_name = name[:-1] if is_dir_entry else name
                entry, is_symlink_dir = entry_map[raw_name]
                file_path = os.path.join(dir_path, raw_name)
                if is_dir_entry:
                    if is_symlink_dir:
                        continue
                    async for child in _iter_dir(file_path):
                        yield child
                else:
                    try:
                        stat_result = await aiofiles.os.stat(file_path)
                    except OSError:
                        continue
                    yield FileEntry(
                        name=entry.name,
                        path=file_path,
                        stat=StatResult(
                            st_size=stat_result.st_size,
                            st_ctime=stat_result.st_ctime,
                            st_mtime=stat_result.st_mtime,
                            isdir=stat.S_ISDIR(stat_result.st_mode),
                            islnk=stat.S_ISLNK(stat_result.st_mode),
                        ),
                    )

        async def aiterator():
            if await aiofiles.ospath.isfile(path):
                stat_result = await aiofiles.os.stat(path)
                yield FileEntry(
                    name=os.path.basename(path),
                    path=path,
                    stat=StatResult(
                        st_size=stat_result.st_size,
                        st_ctime=stat_result.st_ctime,
                        st_mtime=stat_result.st_mtime,
                        isdir=stat.S_ISDIR(stat_result.st_mode),
                        islnk=stat.S_ISLNK(stat_result.st_mode),
                        extra=stat_result,
                    ),
                )
                return
            async for file_entry in _iter_dir(path):
                yield file_entry

        return AioScannableManager(aiterator())

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """
        Move file or directory.

        :param src_path: Source path to move.
        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        :raises FileExistsError: If overwrite is False and destination exists.
        :return: The destination path
        """
        if not overwrite and await aiofiles.ospath.exists(dst_path):
            raise FileExistsError(f"Destination path already exists: {dst_path}")
        dir_path = os.path.dirname(dst_path)
        if dir_path and dir_path != ".":
            await self.mkdir(dir_path, parents=True, exist_ok=True)

        shutil.move(src_path, dst_path)
        return dst_path

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create a symbolic link pointing to src_path named dst_path.

        :param src_path: Source path the link should reference.
        :param dst_path: The symbolic link path.
        """
        await aiofiles.os.symlink(src_path, dst_path)

    async def readlink(self, path: str) -> str:
        """Return a new path representing the symbolic link's target.

        :param path: Path to the symbolic link.
        :return: Target path of the symbolic link.
        """
        return await aiofiles.os.readlink(path)

    async def is_symlink(self, path: str) -> bool:
        """Return True if the path points to a symbolic link.

        :param path: Path to check.
        :return: True if the path is a symbolic link, otherwise False.
        """
        return await aiofiles.ospath.islink(path)

    async def absolute(self, path: str) -> str:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object.

        :param path: Path to convert.
        :return: Absolute version of the provided path.
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

    async def copy(self, src_path, dst_path):
        """
        copy single file, not directory

        :param src_path: Given source path
        :param dst_path: Given destination path
        :return: Destination path after copy.
        """
        dir_name = os.path.dirname(dst_path)
        if dir_name and dir_name != "":
            await self.mkdir(dir_name, parents=True, exist_ok=True)
        return await asyncio.to_thread(shutil.copyfile, src_path, dst_path)

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """
        Local filesystem endpoints match when protocols match.

        :param other_filesystem: Filesystem to compare.
        :return: True if both represent the same endpoint.
        """
        if isinstance(other_filesystem, LocalFileSystem):
            return True
        return False

    def parse_uri(self, uri: str) -> str:
        """
        Parse the path part from a URI.

        :param uri: URI string.
        :return: Path part string.
        """
        _, path, _ = split_uri(uri)
        return path

    def build_uri(self, path: str) -> str:
        """
        Build URI for the filesystem by path part.

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
