import asyncio
import io
import os
import typing as T
from collections import deque

from tqdm import tqdm

from aiomegfile.config import DEFAULT_COPY_BUFFER_SIZE, GLOBAL_MAX_WORKERS
from aiomegfile.interfaces import Access, FileEntry, StatResult
from aiomegfile.lib.cacher import NullCacher, SmartCacher
from aiomegfile.lib.combine_reader import AioCombineReader
from aiomegfile.lib.glob import get_non_glob_dir, has_magic
from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.compare import get_sync_type, is_same_file
from aiomegfile.utils.path import PathLike, copyfileobj, fspath, split_uri

__all__ = [
    "smart_abspath",
    "smart_cache",
    "smart_copy",
    "smart_exists",
    "smart_combine_open",
    "smart_getmd5",
    "smart_glob",
    "smart_glob_stat",
    "smart_iglob",
    "smart_isabs",
    "smart_isdir",
    "smart_isfile",
    "smart_islink",
    "smart_listdir",
    "smart_load_content",
    "smart_load_from",
    "smart_load_text",
    "smart_makedirs",
    "smart_open",
    "smart_path_join",
    "smart_move",
    "smart_rename",
    "smart_scandir",
    "smart_scan",
    "smart_scan_stat",
    "smart_save_as",
    "smart_save_content",
    "smart_save_text",
    "smart_stat",
    "smart_sync",
    "smart_sync_with_progress",
    "smart_touch",
    "smart_unlink",
    "smart_remove",
    "smart_walk",
    "smart_realpath",
    "smart_relpath",
    "smart_symlink",
    "smart_readlink",
    "smart_concat",
    "SmartCacher",
]


async def smart_exists(path: PathLike, *, followlinks: bool = True) -> bool:
    """Return whether the path points to an existing file or directory.

    :param path: Path to check.
    :param followlinks: Whether to follow symbolic links.
    :return: True if the path exists, otherwise False.
    """
    return await SmartPath(path).exists(followlinks=followlinks)


async def smart_isdir(path: PathLike, *, followlinks: bool = True) -> bool:
    """Return True if the path points to a directory.

    :param path: Path to check.
    :param followlinks: Whether to follow symbolic links.
    :return: True if the path is a directory, otherwise False.
    """
    return await SmartPath(path).is_dir(followlinks=followlinks)


async def smart_isfile(path: PathLike, *, followlinks: bool = True) -> bool:
    """Return True if the path points to a regular file.

    :param path: Path to check.
    :param followlinks: Whether to follow symbolic links.
    :return: True if the path is a regular file, otherwise False.
    """
    return await SmartPath(path).is_file(followlinks=followlinks)


async def smart_islink(path: PathLike) -> bool:
    """Return True if the path points to a symbolic link.

    :param path: Path to check.
    :return: True if the path is a symlink, otherwise False.
    """
    return await SmartPath(path).is_symlink()


async def smart_stat(path: PathLike, *, follow_symlinks: bool = True) -> StatResult:
    """Get the status of the path.

    :param path: Path to stat.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: StatResult for the path.
    :rtype: StatResult
    """
    return await SmartPath(path).stat(follow_symlinks=follow_symlinks)


async def smart_lstat(path: PathLike) -> StatResult:
    """Get the status of the path, not following symbolic links.

    :param path: Path to stat.
    :return: StatResult for the path.
    :rtype: StatResult
    """
    return await SmartPath(path).lstat()


async def smart_getsize(path: PathLike, *, follow_symlinks: bool = True) -> int:
    """Return the size of the file at the given path.

    :param path: Path to the file.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: Size of the file in bytes.
    :rtype: int
    """
    stat_result = await smart_stat(path, follow_symlinks=follow_symlinks)
    return stat_result.st_size


async def smart_getmtime(path: PathLike, *, follow_symlinks: bool = True) -> float:
    """Return the last modification time of the file at the given path.

    :param path: Path to the file.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: Last modification time in seconds since the epoch.
    :rtype: float
    """
    stat_result = await smart_stat(path, follow_symlinks=follow_symlinks)
    return stat_result.st_mtime


async def smart_touch(path: PathLike, exist_ok: bool = True) -> None:
    """Create the file if missing, optionally raising on existence.

    :param path: Path to create.
    :param exist_ok: Whether to skip raising if the file already exists.
    """
    await SmartPath(path).touch(exist_ok=exist_ok)


async def smart_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """Remove (delete) the file.

    :param path: Path to remove.
    :param missing_ok: If False, raise when the path does not exist.
    :raises FileNotFoundError: When missing_ok is False and the file is absent.
    :raises IsADirectoryError: If the target is a directory.
    """
    await SmartPath(path).unlink(missing_ok=missing_ok)


async def smart_remove(path: PathLike, missing_ok: bool = False) -> None:
    """Remove (delete) the file or directory.

    :param path: Path to remove.
    :param missing_ok: If False, raise when the path does not exist.
    :raises FileNotFoundError: When missing_ok is False and the path is absent.
    """
    path_obj = SmartPath(path)
    await path_obj.filesystem.remove(path_obj._path, missing_ok=missing_ok)


async def smart_makedirs(
    path: PathLike, *, mode: int = 0o777, exist_ok: bool = False
) -> None:
    """Create a directory and any missing parents.

    :param path: Directory path to create.
    :param mode: Permission bits for the new directory.
    :param exist_ok: Whether to ignore if the directory exists.
    :raises FileExistsError: When directory exists and exist_ok is False.
    """
    await SmartPath(path).mkdir(mode=mode, parents=True, exist_ok=exist_ok)


def smart_open(
    path: PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    newline: T.Optional[str] = None,
    **kwargs: T.Any,
) -> T.AsyncContextManager:
    """Open the file with mode.

    :param path: File path to open.
    :param mode: File open mode.
    :param buffering: Buffering policy.
    :param encoding: Text encoding in text mode.
    :param errors: Error handling strategy.
    :param newline: Newline handling policy in text mode.
    :param kwargs: Extra open options for compatibility with megfile.
    :return: Async file context manager.
    :rtype: T.AsyncContextManager
    """
    return SmartPath(path).open(
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        **kwargs,
    )


async def smart_load_from(path: PathLike) -> T.BinaryIO:
    """Read content in binary from the specified path into memory.

    Caller is responsible for closing the returned BinaryIO.

    :param path: Specified path to read.
    :return: BinaryIO containing file content.
    :rtype: T.BinaryIO
    """
    async with smart_open(path, "rb") as f:
        content = await f.read()
    return io.BytesIO(content)


def smart_combine_open(
    path_glob: str, mode: str = "rb", open_func=smart_open
) -> AioCombineReader:
    """Open a unified reader that supports multi-file reading.

    :param path_glob: Path pattern that may contain shell wildcard characters.
    :param mode: Mode to open file, supports 'rb'.
    :param open_func: Callable to open each file.
    :return: Combined async reader.
    :rtype: AioCombineReader
    """
    return AioCombineReader(
        path_glob,
        mode=mode,
        open_func=open_func,
        glob_func=smart_glob,
    )


async def smart_getmd5(
    path: PathLike, recalculate: bool = False, followlinks: bool = False
) -> str:
    """Get the MD5 value of a file or directory.

    :param path: File path to compute MD5.
    :param recalculate: Calculate MD5 in real-time or return cached value when
        supported by the filesystem.
    :param followlinks: If True, follow symbolic links when calculating MD5.
    :return: MD5 hex digest.
    :rtype: str
    """
    return await SmartPath(path).md5(
        recalculate=recalculate,
        followlinks=followlinks,
    )


async def smart_load_content(
    path: PathLike, start: T.Optional[int] = None, stop: T.Optional[int] = None
) -> bytes:
    """Get specified file range in bytes.

    :param path: Specified path.
    :param start: Start index.
    :param stop: Stop index (exclusive).
    :return: Bytes content in range ``[start, stop)``.
    :rtype: bytes
    :raises ValueError: If stop is less than start.
    """
    async with smart_open(path, "rb") as f:
        if start is not None:
            await f.seek(start)
        offset = -1
        if stop is not None:
            offset = stop - (start or 0)
            if offset < 0:
                raise ValueError("stop should be greater than start")
        return await f.read(offset)


def smart_scandir(
    path: PathLike,
) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
    """Return an async context manager for iterating directory entries.

    :param path: Directory path to scan.
    :return: Async context manager producing FileEntry items.
    :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
    """
    path_obj = SmartPath(path)
    return path_obj.filesystem.scandir(path_obj._path)


async def smart_listdir(path: PathLike) -> T.List[str]:
    """Return names of entries in the given directory.

    :param path: Directory path to list.
    :return: List of entry names.
    :rtype: T.List[str]
    """
    smart_path = SmartPath(path)
    names = []
    async for entry in smart_path.iterdir():
        names.append(entry.name)
    return names


async def smart_scan(
    path: PathLike,
    *,
    missing_ok: bool = True,
    followlinks: bool = False,
    sort: bool = False,
) -> T.AsyncIterator[str]:
    """Iteratively traverse only files in the given path.

    If the path is a file, yields that file only.

    :param path: Given path.
    :param missing_ok: If False and the path is missing, raise FileNotFoundError.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator of file paths.
    :rtype: T.AsyncIterator[str]
    """
    async for file_path in SmartPath(path).scan(
        missing_ok=missing_ok,
        followlinks=followlinks,
        sort=sort,
    ):
        yield file_path


async def smart_scan_stat(
    path: PathLike,
    *,
    missing_ok: bool = True,
    followlinks: bool = False,
    sort: bool = False,
) -> T.AsyncIterator[FileEntry]:
    """Iteratively traverse only files in the given path with stats.

    If the path is a file, yields that file entry only.

    :param path: Given path.
    :param missing_ok: If False and the path is missing, raise FileNotFoundError.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator of FileEntry objects.
    :rtype: T.AsyncIterator[FileEntry]
    :raises FileNotFoundError: If no matches and missing_ok is False.
    """
    async for entry in SmartPath(path).scan_stat(
        missing_ok=missing_ok,
        followlinks=followlinks,
        sort=sort,
    ):
        yield entry


async def smart_path_join(path: PathLike, *paths: PathLike) -> str:
    """Join path components and return the combined path string.

    :param path: Base path.
    :param paths: Additional path components to join.
    :return: Combined path string.
    :rtype: str
    """
    result = SmartPath(path)
    for part in paths:
        result = result / part
    return str(result)


async def smart_abspath(path: PathLike) -> str:
    """Return the absolute path of the given path.

    :param path: Given path.
    :return: Absolute path string.
    :rtype: str
    """
    result = await SmartPath(path).absolute()
    return str(result)


async def smart_isabs(path: PathLike) -> bool:
    """Test whether a path is absolute.

    :param path: Given path.
    :return: True if a path is absolute, else False.
    :rtype: bool
    """
    path_str = fspath(path)
    if "://" in path_str:
        return True
    return os.path.isabs(path_str)


async def smart_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    followlinks: bool = True,
) -> str:
    """Copy a file or directory and return the destination path string.

    :param src_path: Source path to copy.
    :param dst_path: Destination path.
    :param callback: Called periodically during copy with bytes written.
    :param followlinks: Whether to follow symbolic links.
    :return: Destination path string.
    :rtype: str
    """
    result = await SmartPath(src_path).copy(
        dst_path,
        follow_symlinks=followlinks,
        callback=callback,
    )
    return str(result)


async def smart_copy_file(
    src_path: PathLike,
    dst_path: PathLike,
    *,
    followlinks: bool = True,
    callback: T.Optional[T.Callable[[int], None]] = None,
) -> str:
    """Copy a file and return the destination path string.

    :param src_path: Source path to copy.
    :param dst_path: Destination path.
    :param followlinks: Whether to follow symbolic links.
    :param callback: Called periodically during copy with bytes written.
    :return: Destination path string.
    :rtype: str
    """
    if followlinks:
        try:
            src_path = await SmartPath(src_path).readlink()
        except OSError:
            pass

    result = await SmartPath(src_path).copy_file(dst_path, callback=callback)
    return str(result)


async def smart_move(src_path: PathLike, dst_path: PathLike) -> str:
    """Move a file or directory and return the destination path string.

    :param src_path: Source path to move.
    :param dst_path: Destination path.
    :return: Destination path string.
    :rtype: str
    """
    result = await SmartPath(src_path).move(dst_path)
    return str(result)


async def smart_rename(src_path: PathLike, dst_path: PathLike) -> str:
    """Rename a file or directory and return the destination path string.

    :param src_path: Source path to rename.
    :param dst_path: Destination path.
    :return: Destination path string.
    :rtype: str
    """
    result = await SmartPath(src_path).rename(dst_path)
    return str(result)


async def smart_walk(
    path: PathLike, *, followlinks: bool = False
) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
    """Generate the file names in a directory tree by walking the tree.

    :param path: Root directory to walk.
    :param followlinks: Whether to traverse symbolic links to directories.
    :return: Async iterator of (root, dirs, files).
    :rtype: T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]
    """
    async for item in SmartPath(path).walk(follow_symlinks=followlinks):
        yield item


async def smart_glob(
    path: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
    *,
    sort: bool = False,
) -> T.List[str]:
    """Return paths whose paths match the glob pattern.

    :param path: Base path to search under.
    :param recursive: If False, ``**`` will not search directory recursively.
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: List of matching path strings.
    :rtype: T.List[str]
    :raises FileNotFoundError: If no matches and missing_ok is False.
    """
    smart_path = SmartPath(path)
    results = await smart_path.glob(
        "", recursive=recursive, missing_ok=missing_ok, sort=sort
    )
    return [str(item) for item in results]


async def smart_glob_stat(
    pathname: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
    *,
    sort: bool = False,
) -> T.AsyncIterator[FileEntry]:
    """Return entries whose paths match the glob pattern.

    :param pathname: Path pattern that may contain shell wildcard characters.
    :param recursive: If False, ``**`` will not search directory recursively.
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator of FileEntry objects.
    :rtype: T.AsyncIterator[FileEntry]
    :raises FileNotFoundError: If no matches and missing_ok is False.
    """
    async for entry in SmartPath(pathname).glob_stat(
        pattern="",
        recursive=recursive,
        missing_ok=missing_ok,
        sort=sort,
    ):
        yield entry


async def smart_iglob(
    path: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
    *,
    sort: bool = False,
) -> T.AsyncIterator[str]:
    """Yield paths whose paths match the glob pattern.

    :param path: Base path to search under.
    :param recursive: If False, ``**`` will not search directory recursively.
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator of matching path strings.
    :rtype: T.AsyncIterator[str]
    """
    async for item in SmartPath(path).iglob(
        "", recursive=recursive, missing_ok=missing_ok, sort=sort
    ):
        yield str(item)


async def smart_realpath(path: PathLike, *, strict: bool = False) -> str:
    """Resolve symlinks and return the absolute path string.

    :param path: Path to resolve.
    :param strict: Whether to raise if a symlink points to itself.
    :return: Resolved absolute path string.
    :rtype: str
    :raises OSError: If a symlink points to itself and strict is True.
    """
    result = await SmartPath(path).resolve(strict=strict)
    return str(result)


async def smart_save_as(file_object: T.BinaryIO, path: PathLike) -> None:
    """Write an opened binary stream to the specified path.

    The input stream will not be closed.

    :param file_object: Stream to be read.
    :param path: Target path to save the stream content.
    """
    async with smart_open(path, "wb") as f:
        while True:
            chunk = file_object.read(DEFAULT_COPY_BUFFER_SIZE)
            if not chunk:
                break
            await f.write(chunk)


async def smart_save_content(path: PathLike, content: bytes) -> None:
    """Save bytes content to the specified path.

    :param path: Path to save content.
    :param content: Bytes content to write.
    """
    async with smart_open(path, "wb") as f:
        await f.write(content)


async def smart_load_text(path: PathLike) -> str:
    """Read text content from the specified path.

    :param path: Path to read.
    :return: File content as text.
    :rtype: str
    """
    async with smart_open(path, "r") as f:
        return await f.read()  # pytype: disable=bad-return-type


async def smart_save_text(path: PathLike, text: str) -> None:
    """Save text to the specified path.

    :param path: Path to save text.
    :param text: Text content to write.
    """
    async with smart_open(path, "w") as f:
        await f.write(text)


def smart_cache(
    path: PathLike, cacher: T.Type[SmartCacher] = SmartCacher, **options: T.Any
) -> T.AsyncContextManager[str]:
    """Return an async cacher for non-local paths.

    Examples: ::

        >>> import asyncio
        >>> from aiomegfile import smart_cache
        >>> async def main():
        ...     async with smart_cache(
        ...         "s3://mybucket/myfile.mp4",
        ...         mode="r",
        ...     ) as cache_path:
        ...         print(cache_path)
        >>> asyncio.run(main())

    :param path: Path to cache.
    :param cacher: Cacher class for non-local paths.
    :param options: Optional arguments for cacher.
    :return: Async cacher instance.
    :rtype: T.AsyncContextManager[str]
    """
    path_str = fspath(path)
    protocol, _, _ = split_uri(path_str)
    if protocol == "file":
        return NullCacher(path_str)
    return cacher(path_str, **options)


async def smart_relpath(path: PathLike, start: PathLike) -> str:
    """Compute a relative path from start to path.

    :param path: Target path.
    :param start: Base path to compute the relative path against.
    :return: Relative path string.
    :rtype: str
    :raises ValueError: If path is not under the given start path.
    """
    return await SmartPath(path).relative_to(start)


async def smart_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    """Create a symbolic link at dst_path pointing to src_path.

    :param src_path: Target path the link should point to.
    :param dst_path: Path of the symlink to create.
    :raises TypeError: If src_path and dst_path are on different filesystems.
    """
    await SmartPath(dst_path).symlink_to(src_path)


async def smart_readlink(path: PathLike) -> str:
    """Return the target path string of a symbolic link.

    :param path: Path to the symbolic link.
    :return: Target path string.
    :rtype: str
    """
    result = await SmartPath(path).readlink()
    return str(result)


def _get_sync_root_path(src_path: PathLike) -> str:
    """Return the root path used to compute relative sync destinations.

    :param src_path: Source path or glob pattern.
    :return: Root path for sync path calculations.
    :rtype: str
    """
    src_path_str = fspath(src_path)
    if has_magic(src_path_str):
        return str(SmartPath(get_non_glob_dir(src_path_str)))
    return str(SmartPath(src_path))


def _can_use_fast_sync(src_protocol: str, dst_protocol: str) -> bool:
    """Return whether fast merge-based sync is supported for both ends.

    :param src_protocol: Source filesystem protocol.
    :param dst_protocol: Destination filesystem protocol.
    :return: True when both protocols support fast sync.
    :rtype: bool
    """
    supported_protocols = {"file", "s3"}
    return src_protocol in supported_protocols and dst_protocol in supported_protocols


async def _iter_file_stats(
    path: PathLike,
    *,
    missing_ok: bool = True,
    followlinks: bool = False,
    sort: bool = False,
) -> T.AsyncIterator[FileEntry]:
    """Iterate file entries with stats under the given path.

    :param path: Root path to scan.
    :param missing_ok: If False and path is missing, raise FileNotFoundError.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator of FileEntry objects.
    :rtype: T.AsyncIterator[FileEntry]
    :raises FileNotFoundError: If missing_ok is False and path is absent.
    """
    smart_path = SmartPath(path)
    if not await smart_path.exists(followlinks=followlinks):
        if missing_ok:
            return
        raise FileNotFoundError(f"No match file: {smart_path}")
    if followlinks:
        try:
            smart_path = await smart_path.readlink()
        except OSError:
            pass

    async with smart_path.filesystem.scanfile(smart_path._path, sort=sort) as iterator:
        max_workers = max(GLOBAL_MAX_WORKERS, 1)
        semaphore = asyncio.Semaphore(max_workers)
        max_in_flight = max_workers * 2
        pending: deque[asyncio.Task[FileEntry]] = deque()

        async def _resolve_entry(entry: FileEntry) -> FileEntry:
            """Resolve entry details, following symlinks if requested.

            :param entry: File entry to resolve.
            :return: Resolved FileEntry with updated path and stat.
            """
            if followlinks and entry.is_symlink():
                async with semaphore:
                    resolved_path = await smart_path.filesystem.readlink(entry.path)
                    resolved_name = os.path.basename(resolved_path)
                    resolved_stat = await smart_path.filesystem.stat(
                        resolved_path, followlinks=followlinks
                    )
                return FileEntry(
                    name=resolved_name,
                    path=smart_path.filesystem.build_uri(resolved_path),
                    stat=resolved_stat,
                )
            return FileEntry(
                name=entry.name,
                path=smart_path.filesystem.build_uri(entry.path),
                stat=entry.stat,
            )

        async def _await_next(
            tasks: deque[asyncio.Task[FileEntry]],
        ) -> FileEntry:
            """Await the next pending task and cancel on failure.

            :param tasks: Queue of pending tasks.
            :return: Resolved FileEntry.
            """
            task = tasks.popleft()
            try:
                return await task
            except Exception:
                for pending_task in tasks:
                    pending_task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

        async for entry in iterator:
            pending.append(asyncio.create_task(_resolve_entry(entry)))
            if len(pending) >= max_in_flight:
                yield await _await_next(pending)

        while pending:
            yield await _await_next(pending)


async def _iter_sync_entries(
    path: PathLike,
    *,
    followlinks: bool = False,
    sort: bool = False,
) -> T.AsyncIterator[T.Tuple[str, FileEntry]]:
    """Iterate file entries with comparison keys for sync.

    :param path: Root path to scan.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator yielding ``(key, FileEntry)`` tuples.
    :rtype: T.AsyncIterator[T.Tuple[str, FileEntry]]
    """

    async for entry in _iter_file_stats(
        path,
        missing_ok=True,
        followlinks=followlinks,
        sort=sort,
    ):
        if not entry.name:
            continue
        content_path = await smart_relpath(entry.path, start=path)
        if content_path and content_path != ".":
            key = content_path.lstrip("/")
        else:
            key = ""
        yield key, entry


async def _iter_sync_entries_from_iter(
    root_path: PathLike,
    entries: T.AsyncIterator[FileEntry],
) -> T.AsyncIterator[T.Tuple[str, FileEntry]]:
    """Iterate file entries from an iterator with comparison keys.

    :param root_path: Root path to compute relative keys.
    :param entries: Async iterator of FileEntry objects.
    :return: Async iterator yielding ``(key, FileEntry)`` tuples.
    :rtype: T.AsyncIterator[T.Tuple[str, FileEntry]]
    """
    async for entry in entries:
        if not entry.name:
            continue
        content_path = await smart_relpath(entry.path, start=root_path)
        if content_path and content_path != ".":
            key = content_path.lstrip("/")
        else:
            key = ""
        yield key, entry


async def _iter_glob_file_stats(
    pattern: PathLike,
    *,
    followlinks: bool = False,
    sort: bool = False,
) -> T.AsyncIterator[FileEntry]:
    """Iterate file entries that match a glob pattern.

    :param pattern: Glob pattern for source files.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :return: Async iterator yielding FileEntry objects.
    :rtype: T.AsyncIterator[FileEntry]
    """
    async for entry in smart_glob_stat(
        pattern,
        recursive=True,
        missing_ok=False,
        sort=sort,
    ):
        if followlinks and entry.is_symlink():
            resolved_stat: T.Optional[StatResult] = None
            try:
                resolved = await SmartPath(entry.path).readlink()
                resolved_stat = await resolved.stat(follow_symlinks=followlinks)
            except OSError:
                resolved = None
            if resolved is not None and resolved_stat is not None:
                if resolved_stat.is_dir():
                    async for child in smart_scan_stat(
                        str(resolved),
                        followlinks=followlinks,
                        sort=sort,
                    ):
                        yield child
                    continue
                entry = FileEntry(
                    name=resolved.name,
                    path=str(resolved),
                    stat=resolved_stat,
                )
        if entry.is_file():
            yield entry
            continue
        async for child in smart_scan_stat(
            entry.path,
            followlinks=followlinks,
            sort=sort,
        ):
            yield child


def _get_sync_source(
    src_path: PathLike,
    *,
    followlinks: bool = False,
    sort: bool = False,
    on_entry: T.Optional[T.Callable[[FileEntry], None]] = None,
    on_done: T.Optional[T.Callable[[], None]] = None,
) -> T.Tuple[str, T.AsyncIterator[T.Tuple[str, FileEntry]]]:
    """Build the source iterator and root path for sync.

    :param src_path: Source path or glob pattern.
    :param followlinks: Whether to follow symbolic links.
    :param sort: Whether to request sorted traversal when supported by the
        filesystem.
    :param on_entry: Optional callback invoked for each FileEntry.
    :param on_done: Optional callback invoked when iteration completes.
    :return: Tuple of root path and async iterator of ``(key, FileEntry)``.
    :rtype: T.Tuple[str, T.AsyncIterator[T.Tuple[str, FileEntry]]]
    """
    src_path_str = fspath(src_path)
    src_root_path = _get_sync_root_path(src_path)
    if has_magic(src_path_str):
        entry_iter = _iter_glob_file_stats(
            src_path_str,
            followlinks=followlinks,
            sort=sort,
        )
    else:
        entry_iter = _iter_file_stats(
            src_root_path,
            missing_ok=False,
            followlinks=followlinks,
            sort=sort,
        )

    async def _iterator() -> T.AsyncIterator[T.Tuple[str, FileEntry]]:
        try:
            async for key, entry in _iter_sync_entries_from_iter(
                src_root_path, entry_iter
            ):
                if on_entry:
                    on_entry(entry)
                yield key, entry
        finally:
            if on_done:
                on_done()

    return src_root_path, _iterator()


async def _drain_copy_tasks(
    tasks: set[asyncio.Task[None]],
) -> set[asyncio.Task[None]]:
    """Wait for at least one copy task and propagate errors.

    :param tasks: Active copy tasks.
    :return: Remaining pending tasks.
    """
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for completed_task in done:
        try:
            await completed_task
        except Exception:
            for pending_task in pending:
                pending_task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise
    return set(pending)


async def _run_sync_fast(
    src_iter: T.AsyncIterator[T.Tuple[str, FileEntry]],
    dst_iter: T.Optional[T.AsyncIterator[T.Tuple[str, FileEntry]]],
    dst_root_path: str,
    *,
    followlinks: bool,
    callback: T.Optional[T.Callable[[str, int], None]],
    callback_after_copy_file: T.Optional[T.Callable[[str, str], None]],
    force: bool,
    worker: int,
    sync_type: str,
) -> None:
    """Run the fast merge-based sync loop using prepared iterators.

    :param src_iter: Async iterator over source entries.
    :param dst_iter: Async iterator over destination entries, or None if missing.
    :param dst_root_path: Destination root path.
    :param followlinks: Whether to follow symbolic links.
    :param callback: Callback for copied bytes.
    :param callback_after_copy_file: Callback after each source file is processed,
        including files skipped because the destination is already up to date.
    :param force: Whether to force copy even if files are the same.
    :param worker: Maximum number of concurrent workers for copy tasks.
    :param sync_type: Sync type for file comparison.
    """
    dst_done = dst_iter is None
    src_done = False
    src_take = True
    dst_take = True
    src_item: T.Optional[T.Tuple[str, FileEntry]] = None
    dst_item: T.Optional[T.Tuple[str, FileEntry]] = None
    max_workers = worker if worker > 0 else GLOBAL_MAX_WORKERS
    max_workers = max(max_workers, 1)
    semaphore = asyncio.Semaphore(max_workers)
    max_in_flight = max_workers * 2
    copy_tasks: set[asyncio.Task[None]] = set()

    async def _copy_and_callback(src_file: str, dst_file: str) -> None:
        """Copy a file and invoke post-copy callback when provided.

        :param src_file: Source file path.
        :param dst_file: Destination file path.
        """
        async with semaphore:
            wrapped_callback = None
            if callback:

                def wrapped_callback(length: int) -> None:
                    """Invoke copy callback with source path."""
                    callback(src_file, length)  # pyre-ignore[29]

            await smart_copy_file(
                src_file,
                dst_file,
                followlinks=followlinks,
                callback=wrapped_callback,
            )
            if callback_after_copy_file:
                callback_after_copy_file(src_file, dst_file)

    error: T.Optional[Exception] = None
    try:
        while True:
            if src_take and not src_done:
                try:
                    src_item = await anext(src_iter)
                except StopAsyncIteration:
                    src_done = True
                    src_item = None
            if dst_take and not dst_done:
                if dst_iter is None:
                    dst_done = True
                    dst_item = None
                else:
                    try:
                        dst_item = await anext(dst_iter)
                    except StopAsyncIteration:
                        dst_done = True
                        dst_item = None

            if src_done or src_item is None:
                break

            src_key, src_entry = src_item
            if src_key:
                dst_abs_file_path = await smart_path_join(dst_root_path, src_key)
            else:
                dst_abs_file_path = dst_root_path

            if dst_done:
                copy_tasks.add(
                    asyncio.create_task(
                        _copy_and_callback(src_entry.path, dst_abs_file_path)
                    )
                )
                if len(copy_tasks) >= max_in_flight:
                    copy_tasks = await _drain_copy_tasks(copy_tasks)
                src_take = True
                dst_take = False
                continue

            if dst_item is None:
                dst_done = True
                src_take = False
                dst_take = True
                continue

            dst_key, dst_entry = dst_item
            if src_key == dst_key:
                should_sync = True
                if not force:
                    if is_same_file(src_entry.stat, dst_entry.stat, sync_type):
                        should_sync = False
                if should_sync:
                    copy_tasks.add(
                        asyncio.create_task(
                            _copy_and_callback(src_entry.path, dst_abs_file_path)
                        )
                    )
                    if len(copy_tasks) >= max_in_flight:
                        copy_tasks = await _drain_copy_tasks(copy_tasks)
                else:
                    if callback:
                        callback(src_entry.path, src_entry.stat.st_size)
                    if callback_after_copy_file:
                        callback_after_copy_file(src_entry.path, dst_abs_file_path)
                src_take = True
                dst_take = True
            elif src_key < dst_key:
                copy_tasks.add(
                    asyncio.create_task(
                        _copy_and_callback(src_entry.path, dst_abs_file_path)
                    )
                )
                if len(copy_tasks) >= max_in_flight:
                    copy_tasks = await _drain_copy_tasks(copy_tasks)
                src_take = True
                dst_take = False
            else:
                src_take = False
                dst_take = True
    except Exception as exc:
        error = exc
        raise
    finally:
        if copy_tasks:
            if error is not None:
                for pending_task in copy_tasks:
                    pending_task.cancel()
            results = await asyncio.gather(*copy_tasks, return_exceptions=True)
            if error is None:
                for result in results:
                    if isinstance(result, Exception) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        raise result


async def _run_sync(
    src_iter: T.AsyncIterator[T.Tuple[str, FileEntry]],
    dst_root_path: str,
    *,
    followlinks: bool,
    callback: T.Optional[T.Callable[[str, int], None]],
    callback_after_copy_file: T.Optional[T.Callable[[str, str], None]],
    force: bool,
    worker: int,
    sync_type: str,
) -> None:
    """Run sync by checking destination files one by one.

    This path mirrors megfile's ``smart_sync`` strategy and does not require
    source and destination iterators to be sorted.

    :param src_iter: Async iterator over source entries.
    :param dst_root_path: Destination root path.
    :param followlinks: Whether to follow symbolic links.
    :param callback: Callback for copied bytes.
    :param callback_after_copy_file: Callback after each source file is processed,
        including files skipped because the destination is already up to date.
    :param force: Whether to force copy even if files are the same.
    :param worker: Maximum number of concurrent workers for copy tasks.
    :param sync_type: Sync type for file comparison.
    """
    max_workers = worker if worker > 0 else GLOBAL_MAX_WORKERS
    max_workers = max(max_workers, 1)
    semaphore = asyncio.Semaphore(max_workers)
    max_in_flight = max_workers * 2
    copy_tasks: set[asyncio.Task[None]] = set()

    async def _sync_single_entry(src_key: str, src_entry: FileEntry) -> None:
        """Sync a single source entry to its destination.

        :param src_key: Relative key for the source entry.
        :param src_entry: Source file entry.
        """
        if src_key:
            dst_abs_file_path = await smart_path_join(dst_root_path, src_key)
        else:
            dst_abs_file_path = dst_root_path

        async with semaphore:
            should_sync = True
            if not force:
                try:
                    dst_stat = await smart_stat(
                        dst_abs_file_path, follow_symlinks=followlinks
                    )
                except (FileNotFoundError, NotImplementedError):
                    pass
                else:
                    if is_same_file(src_entry.stat, dst_stat, sync_type):
                        should_sync = False

            if should_sync:
                wrapped_callback = None
                if callback:

                    def wrapped_callback(length: int) -> None:
                        """Invoke copy callback with source path."""
                        callback(src_entry.path, length)  # pyre-ignore[29]

                await smart_copy_file(
                    src_entry.path,
                    dst_abs_file_path,
                    followlinks=followlinks,
                    callback=wrapped_callback,
                )
                if callback_after_copy_file:
                    callback_after_copy_file(src_entry.path, dst_abs_file_path)
            else:
                if callback:
                    callback(src_entry.path, src_entry.stat.st_size)
                if callback_after_copy_file:
                    callback_after_copy_file(src_entry.path, dst_abs_file_path)

    error: T.Optional[Exception] = None
    try:
        async for src_key, src_entry in src_iter:
            copy_tasks.add(asyncio.create_task(_sync_single_entry(src_key, src_entry)))
            if len(copy_tasks) >= max_in_flight:
                copy_tasks = await _drain_copy_tasks(copy_tasks)
    except Exception as exc:
        error = exc
        raise
    finally:
        if copy_tasks:
            if error is not None:
                for pending_task in copy_tasks:
                    pending_task.cancel()
            results = await asyncio.gather(*copy_tasks, return_exceptions=True)
            if error is None:
                for result in results:
                    if isinstance(result, Exception) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        raise result


async def smart_sync(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[str, int], None]] = None,
    followlinks: bool = True,
    callback_after_copy_file: T.Optional[T.Callable[[str, str], None]] = None,
    force: bool = False,
    *,
    worker: int = -1,
) -> None:
    """Sync file or directory to the destination path.

    .. note ::

        When the parameter is file, this function behaves like ``smart_copy``.

        If file and directory of same name and same level, sync considers it as file
        first.

    :param src_path: Given source path.
    :param dst_path: Given destination path.
    :param callback: Called periodically during copy with source path and bytes
        written.
    :param followlinks: Whether to follow symbolic links.
    :param callback_after_copy_file: Called after each source file is processed,
        including files skipped because the destination is already up to date. The
        input parameters are src file path and dst file path.
    :param worker: Maximum number of concurrent workers for copy tasks.
    :param force: Sync file forcible, do not ignore same files.
    :raises FileNotFoundError: If source path does not exist.
    """
    src_path_str = fspath(src_path)
    if not has_magic(src_path_str) and not await smart_exists(
        src_path, followlinks=followlinks
    ):
        raise FileNotFoundError(f"No match file: {src_path}")

    src_root_path = _get_sync_root_path(src_path)
    dst_root_path = str(SmartPath(dst_path))

    src_protocol = SmartPath(src_root_path).filesystem.protocol
    dst_protocol = SmartPath(dst_root_path).filesystem.protocol
    sync_type = get_sync_type(src_protocol, dst_protocol)

    dst_missing = not await smart_exists(dst_path, followlinks=followlinks)
    if dst_missing:
        force = True
        use_fast_sync = False
        dst_iter = None
    elif force:
        use_fast_sync = False
        dst_iter = None
    else:
        use_fast_sync = _can_use_fast_sync(src_protocol, dst_protocol)
        dst_iter = _iter_sync_entries(
            dst_root_path,
            followlinks=followlinks,
            sort=use_fast_sync,
        )

    src_root_path, src_iter = _get_sync_source(
        src_path,
        followlinks=followlinks,
        sort=use_fast_sync,
    )

    if use_fast_sync:
        await _run_sync_fast(
            src_iter,
            dst_iter,
            dst_root_path,
            followlinks=followlinks,
            callback=callback,
            callback_after_copy_file=callback_after_copy_file,
            force=force,
            worker=worker,
            sync_type=sync_type,
        )
        return

    await _run_sync(
        src_iter,
        dst_root_path,
        followlinks=followlinks,
        callback=callback,
        callback_after_copy_file=callback_after_copy_file,
        force=force,
        worker=worker,
        sync_type=sync_type,
    )


async def smart_sync_with_progress(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[str, int], None]] = None,
    followlinks: bool = True,
    force: bool = False,
    *,
    callback_after_copy_file: T.Optional[T.Callable[[str, str], None]] = None,
    worker: int = -1,
) -> None:
    """Sync file or directory with progress bars.

    :param src_path: Given source path.
    :param dst_path: Given destination path.
    :param callback: Called periodically during copy with source path and bytes
        written.
    :param followlinks: Whether to follow symbolic links.
    :param force: Sync file forcible, do not ignore same files.
    :param callback_after_copy_file: Called after each source file is processed,
        including files skipped because the destination is already up to date. The
        input parameters are src file path and dst file path.
    :param worker: Maximum number of concurrent workers for copy tasks.
    :raises FileNotFoundError: If source path does not exist.
    """
    src_path_str = fspath(src_path)
    if not has_magic(src_path_str) and not await smart_exists(
        src_path, followlinks=followlinks
    ):
        raise FileNotFoundError(f"No match file: {src_path}")

    tbar = tqdm(total=0, ascii=True, desc="Files (scanning)")
    sbar = tqdm(
        total=0,
        unit="B",
        ascii=True,
        unit_scale=True,
        unit_divisor=1024,
        desc="Bytes (scanning)",
    )

    def tqdm_callback(src_file_path: str, length: int) -> None:
        """Update progress for copied bytes."""
        sbar.update(length)
        if callback:
            callback(src_file_path, length)

    def on_entry(entry: FileEntry) -> None:
        """Update progress totals while scanning."""
        tbar.total += 1
        sbar.total += entry.stat.st_size
        tbar.refresh()
        sbar.refresh()

    def on_done() -> None:
        """Finalize progress descriptions after scanning."""
        tbar.set_description_str("Files")
        sbar.set_description_str("Bytes")
        tbar.refresh()
        sbar.refresh()

    def tqdm_after_copy_file(src_file_path: str, dst_file_path: str) -> None:
        """Update progress for copied files."""
        tbar.update(1)
        if callback_after_copy_file:
            callback_after_copy_file(src_file_path, dst_file_path)

    try:
        src_root_path = _get_sync_root_path(src_path)
        dst_root_path = str(SmartPath(dst_path))
        src_protocol = SmartPath(src_root_path).filesystem.protocol
        dst_protocol = SmartPath(dst_root_path).filesystem.protocol
        sync_type = get_sync_type(src_protocol, dst_protocol)

        dst_missing = not await smart_exists(dst_path, followlinks=followlinks)
        if dst_missing:
            force = True
            use_fast_sync = False
            dst_iter = None
        elif force:
            use_fast_sync = False
            dst_iter = None
        else:
            use_fast_sync = _can_use_fast_sync(src_protocol, dst_protocol)
            dst_iter = _iter_sync_entries(
                dst_root_path,
                followlinks=followlinks,
                sort=use_fast_sync,
            )

        src_root_path, src_iter = _get_sync_source(
            src_path,
            followlinks=followlinks,
            sort=use_fast_sync,
            on_entry=on_entry,
            on_done=on_done,
        )

        if use_fast_sync:
            await _run_sync_fast(
                src_iter,
                dst_iter,
                dst_root_path,
                followlinks=followlinks,
                callback=tqdm_callback,
                callback_after_copy_file=tqdm_after_copy_file,
                force=force,
                worker=worker,
                sync_type=sync_type,
            )
            return

        await _run_sync(
            src_iter,
            dst_root_path,
            followlinks=followlinks,
            callback=tqdm_callback,
            callback_after_copy_file=tqdm_after_copy_file,
            force=force,
            worker=worker,
            sync_type=sync_type,
        )
    finally:
        tbar.close()
        sbar.close()


async def _default_concat(src_paths: T.List[PathLike], dst_path: PathLike) -> None:
    """Default implementation for concatenating files.

    :param src_paths: List of source file paths to concatenate.
    :param dst_path: Destination path for the concatenated file.
    """
    async with smart_open(dst_path, "wb") as dst_file:
        for src_path in src_paths:
            async with smart_open(src_path, "rb") as src_file:
                await copyfileobj(src_file, dst_file)


async def smart_concat(src_paths: T.List[PathLike], dst_path: PathLike) -> None:
    """Concatenate files in src_paths into a single file at dst_path.

    :param src_paths: List of source file paths to concatenate.
    :param dst_path: Destination path for the concatenated file.
    """
    if not src_paths:
        return

    smart_path = SmartPath(dst_path)

    concat_func = _default_concat
    dst_protocol = smart_path.filesystem.protocol
    for src_path in src_paths:
        src_protocol, _, _ = split_uri(src_path)
        if src_protocol != dst_protocol:
            break
    else:
        if hasattr(smart_path.filesystem, "concat"):
            concat_func = smart_path.filesystem.concat
    await concat_func(src_paths, dst_path)


async def smart_access(path: PathLike, mode: Access = Access.READ) -> bool:
    """Test if path has access permission described by mode.

    :param path: Path to check.
    :param mode: Access mode to check.
    :return: True if the path has the specified access, otherwise False.
    :rtype: bool
    :raises TypeError: If an unsupported access mode is provided.
    """
    return await SmartPath(path).access(mode)
