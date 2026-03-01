import asyncio
import io
import os
import typing as T
from collections import deque

from aiomegfile.config import GLOBAL_MAX_WORKERS
from aiomegfile.interfaces import FileEntry, StatResult
from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.compare import get_sync_type, is_same_file
from aiomegfile.utils.path import PathLike, copyfileobj, fspath, split_uri

__all__ = [
    "smart_abspath",
    "smart_copy",
    "smart_exists",
    "smart_glob",
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
    "smart_save_as",
    "smart_save_content",
    "smart_save_text",
    "smart_stat",
    "smart_sync",
    "smart_touch",
    "smart_unlink",
    "smart_remove",
    "smart_walk",
    "smart_realpath",
    "smart_relpath",
    "smart_symlink",
    "smart_readlink",
    "smart_concat",
]


async def smart_exists(path: PathLike, *, followlinks: bool = False) -> bool:
    """Return whether the path points to an existing file or directory.

    :param path: Path to check.
    :param followlinks: Whether to follow symbolic links.
    :return: True if the path exists, otherwise False.
    """
    return await SmartPath(path).exists(followlinks=followlinks)


async def smart_isdir(path: PathLike, *, followlinks: bool = False) -> bool:
    """Return True if the path points to a directory.

    :param path: Path to check.
    :param followlinks: Whether to follow symbolic links.
    :return: True if the path is a directory, otherwise False.
    """
    return await SmartPath(path).is_dir(followlinks=followlinks)


async def smart_isfile(path: PathLike, *, followlinks: bool = False) -> bool:
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


async def smart_stat(path: PathLike, *, follow_symlinks: bool = False) -> StatResult:
    """Get the status of the path.

    :param path: Path to stat.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: StatResult for the path.
    :rtype: StatResult
    """
    return await SmartPath(path).stat(follow_symlinks=follow_symlinks)


async def smart_getsize(path: PathLike, *, follow_symlinks: bool = False) -> int:
    """Return the size of the file at the given path.

    :param path: Path to the file.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: Size of the file in bytes.
    :rtype: int
    """
    stat_result = await smart_stat(path, follow_symlinks=follow_symlinks)
    return stat_result.st_size


async def smart_getmtime(path: PathLike, *, follow_symlinks: bool = False) -> float:
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
) -> T.AsyncContextManager:
    """Open the file with mode.

    :param path: File path to open.
    :param mode: File open mode.
    :param buffering: Buffering policy.
    :param encoding: Text encoding in text mode.
    :param errors: Error handling strategy.
    :param newline: Newline handling policy in text mode.
    :return: Async file context manager.
    :rtype: T.AsyncContextManager
    """
    return SmartPath(path).open(
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
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
    path: PathLike, *, missing_ok: bool = True, followlinks: bool = False
) -> T.AsyncIterator[str]:
    """Iteratively traverse only files in the given path.

    If the path is a file, yields that file only.

    :param path: Given path.
    :param missing_ok: If False and the path is missing, raise FileNotFoundError.
    :param followlinks: Whether to follow symbolic links.
    :return: Async iterator of file paths.
    :rtype: T.AsyncIterator[str]
    """
    async for entry in _iter_file_stats(
        path, missing_ok=missing_ok, followlinks=followlinks
    ):
        yield entry.path


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
    src_path: PathLike, dst_path: PathLike, *, followlinks: bool = False
) -> str:
    """Copy a file or directory and return the destination path string.

    :param src_path: Source path to copy.
    :param dst_path: Destination path.
    :param followlinks: Whether to follow symbolic links.
    :return: Destination path string.
    :rtype: str
    """
    result = await SmartPath(src_path).copy(dst_path, follow_symlinks=followlinks)
    return str(result)


async def smart_copy_file(
    src_path: PathLike,
    dst_path: PathLike,
    *,
    followlinks: bool = False,
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


async def smart_glob(path: PathLike, *, recursive: bool = True) -> T.List[str]:
    """Return paths whose paths match the glob pattern.

    :param path: Base path to search under.
    :param recursive: If False, ``**`` will not search directory recursively.
    :return: List of matching path strings.
    :rtype: T.List[str]
    """
    results = await SmartPath(path).glob("", recursive=recursive)
    return [str(item) for item in results]


async def smart_iglob(
    path: PathLike, *, recursive: bool = True
) -> T.AsyncIterator[str]:
    """Yield paths whose paths match the glob pattern.

    :param path: Base path to search under.
    :param recursive: If False, ``**`` will not search directory recursively.
    :return: Async iterator of matching path strings.
    :rtype: T.AsyncIterator[str]
    """
    async for item in SmartPath(path).iglob("", recursive=recursive):
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
            chunk = file_object.read(16 * 1024)
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


async def _iter_file_stats(
    path: PathLike,
    *,
    missing_ok: bool = True,
    followlinks: bool = False,
) -> T.AsyncIterator[FileEntry]:
    """Iterate file entries with stats under the given path.

    :param path: Root path to scan.
    :param missing_ok: If False and path is missing, raise FileNotFoundError.
    :param followlinks: Whether to follow symbolic links.
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
            if missing_ok:
                return
            raise

    async with smart_path.filesystem.scanfile(smart_path._path) as iterator:
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
) -> T.AsyncIterator[T.Tuple[str, FileEntry]]:
    """Iterate file entries with comparison keys for sync.

    :param path: Root path to scan.
    :param followlinks: Whether to follow symbolic links.
    :return: Async iterator yielding ``(key, FileEntry)`` tuples.
    :rtype: T.AsyncIterator[T.Tuple[str, FileEntry]]
    """

    async for entry in _iter_file_stats(path, missing_ok=True, followlinks=followlinks):
        if not entry.name:
            continue
        content_path = await smart_relpath(entry.path, start=path)
        if content_path and content_path != ".":
            key = content_path.lstrip("/")
        else:
            key = ""
        yield key, entry


async def smart_sync(
    src_path: PathLike,
    dst_path: PathLike,
    callback: T.Optional[T.Callable[[int], None]] = None,
    callback_after_copy_file: T.Optional[T.Callable[[str, str], None]] = None,
    followlinks: bool = False,
    force: bool = False,
    overwrite: bool = True,
) -> None:
    """Sync file or directory to the destination path.

    .. note ::

        When the parameter is file, this function behaves like ``smart_copy``.

        If file and directory of same name and same level, sync considers it as file
        first.

    :param src_path: Given source path.
    :param dst_path: Given destination path.
    :param callback: Called periodically during copy with bytes written.
    :param callback_after_copy_file: Called after copy success, and the input parameter
        is src file path and dst file path.
    :param followlinks: False if regard symlink as file, else True.
    :param force: Sync file forcible, do not ignore same files, priority is higher than
        ``overwrite``.
    :param overwrite: Whether to overwrite files when they already exist.
    :raises FileNotFoundError: If source path does not exist.
    """
    if not await smart_exists(src_path):
        raise FileNotFoundError(f"No match file: {src_path}")

    src_root_path = str(SmartPath(src_path))
    dst_root_path = str(SmartPath(dst_path))

    src_protocol = SmartPath(src_root_path).filesystem.protocol
    dst_protocol = SmartPath(dst_root_path).filesystem.protocol
    sync_type = get_sync_type(src_protocol, dst_protocol)

    dst_missing = not await smart_exists(dst_path)
    if dst_missing:
        force = True
        dst_iter = None
    else:
        dst_iter = _iter_sync_entries(dst_root_path, followlinks=followlinks)

    src_iter = _iter_sync_entries(src_root_path, followlinks=followlinks)
    src_done = False
    dst_done = dst_missing
    src_take = True
    dst_take = True
    src_item: T.Optional[T.Tuple[str, FileEntry]] = None
    dst_item: T.Optional[T.Tuple[str, FileEntry]] = None
    max_workers = max(GLOBAL_MAX_WORKERS, 1)
    semaphore = asyncio.Semaphore(max_workers)
    max_in_flight = max_workers * 2
    copy_tasks: set[asyncio.Task[None]] = set()

    async def _copy_and_callback(src_file: str, dst_file: str) -> None:
        """Copy a file and invoke post-copy callback when provided.

        :param src_file: Source file path.
        :param dst_file: Destination file path.
        """
        async with semaphore:
            await smart_copy_file(
                src_file,
                dst_file,
                followlinks=followlinks,
                callback=callback,
            )
            if callback_after_copy_file:
                callback_after_copy_file(src_file, dst_file)

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
                    if not overwrite:
                        should_sync = False
                    elif is_same_file(src_entry.stat, dst_entry.stat, sync_type):
                        should_sync = False
                if should_sync:
                    copy_tasks.add(
                        asyncio.create_task(
                            _copy_and_callback(src_entry.path, dst_abs_file_path)
                        )
                    )
                    if len(copy_tasks) >= max_in_flight:
                        copy_tasks = await _drain_copy_tasks(copy_tasks)
                elif callback:
                    callback(src_entry.stat.st_size)
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
