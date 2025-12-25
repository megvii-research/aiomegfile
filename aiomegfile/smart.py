import os
import typing as T

from aiomegfile.interfaces import FileEntry, StatResult
from aiomegfile.smart_path import SmartPath

PathLike = T.Union[str, os.PathLike]

__all__ = [
    "smart_copy",
    "smart_exists",
    "smart_glob",
    "smart_iglob",
    "smart_isdir",
    "smart_isfile",
    "smart_islink",
    "smart_listdir",
    "smart_makedirs",
    "smart_open",
    "smart_path_join",
    "smart_move",
    "smart_rename",
    "smart_scandir",
    "smart_stat",
    "smart_touch",
    "smart_unlink",
    "smart_walk",
    "smart_realpath",
    "smart_relpath",
    "smart_symlink",
    "smart_readlink",
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


async def smart_stat(path: PathLike, *, follow_symlinks: bool = True) -> StatResult:
    """Get the status of the path.

    :param path: Path to stat.
    :param follow_symlinks: Whether to follow symbolic links when resolving.
    :return: StatResult for the path.
    :rtype: StatResult
    """
    return await SmartPath(path).stat(follow_symlinks=follow_symlinks)


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
