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
    return await SmartPath(path).exists(followlinks=followlinks)


async def smart_isdir(path: PathLike, *, followlinks: bool = False) -> bool:
    return await SmartPath(path).is_dir(followlinks=followlinks)


async def smart_isfile(path: PathLike, *, followlinks: bool = False) -> bool:
    return await SmartPath(path).is_file(followlinks=followlinks)


async def smart_islink(path: PathLike) -> bool:
    return await SmartPath(path).is_symlink()


async def smart_stat(path: PathLike, *, follow_symlinks: bool = True) -> StatResult:
    return await SmartPath(path).stat(follow_symlinks=follow_symlinks)


async def smart_touch(path: PathLike, exist_ok: bool = True) -> None:
    await SmartPath(path).touch(exist_ok=exist_ok)


async def smart_unlink(path: PathLike, missing_ok: bool = False) -> None:
    await SmartPath(path).unlink(missing_ok=missing_ok)


async def smart_makedirs(
    path: PathLike, *, mode: int = 0o777, exist_ok: bool = False
) -> None:
    await SmartPath(path).mkdir(mode=mode, parents=True, exist_ok=exist_ok)


def smart_open(
    path: PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: T.Optional[str] = None,
    errors: T.Optional[str] = None,
    newline: T.Optional[str] = None,
) -> T.AsyncContextManager:
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
    path_obj = SmartPath(path)
    return path_obj.filesystem.scandir(path_obj._path)


async def smart_listdir(path: PathLike) -> T.List[str]:
    smart_path = SmartPath(path)
    names = []
    async for entry in smart_path.iterdir():
        names.append(entry.name)
    return names


async def smart_path_join(path: PathLike, *paths: PathLike) -> str:
    result = SmartPath(path)
    for part in paths:
        result = result / part
    return str(result)


async def smart_copy(
    src_path: PathLike, dst_path: PathLike, *, follow_symlinks: bool = False
) -> str:
    result = await SmartPath(src_path).copy(dst_path, follow_symlinks=follow_symlinks)
    return str(result)


async def smart_move(src_path: PathLike, dst_path: PathLike) -> str:
    result = await SmartPath(src_path).move(dst_path)
    return str(result)


async def smart_rename(src_path: PathLike, dst_path: PathLike) -> str:
    result = await SmartPath(src_path).rename(dst_path)
    return str(result)


async def smart_walk(
    path: PathLike, *, follow_symlinks: bool = False
) -> T.AsyncIterator[T.Tuple[str, T.List[str], T.List[str]]]:
    async for item in SmartPath(path).walk(follow_symlinks=follow_symlinks):
        yield item


async def smart_glob(path: PathLike, *, recursive: bool = True) -> T.List[str]:
    results = await SmartPath(path).glob("", recursive=recursive)
    return [str(item) for item in results]


async def smart_iglob(
    path: PathLike, *, recursive: bool = True
) -> T.AsyncIterator[str]:
    async for item in SmartPath(path).iglob("", recursive=recursive):
        yield str(item)


async def smart_realpath(path: PathLike, *, strict: bool = False) -> str:
    result = await SmartPath(path).resolve(strict=strict)
    return str(result)


async def smart_relpath(path: PathLike, start: PathLike) -> str:
    return await SmartPath(path).relative_to(start)


async def smart_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    await SmartPath(dst_path).symlink_to(src_path)


async def smart_readlink(path: PathLike) -> str:
    result = await SmartPath(path).readlink()
    return str(result)
