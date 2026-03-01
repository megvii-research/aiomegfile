import asyncio
import glob
import hashlib
import inspect
import logging
import os
import signal
import sys
import time
import typing as T

import click
from click import ParamType
from click.shell_completion import CompletionItem, ZshComplete
from tqdm import tqdm

from aiomegfile.__version__ import __version__
from aiomegfile.config import READER_BLOCK_SIZE
from aiomegfile.interfaces import FILE_SYSTEMS, FileEntry
from aiomegfile.lib.glob import FSFunc, get_non_glob_dir, has_magic, iglob
from aiomegfile.smart import (
    smart_copy,
    smart_exists,
    smart_getmtime,
    smart_getsize,
    smart_isdir,
    smart_makedirs,
    smart_move,
    smart_open,
    smart_readlink,
    smart_relpath,
    smart_remove,
    smart_rename,
    smart_stat,
    smart_sync,
    smart_touch,
    smart_unlink,
)
from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.path import copyfileobj

options: dict[str, T.Any] = {}


def _configure_logging(level: str) -> None:
    """Configure logging with a basic formatter.

    :param level: Logging level name.
    """
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def _run_async(task: T.Coroutine[T.Any, T.Any, T.Any]) -> T.Any:
    """Run a coroutine in a new event loop.

    :param task: Coroutine to execute.
    :return: Awaitable result.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(task)
    raise RuntimeError("aiomegfile CLI cannot run inside an active event loop")


async def _maybe_await(result: T.Any) -> T.Any:
    """Await the result if needed.

    :param result: Possibly awaitable result.
    :return: Final resolved result.
    """
    if inspect.isawaitable(result):
        return await result
    return result


def _get_human_size(num_bytes: int) -> str:
    """Return a human-readable size string.

    :param num_bytes: Size in bytes.
    :return: Human-readable size.
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{int(num_bytes)}B"


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Set logging level.",
)
def cli(debug: bool, log_level: str | None) -> None:
    """Client for aiomegfile."""
    options["debug"] = debug
    options["log_level"] = log_level or ("DEBUG" if debug else "INFO")
    if not debug:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    _configure_logging(options["log_level"])


def safe_cli() -> None:  # pragma: no cover
    """Run CLI and display friendly errors unless debug mode is enabled."""
    try:
        cli()
    except Exception as exc:
        if options.get("debug", False):
            raise
        click.echo(f"\n[{type(exc).__name__}] {exc}", err=True)
        sys.exit(1)


async def _get_echo_path(
    file_stat: FileEntry, base_path: str = "", full: bool = False
) -> str:
    """Return the display path for a file entry.

    :param file_stat: File entry to render.
    :param base_path: Base path for relative rendering.
    :param full: Whether to display full path.
    :return: Display path string.
    """
    if base_path.startswith("file://"):
        base_path = base_path[7:]
    if base_path == file_stat.path:
        return file_stat.name
    if full:
        return file_stat.path
    return await smart_relpath(file_stat.path, start=base_path)


async def _simple_echo(
    file_stat: FileEntry, base_path: str = "", full: bool = False
) -> str:
    """Return the short echo format.

    :param file_stat: File entry to render.
    :param base_path: Base path for relative rendering.
    :param full: Whether to display full path.
    :return: Display line.
    """
    return await _get_echo_path(file_stat, base_path, full)


async def _long_echo(
    file_stat: FileEntry, base_path: str = "", full: bool = False
) -> str:
    """Return the long echo format.

    :param file_stat: File entry to render.
    :param base_path: Base path for relative rendering.
    :param full: Whether to display full path.
    :return: Display line with size and mtime.
    """
    return "%12d %s %s" % (
        file_stat.stat.st_size,
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file_stat.stat.st_mtime)),
        await _get_echo_path(file_stat, base_path, full),
    )


async def _human_echo(
    file_stat: FileEntry, base_path: str = "", full: bool = False
) -> str:
    """Return the human-readable echo format.

    :param file_stat: File entry to render.
    :param base_path: Base path for relative rendering.
    :param full: Whether to display full path.
    :return: Display line with human size and mtime.
    """
    return "%10s %s %s" % (
        _get_human_size(file_stat.stat.st_size),
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file_stat.stat.st_mtime)),
        await _get_echo_path(file_stat, base_path, full),
    )


async def _wrap_entry(path: str, entry: FileEntry, filesystem) -> FileEntry:
    """Wrap a filesystem entry to ensure its path is a full URI.

    :param path: Base path used to determine filesystem.
    :param entry: File entry from filesystem.
    :param filesystem: Filesystem instance.
    :return: FileEntry with URI path.
    """
    return FileEntry(
        name=entry.name,
        path=filesystem.build_uri(entry.path),
        stat=entry.stat,
    )


async def _list_stat(path: str) -> T.AsyncIterator[FileEntry]:
    """List entries for a path, yielding FileEntry items.

    :param path: Path to list.
    :return: Async iterator of FileEntry items.
    """
    path_obj = SmartPath(path)
    if await path_obj.is_file():
        stat_result = await path_obj.stat()
        yield FileEntry(path_obj.name, str(path_obj), stat_result)
        return
    async with path_obj.filesystem.scandir(path_obj._path) as iterator:
        async for entry in iterator:
            yield await _wrap_entry(path, entry, path_obj.filesystem)


async def _scan_stat(path: str) -> T.AsyncIterator[FileEntry]:
    """Recursively scan files under a path.

    :param path: Path to scan.
    :return: Async iterator of FileEntry items.
    """
    path_obj = SmartPath(path)
    async with path_obj.filesystem.scanfile(path_obj._path) as iterator:
        async for entry in iterator:
            yield await _wrap_entry(path, entry, path_obj.filesystem)


async def _glob_stat(path: str, recursive: bool = True) -> T.AsyncIterator[FileEntry]:
    """Yield FileEntry items for a glob pattern.

    :param path: Glob pattern.
    :param recursive: Whether to allow recursive patterns.
    :return: Async iterator of FileEntry items.
    """
    path_obj = SmartPath(path)
    filesystem = path_obj.filesystem
    if hasattr(filesystem, "glob_stat"):
        iterator = filesystem.glob_stat(path_obj._path, recursive=recursive)
        async for entry in iterator:
            yield await _wrap_entry(path, entry, filesystem)
        return
    fs_func = FSFunc(
        exists=filesystem.exists,
        isdir=filesystem.is_dir,
        scandir=filesystem.scandir,
    )
    async for matched_path in iglob(path_obj._path, fs=fs_func, recursive=recursive):
        stat_result = await filesystem.stat(matched_path)
        name = os.path.basename(matched_path.rstrip("/"))
        if not name:
            name = matched_path
        yield FileEntry(
            name=name,
            path=filesystem.build_uri(matched_path),
            stat=stat_result,
        )


async def _ls(
    path: str,
    *,
    long: bool,
    full: bool,
    recursive: bool,
    human_readable: bool,
) -> None:
    """List files for the CLI.

    :param path: Path or glob pattern.
    :param long: Whether to show long format.
    :param full: Whether to show full paths.
    :param recursive: Whether to traverse recursively.
    :param human_readable: Whether to show human-readable sizes.
    """
    base_path = path
    if path == "file://":
        path = "./"
    if has_magic(path):
        scan_func = _glob_stat
        base_path = get_non_glob_dir(path)
        full = True
    elif recursive:
        scan_func = _scan_stat
    else:
        scan_func = _list_stat

    if long:
        if human_readable:
            echo_func = _human_echo
        else:
            echo_func = _long_echo
    else:
        echo_func = _simple_echo

    total_size = 0
    total_count = 0
    async for file_stat in scan_func(path):
        total_size += file_stat.stat.st_size
        total_count += 1
        output = await echo_func(file_stat, base_path, full=full)
        if long and file_stat.is_symlink():
            target = await smart_readlink(file_stat.path)
            output += f" -> {target}"
        click.echo(output)
    if long:
        click.echo(f"total({total_count}): {_get_human_size(total_size)}")


class PathType(ParamType):
    """Click parameter type for aiomegfile paths."""

    name = "path"

    def shell_complete(self, ctx, param, incomplete):  # noqa: D401
        """Return shell completion candidates for paths."""
        if not incomplete:
            return [CompletionItem(f"{protocol}://") for protocol in FILE_SYSTEMS]

        if "//" not in incomplete:
            matches = glob.glob(incomplete + "*")
            items: list[CompletionItem] = []
            for match in matches[:128]:
                suffix = "/" if os.path.isdir(match) else ""
                items.append(CompletionItem(f"{match}{suffix}"))
            return items

        if incomplete.startswith("file://"):
            matches = glob.glob(incomplete[7:] + "*")
            items: list[CompletionItem] = []
            for match in matches[:128]:
                suffix = "/" if os.path.isdir(match) else ""
                items.append(CompletionItem(f"file://{match}{suffix}"))
            return items

        async def _complete() -> list[CompletionItem]:
            """Complete remote paths using filesystem globbing."""
            try:
                entries: list[CompletionItem] = []
                count = 0
                async for entry in _glob_stat(incomplete + "*", recursive=False):
                    if count >= 128:
                        break
                    suffix = "/" if entry.is_dir() else ""
                    entries.append(CompletionItem(f"{entry.path}{suffix}"))
                    count += 1
                return entries
            except Exception:
                return []

        return _run_async(_complete())


# Remove trailing spaces in completion
ZshComplete.source_template = ZshComplete.source_template.replace(
    "compadd -U -V", "compadd -S '' -U -V"
)


@cli.command(short_help="List all the objects in the path.")
@click.argument("path", type=PathType())
@click.option(
    "-l",
    "--long",
    is_flag=True,
    help="List all the objects with size, modification time and path.",
)
@click.option(
    "-f",
    "--full",
    is_flag=True,
    help="Displays the full path of each file.",
)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    help="Command is performed on all files under the specified path.",
)
@click.option(
    "-h",
    "--human-readable",
    is_flag=True,
    help="Displays file sizes in human readable format.",
)
def ls(
    path: str, long: bool, full: bool, recursive: bool, human_readable: bool
) -> None:
    """List all the objects in the path."""
    _run_async(
        _ls(
            path,
            long=long,
            full=full,
            recursive=recursive,
            human_readable=human_readable,
        )
    )


@cli.command(short_help="List all the objects in the path.")
@click.argument("path", type=PathType())
@click.option(
    "-f",
    "--full",
    is_flag=True,
    help="Displays the full path of each file.",
)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    help="Command is performed on all files under the specified path.",
)
def ll(path: str, recursive: bool, full: bool) -> None:
    """List all the objects in the path with human readable sizes."""
    _run_async(
        _ls(
            path,
            long=True,
            full=full,
            recursive=recursive,
            human_readable=True,
        )
    )


async def _copy_file_with_progress(
    src_path: str,
    dst_path: str,
    *,
    overwrite: bool,
    skip: bool,
) -> None:
    """Copy a file with a progress bar.

    :param src_path: Source file path.
    :param dst_path: Destination file path.
    :param overwrite: Whether to overwrite existing files.
    :param skip: Whether to skip existing files.
    """
    if skip and await smart_exists(dst_path):
        return
    if not overwrite and await smart_exists(dst_path):
        raise FileExistsError(f"Destination path already exists: {dst_path}")
    file_size = (await smart_stat(src_path)).st_size
    sbar = tqdm(
        total=file_size,
        unit="B",
        ascii=True,
        unit_scale=True,
        unit_divisor=1024,
    )

    def callback(length: int) -> None:
        """Update progress for copied bytes."""
        sbar.update(length)

    async with smart_open(src_path, "rb") as src_file:
        async with smart_open(dst_path, "wb") as dst_file:
            await copyfileobj(src_file, dst_file, callback=callback)
    sbar.close()


@cli.command(short_help="Copy files from source to dest, skipping already copied.")
@click.argument("src_path", type=PathType())
@click.argument("dst_path", type=PathType())
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    help="Command is performed on all files under the specified path.",
)
@click.option(
    "-T",
    "--no-target-directory",
    is_flag=True,
    help="Treat dst_path as a normal file.",
)
@click.option("-g", "--progress-bar", is_flag=True, help="Show progress bar.")
@click.option("--skip", is_flag=True, help="Skip existed files.")
def cp(
    src_path: str,
    dst_path: str,
    recursive: bool,
    no_target_directory: bool,
    progress_bar: bool,
    skip: bool,
) -> None:
    """Copy files from source to destination."""

    async def _run() -> None:
        """Execute the copy operation."""
        nonlocal dst_path
        if not no_target_directory and (
            dst_path.endswith("/") or await smart_isdir(dst_path)
        ):
            dst_path = str(SmartPath(dst_path) / SmartPath(src_path).name)

        if recursive:

            def callback(length: int) -> None:
                """Update progress for copied bytes."""
                if pbar is not None:
                    pbar.update(length)

            pbar = None
            if progress_bar:
                pbar = tqdm(
                    total=None,
                    unit="B",
                    ascii=True,
                    unit_scale=True,
                    unit_divisor=1024,
                )
            try:
                await smart_sync(
                    src_path,
                    dst_path,
                    followlinks=True,
                    overwrite=not skip,
                    callback=callback if progress_bar else None,
                )
            finally:
                if pbar is not None:
                    pbar.close()
        else:
            if progress_bar:
                await _copy_file_with_progress(
                    src_path,
                    dst_path,
                    overwrite=not skip,
                    skip=skip,
                )
            else:
                if skip and await smart_exists(dst_path):
                    return
                await smart_copy(src_path, dst_path, followlinks=True)

    _run_async(_run())


@cli.command(short_help="Move files from source to dest.")
@click.argument("src_path", type=PathType())
@click.argument("dst_path", type=PathType())
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    help="Command is performed on all files under the specified path.",
)
@click.option(
    "-T",
    "--no-target-directory",
    is_flag=True,
    help="Treat dst_path as a normal file.",
)
@click.option("-g", "--progress-bar", is_flag=True, help="Show progress bar.")
@click.option("--skip", is_flag=True, help="Skip existed files.")
def mv(
    src_path: str,
    dst_path: str,
    recursive: bool,
    no_target_directory: bool,
    progress_bar: bool,
    skip: bool,
) -> None:
    """Move files from source to destination."""

    async def _run() -> None:
        """Execute the move operation."""
        nonlocal dst_path
        if not no_target_directory and (
            dst_path.endswith("/") or await smart_isdir(dst_path)
        ):
            dst_path = str(SmartPath(dst_path) / SmartPath(src_path).name)

        src_fs = SmartPath(src_path).filesystem
        dst_fs = SmartPath(dst_path).filesystem
        same_endpoint = src_fs.same_endpoint(dst_fs)
        dst_exists = await smart_exists(dst_path)
        if skip and dst_exists and same_endpoint:
            return

        if progress_bar:
            if recursive:
                if same_endpoint:
                    with tqdm(total=1) as tbar:
                        await smart_move(src_path, dst_path)
                        tbar.update(1)
                else:
                    if skip and dst_exists:
                        await smart_sync(
                            src_path,
                            dst_path,
                            followlinks=True,
                            overwrite=False,
                        )
                        return
                    pbar = tqdm(
                        total=None,
                        unit="B",
                        ascii=True,
                        unit_scale=True,
                        unit_divisor=1024,
                    )

                    def callback(length: int) -> None:
                        """Update progress for copied bytes."""
                        pbar.update(length)

                    try:
                        await smart_sync(
                            src_path,
                            dst_path,
                            followlinks=True,
                            overwrite=not skip,
                            callback=callback,
                        )
                        if not (skip and dst_exists):
                            await smart_remove(src_path)
                    finally:
                        pbar.close()
            else:
                if same_endpoint:
                    if skip and dst_exists:
                        return
                    with tqdm(total=1) as tbar:
                        await smart_rename(src_path, dst_path)
                        tbar.update(1)
                else:
                    if skip and dst_exists:
                        return
                    await _copy_file_with_progress(
                        src_path,
                        dst_path,
                        overwrite=not skip,
                        skip=skip,
                    )
                    if not (skip and dst_exists):
                        await smart_unlink(src_path)
        else:
            if skip and dst_exists:
                if same_endpoint:
                    return
                await smart_sync(
                    src_path,
                    dst_path,
                    followlinks=True,
                    overwrite=False,
                )
                return
            move_func = smart_move if recursive else smart_rename
            await move_func(src_path, dst_path)

    _run_async(_run())


@cli.command(short_help="Remove files from path.")
@click.argument("path", type=PathType())
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    help="Command is performed on all files under the specified path.",
)
def rm(path: str, recursive: bool) -> None:
    """Remove files from a path."""

    async def _run() -> None:
        """Execute the remove operation."""
        remove_func = smart_remove if recursive else smart_unlink
        await remove_func(path)

    _run_async(_run())


@cli.command(short_help="Make source and dest identical, modifying destination only.")
@click.argument("src_path", type=PathType())
@click.argument("dst_path", type=PathType())
@click.option(
    "-f", "--force", is_flag=True, help="Copy files forcible, ignore same files."
)
@click.option("--skip", is_flag=True, help="Skip existed files.")
@click.option("-w", "--worker", type=click.INT, default=-1, help="Number of workers.")
@click.option("-g", "--progress-bar", is_flag=True, help="Show progress bar.")
@click.option("-v", "--verbose", is_flag=True, help="Show more progress log.")
@click.option("-q", "--quiet", is_flag=True, help="Not show any progress log.")
def sync(
    src_path: str,
    dst_path: str,
    force: bool,
    skip: bool,
    worker: int,
    progress_bar: bool,
    verbose: bool,
    quiet: bool,
) -> None:
    """Sync files from source to destination."""

    async def _run() -> None:
        """Execute the sync operation."""
        if worker != -1 and options.get("debug"):
            logging.getLogger(__name__).debug("worker option is ignored")

        progress_enabled = progress_bar
        verbose_enabled = verbose
        if quiet:
            progress_enabled = False
            verbose_enabled = False

        if progress_enabled:
            tbar = tqdm(
                total=None,
                ascii=True,
                desc="Files",
            )
            sbar = tqdm(
                total=None,
                ascii=True,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Bytes",
            )

            def callback_after_copy_file(
                src_file_path: str, dst_file_path: str
            ) -> None:
                """Update file count after each file copy."""
                if verbose_enabled:
                    tqdm.write(f"copy {src_file_path} -> {dst_file_path} done")
                tbar.update(1)

            def callback(length: int) -> None:
                """Update size progress after each data chunk."""
                sbar.update(length)

            try:
                await smart_sync(
                    src_path,
                    dst_path,
                    followlinks=True,
                    force=force,
                    overwrite=not skip,
                    callback=callback,
                    callback_after_copy_file=callback_after_copy_file,
                )
            finally:
                tbar.set_description_str("Files")
                sbar.set_description_str("Bytes")
                tbar.refresh()
                sbar.refresh()
                tbar.close()
                sbar.close()
        else:

            def callback_after_copy_file(
                src_file_path: str, dst_file_path: str
            ) -> None:
                """Emit verbose logs after each file copy."""
                if verbose_enabled:
                    click.echo(f"copy {src_file_path} -> {dst_file_path} done")

            await smart_sync(
                src_path,
                dst_path,
                followlinks=True,
                force=force,
                overwrite=not skip,
                callback_after_copy_file=callback_after_copy_file
                if verbose_enabled
                else None,
            )

    _run_async(_run())


@cli.command(short_help="Make the path if it doesn't already exist.")
@click.argument("path", type=PathType())
def mkdir(path: str) -> None:
    """Create a directory."""

    async def _run() -> None:
        """Execute mkdir."""
        await smart_makedirs(path)

    _run_async(_run())


@cli.command(short_help="Make the file if it doesn't already exist.")
@click.argument("path", type=PathType())
def touch(path: str) -> None:
    """Touch a file."""

    async def _run() -> None:
        """Execute touch."""
        await smart_touch(path)

    _run_async(_run())


@cli.command(short_help="Concatenate any files and send them to stdout.")
@click.argument("path", type=PathType())
def cat(path: str) -> None:
    """Print the file content to stdout."""

    async def _run() -> None:
        """Execute cat."""
        stdout = click.get_binary_stream("stdout")
        async with smart_open(path, "rb") as src_file:
            while True:
                chunk = await src_file.read(READER_BLOCK_SIZE)
                if not chunk:
                    break
                stdout.write(chunk)
        stdout.flush()

    _run_async(_run())


@cli.command(short_help="Concatenate any files and send first n lines of them.")
@click.argument("path", type=PathType())
@click.option(
    "-n", "--lines", type=click.INT, default=10, help="Print the first NUM lines"
)
def head(path: str, lines: int) -> None:
    """Print the first lines of a file."""

    async def _run() -> None:
        """Execute head."""
        stdout = click.get_binary_stream("stdout")
        async with smart_open(path, "rb") as src_file:
            for _ in range(lines):
                content = await src_file.readline()
                if not content:
                    break
                stdout.write(content)
        stdout.flush()

    _run_async(_run())


async def _tail_follow_content(path: str, offset: int) -> int:
    """Follow file changes and output appended data.

    :param path: File path.
    :param offset: Current offset in bytes.
    :return: Updated offset.
    """
    stdout = click.get_binary_stream("stdout")
    async with smart_open(path, "rb") as src_file:
        await _maybe_await(src_file.seek(offset))
        for line in await src_file.readlines():
            stdout.write(line)
        stdout.flush()
        offset = await _maybe_await(src_file.tell())
    return offset


@cli.command(short_help="Concatenate any files and send last n lines of them.")
@click.argument("path", type=PathType())
@click.option(
    "-n", "--lines", type=click.INT, default=10, help="Print the last NUM lines"
)
@click.option("-f", "--follow", is_flag=True, help="Output appended data")
def tail(path: str, lines: int, follow: bool) -> None:
    """Print the last lines of a file."""

    async def _run() -> None:
        """Execute tail."""
        stdout = click.get_binary_stream("stdout")
        line_list: list[bytes] = []
        async with smart_open(path, "rb") as src_file:
            await _maybe_await(src_file.seek(0, os.SEEK_END))
            file_size = await _maybe_await(src_file.tell())
            await _maybe_await(src_file.seek(0, os.SEEK_SET))

            for current_offset in range(
                file_size - READER_BLOCK_SIZE,
                0 - READER_BLOCK_SIZE,
                -READER_BLOCK_SIZE,
            ):
                current_offset = max(0, current_offset)
                await _maybe_await(src_file.seek(current_offset))
                block_lines = (await src_file.read(READER_BLOCK_SIZE)).split(b"\n")
                if line_list:
                    block_lines[-1] += line_list[0]
                    block_lines.extend(line_list[1:])
                if len(block_lines) > lines:
                    line_list = block_lines[-lines:]
                    break
                line_list = block_lines

        for line in line_list[:-1]:
            stdout.write(line + b"\n")
        if line_list:
            stdout.write(line_list[-1])
        stdout.flush()

        if follow:  # pragma: no cover
            offset = file_size
            while True:
                new_offset = await _tail_follow_content(path, offset)
                if new_offset == offset:
                    await asyncio.sleep(1)
                else:
                    offset = new_offset

    _run_async(_run())


@cli.command(short_help="Write bytes from stdin to file.")
@click.argument("path", type=PathType())
@click.option("-a", "--append", is_flag=True, help="Append to the given file")
@click.option("-o", "--stdout", is_flag=True, help="Output to stdout as well")
def to(path: str, append: bool, stdout: bool) -> None:
    """Write stdin to a file."""

    async def _run() -> None:
        """Execute streaming stdin to file."""
        mode = "ab" if append else "wb"
        stdin = click.get_binary_stream("stdin")
        stdout_stream = click.get_binary_stream("stdout")
        async with smart_open(path, mode) as dst_file:
            while True:
                chunk = await asyncio.to_thread(stdin.read, READER_BLOCK_SIZE)
                if not chunk:
                    break
                await dst_file.write(chunk)
                if stdout:
                    stdout_stream.write(chunk)
        if stdout:
            stdout_stream.flush()

    _run_async(_run())


async def _md5sum(path: str) -> str:
    """Compute MD5 checksum for a file.

    :param path: File path.
    :return: MD5 hex digest.
    """
    # TODO: support smart_getmd5
    if await smart_isdir(path):
        raise IsADirectoryError(f"md5sum only supports files: {path}")
    md5 = hashlib.md5()
    async with smart_open(path, "rb") as src_file:
        while True:
            chunk = await src_file.read(READER_BLOCK_SIZE)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()


@cli.command(short_help="Produce an md5sum file for the objects in the path.")
@click.argument("path", type=PathType())
def md5sum(path: str) -> None:
    """Compute md5 checksum for a file."""

    async def _run() -> None:
        """Execute md5sum."""
        click.echo(await _md5sum(path))

    _run_async(_run())


@cli.command(short_help="Return the total size in bytes for path.")
@click.argument("path", type=PathType())
def size(path: str) -> None:
    """Return the size of a file or directory."""

    async def _run() -> None:
        """Execute size."""
        click.echo(await smart_getsize(path))

    _run_async(_run())


@cli.command(short_help="Return the mtime timestamp for path.")
@click.argument("path", type=PathType())
def mtime(path: str) -> None:
    """Return the last modification time of a path."""

    async def _run() -> None:
        """Execute mtime."""
        click.echo(await smart_getmtime(path))

    _run_async(_run())


@cli.command(short_help="Return the stat for path.")
@click.argument("path", type=PathType())
def stat(path: str) -> None:
    """Return stat info for a path."""

    async def _run() -> None:
        """Execute stat."""
        click.echo(await smart_stat(path))

    _run_async(_run())


@cli.command(short_help="Return the aiomegfile version.")
def version() -> None:
    """Return the aiomegfile version."""
    click.echo(__version__)


@cli.group(short_help="Update credentials and config files.")
def config() -> None:
    """Configuration helpers."""


def _safe_makedirs(path: str) -> None:
    """Create a directory path if needed.

    :param path: Directory path.
    """
    if path not in ("", ".", "/"):
        os.makedirs(path, exist_ok=True)


@config.command(short_help="Update the config file for s3")
@click.option(
    "-p",
    "--path",
    type=str,
    default="~/.aws/credentials",
    help="S3 config file, default is $HOME/.aws/credentials",
)
@click.option(
    "-n", "--profile-name", type=str, default="default", help="S3 profile name"
)
@click.argument("aws_access_key_id")
@click.argument("aws_secret_access_key")
@click.option("-e", "--endpoint-url", help="endpoint-url")
@click.option("-st", "--session-token", help="session-token")
@click.option("-as", "--addressing-style", help="addressing-style")
@click.option("-sv", "--signature-version", help="signature-version")
@click.option("--no-cover", is_flag=True, help="Not cover the same-name config")
def s3(
    path: str,
    profile_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    endpoint_url: str | None,
    session_token: str | None,
    addressing_style: str | None,
    signature_version: str | None,
    no_cover: bool,
) -> None:
    """Update S3 credentials in the AWS credentials file."""
    path = os.path.expanduser(path)

    config_dict: dict[str, str | dict[str, str]] = {"name": profile_name}
    if aws_access_key_id:
        config_dict["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        config_dict["aws_secret_access_key"] = aws_secret_access_key
    if session_token:
        config_dict["aws_session_token"] = session_token

    s3_config_dict: dict[str, str] = {}
    if endpoint_url:
        s3_config_dict["endpoint_url"] = endpoint_url
    if addressing_style:
        s3_config_dict["addressing_style"] = addressing_style
    if signature_version:
        s3_config_dict["signature_version"] = signature_version
    if s3_config_dict:
        config_dict["s3"] = s3_config_dict

    def _dumps(config_payload: dict[str, str | dict[str, str]]) -> str:
        """Serialize config payload to ini-like content."""
        content = f"[{config_payload['name']}]\n"
        for key in ("aws_access_key_id", "aws_secret_access_key", "session_token"):
            if key in config_payload:
                content += f"{key} = {config_payload[key]}\n"
        if "s3" in config_payload:
            s3_config = T.cast(dict[str, str], config_payload["s3"])
            content += "\ns3 = \n"
            for key, value in s3_config.items():
                content += f"    {key} = {value}\n"
        return content

    _safe_makedirs(os.path.dirname(path))
    if not os.path.exists(path):
        content_str = _dumps(config_dict)
        with open(path, "w", encoding="utf-8") as fp:
            fp.write(content_str)
        click.echo(f"Your s3 config has been saved into {path}")
        return

    used = False
    with open(path, "r", encoding="utf-8") as fp:
        text = fp.read()
    sections = text.strip().split("[")

    if sections and len(sections[0]) <= 1:
        sections = sections[1:]

    for idx in range(len(sections)):
        section = sections[idx]
        cur_name = section.split("]")[0]
        if cur_name == profile_name:
            if no_cover:
                raise NameError(f"profile-name has been used: {profile_name}")
            used = True
            sections[idx] = _dumps(config_dict)
            click.echo(f"The {profile_name} config has been updated.")
            continue
        sections[idx] = "\n" + ("[" + section).strip() + "\n"
    text = "\n".join(sections)
    if not used:
        text += "\n" + _dumps(config_dict)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(text)
    click.echo(f"Your s3 config has been saved into {path}")


@cli.group(short_help="Return the completion file")
def completion() -> None:
    """Shell completion helper."""


@completion.command(short_help="Update the config file for bash")
def bash() -> None:
    """Enable bash completion for aiomegfile."""
    script_name = os.path.basename(sys.argv[0])
    command = f'eval "$(_{script_name.upper()}_COMPLETE=bash_source {script_name})"'
    config_path = os.path.expanduser("~/.bashrc")
    with open(config_path, "r", encoding="utf-8") as fp:
        if command in fp.read():
            click.echo("Your bashrc has already been updated.")
            return
    with open(config_path, "a", encoding="utf-8") as fp:
        fp.write("\n" + command + "\n")
    click.echo("Your bashrc has been updated.")


@completion.command(short_help="Update the config file for zsh")
def zsh() -> None:
    """Enable zsh completion for aiomegfile."""
    script_name = os.path.basename(sys.argv[0])
    command = f'eval "$(_{script_name.upper()}_COMPLETE=zsh_source {script_name})"'
    config_path = os.path.expanduser("~/.zshrc")
    with open(config_path, "r", encoding="utf-8") as fp:
        if command in fp.read():
            click.echo("Your zshrc has already been updated.")
            return
    with open(config_path, "a", encoding="utf-8") as fp:
        fp.write("\n" + command + "\n")
    click.echo("Your zshrc has been updated.")


@completion.command(short_help="Update the config file for fish")
def fish() -> None:
    """Enable fish completion for aiomegfile."""
    script_name = os.path.basename(sys.argv[0])
    command = f"_{script_name.upper()}_COMPLETE=fish_source {script_name} | source"
    config_path = os.path.expanduser(f"~/.config/fish/completions/{script_name}.fish")
    _safe_makedirs(os.path.dirname(config_path))
    with open(config_path, "w", encoding="utf-8") as fp:
        fp.write(command)
    click.echo(f"Your fish config has been saved into {config_path}.")


if __name__ == "__main__":
    # Usage: python -m aiomegfile.cli
    safe_cli()  # pragma: no cover
