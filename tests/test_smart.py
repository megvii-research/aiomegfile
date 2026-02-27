import os

import pytest

from aiomegfile.smart import (
    smart_copy,
    smart_exists,
    smart_glob,
    smart_iglob,
    smart_isdir,
    smart_isfile,
    smart_islink,
    smart_listdir,
    smart_load_content,
    smart_load_from,
    smart_makedirs,
    smart_move,
    smart_open,
    smart_path_join,
    smart_readlink,
    smart_realpath,
    smart_relpath,
    smart_remove,
    smart_rename,
    smart_scandir,
    smart_stat,
    smart_symlink,
    smart_sync,
    smart_touch,
    smart_unlink,
    smart_walk,
)


async def test_smart_exists_isfile_isdir(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_text("data")
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    assert await smart_exists(file_path)
    assert not await smart_exists(tmp_path / "missing.txt")
    assert await smart_isfile(file_path)
    assert not await smart_isfile(dir_path)
    assert await smart_isdir(dir_path)
    assert not await smart_isdir(file_path)


async def test_smart_touch_unlink_makedirs(tmp_path):
    nested_dir = tmp_path / "nested" / "dir"
    await smart_makedirs(nested_dir)
    assert nested_dir.exists()

    file_path = nested_dir / "new.txt"
    await smart_touch(file_path)
    assert file_path.exists()

    await smart_unlink(file_path)
    assert not file_path.exists()


async def test_smart_remove_file_and_dir(tmp_path):
    """Test removing a file and a directory recursively."""
    file_path = tmp_path / "remove.txt"
    file_path.write_text("data")
    await smart_remove(file_path)
    assert not file_path.exists()

    dir_path = tmp_path / "remove_dir"
    dir_path.mkdir()
    (dir_path / "nested.txt").write_text("nested")
    await smart_remove(dir_path)
    assert not dir_path.exists()


async def test_smart_remove_missing_ok(tmp_path):
    """Test missing_ok behavior for smart_remove."""
    missing_path = tmp_path / "missing.txt"
    await smart_remove(missing_path, missing_ok=True)

    with pytest.raises(FileNotFoundError):
        await smart_remove(missing_path, missing_ok=False)


async def test_smart_open_read_write(tmp_path):
    file_path = tmp_path / "open.txt"
    async with smart_open(file_path, "w") as f:
        await f.write("hello")
    async with smart_open(file_path, "r") as f:
        assert await f.read() == "hello"


async def test_smart_load_from(tmp_path):
    """Test smart_load_from returns a binary reader with content."""
    file_path = tmp_path / "load.bin"
    file_path.write_bytes(b"abc")

    reader = await smart_load_from(file_path)
    try:
        assert reader.read() == b"abc"
    finally:
        reader.close()


async def test_smart_load_content(tmp_path):
    """Test smart_load_content reads full and ranged content."""
    file_path = tmp_path / "range.bin"
    file_path.write_bytes(b"abcdef")

    assert await smart_load_content(file_path) == b"abcdef"
    assert await smart_load_content(file_path, start=2, stop=5) == b"cde"


async def test_smart_path_join(tmp_path):
    joined = await smart_path_join(tmp_path, "a", "b.txt")
    assert joined == os.path.join(str(tmp_path), "a", "b.txt")


async def test_smart_copy_move_rename(tmp_path):
    src_file = tmp_path / "src.txt"
    src_file.write_text("content")

    dst_file = tmp_path / "dst.txt"
    copied = await smart_copy(src_file, dst_file)
    assert copied == str(dst_file)
    assert dst_file.read_text() == "content"

    move_src = tmp_path / "move_src.txt"
    move_src.write_text("move")
    move_dst = tmp_path / "move_dst.txt"
    moved = await smart_move(move_src, move_dst)
    assert moved == str(move_dst)
    assert move_dst.exists()
    assert not move_src.exists()

    rename_src = tmp_path / "rename_src.txt"
    rename_src.write_text("rename")
    rename_dst = tmp_path / "rename_dst.txt"
    renamed = await smart_rename(rename_src, rename_dst)
    assert renamed == str(rename_dst)
    assert rename_dst.exists()
    assert not rename_src.exists()


async def test_smart_sync_directory(tmp_path):
    """Test syncing directory contents to a destination path."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "a.txt").write_text("a")
    subdir = src_dir / "sub"
    subdir.mkdir()
    (subdir / "b.txt").write_text("b")

    dst_dir = tmp_path / "dst"
    await smart_sync(src_dir, dst_dir)

    assert (dst_dir / "a.txt").read_text() == "a"
    assert (dst_dir / "sub" / "b.txt").read_text() == "b"


async def test_smart_sync_file(tmp_path):
    """Test syncing a single file to a destination path."""
    src_file = tmp_path / "src.txt"
    src_file.write_text("sync")
    dst_file = tmp_path / "dst.txt"

    await smart_sync(src_file, dst_file)

    assert dst_file.read_text() == "sync"


async def test_smart_sync_no_overwrite(tmp_path):
    """Test syncing does not overwrite when overwrite is disabled."""
    src_file = tmp_path / "src.txt"
    src_file.write_text("new")
    dst_file = tmp_path / "dst.txt"
    dst_file.write_text("old")

    await smart_sync(src_file, dst_file, overwrite=False)

    assert dst_file.read_text() == "old"


async def test_smart_sync_skip_when_dest_newer(tmp_path):
    """Test sync skips when destination is newer with same size."""
    src_file = tmp_path / "src.txt"
    src_file.write_text("data")
    dst_file = tmp_path / "dst.txt"
    dst_file.write_text("data")

    future_time = os.path.getmtime(dst_file) + 3600
    os.utime(dst_file, (future_time, future_time))
    before_mtime = os.path.getmtime(dst_file)

    await smart_sync(src_file, dst_file)

    assert os.path.getmtime(dst_file) == before_mtime


async def test_smart_scandir_and_listdir(tmp_path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    (tmp_path / "subdir").mkdir()

    entries = []
    async with smart_scandir(tmp_path) as it:
        async for entry in it:
            entries.append(entry.name)

    names = set(entries)
    assert names == {"a.txt", "b.txt", "subdir"}

    listed = await smart_listdir(tmp_path)
    assert set(listed) == names


async def test_smart_stat(tmp_path):
    file_path = tmp_path / "stat.txt"
    file_path.write_text("data")
    result = await smart_stat(file_path)
    assert result.st_size > 0


async def test_smart_glob_and_iglob(tmp_path):
    (tmp_path / "file1.txt").write_text("a")
    (tmp_path / "file2.txt").write_text("b")
    (tmp_path / "other.md").write_text("c")

    pattern = os.path.join(str(tmp_path), "*.txt")
    results = await smart_glob(pattern)
    assert {os.path.basename(path) for path in results} == {"file1.txt", "file2.txt"}

    collected = []
    async for item in smart_iglob(pattern):
        collected.append(item)
    assert {os.path.basename(path) for path in collected} == {"file1.txt", "file2.txt"}


async def test_smart_walk(tmp_path):
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (tmp_path / "root.txt").write_text("root")
    (subdir / "child.txt").write_text("child")

    seen_files = []
    async for _root, _dirs, files in smart_walk(tmp_path):
        seen_files.extend(files)

    assert {"root.txt", "child.txt"}.issubset(set(seen_files))


async def test_smart_realpath_relpath_symlink(tmp_path):
    src_file = tmp_path / "src.txt"
    src_file.write_text("x")
    link_path = tmp_path / "link.txt"

    await smart_symlink(src_file, link_path)
    assert await smart_islink(link_path)

    target = await smart_readlink(link_path)
    assert target == str(src_file)

    resolved = await smart_realpath(link_path)
    assert os.path.isabs(resolved)

    rel = await smart_relpath(src_file, tmp_path)
    assert rel == "src.txt"
