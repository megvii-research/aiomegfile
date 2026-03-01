import io
import os

import pytest
from moto.server import ThreadedMotoServer

from aiomegfile.filesystem.s3 import S3FileSystem
from aiomegfile.smart import (
    smart_abspath,
    smart_concat,
    smart_copy,
    smart_copy_file,
    smart_exists,
    smart_glob,
    smart_iglob,
    smart_isabs,
    smart_isdir,
    smart_isfile,
    smart_islink,
    smart_listdir,
    smart_load_content,
    smart_load_from,
    smart_load_text,
    smart_makedirs,
    smart_move,
    smart_open,
    smart_path_join,
    smart_readlink,
    smart_realpath,
    smart_relpath,
    smart_remove,
    smart_rename,
    smart_save_as,
    smart_save_content,
    smart_save_text,
    smart_scan,
    smart_scandir,
    smart_stat,
    smart_symlink,
    smart_sync,
    smart_touch,
    smart_unlink,
    smart_walk,
)

_aws_access_key_id = "testing"
_aws_secret_access_key = "testing"
_bucket_name = "test-bucket"


@pytest.fixture(scope="module")
def moto_server():
    """Start a moto server for S3 sync tests.

    :return: Endpoint URL for the moto server.
    :rtype: str
    """
    server = ThreadedMotoServer()
    try:
        server.start()
        host, port = server.get_host_and_port()
        if host == "0.0.0.0":
            host = "localhost"
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def mock_s3(moto_server, monkeypatch):
    """Mock AWS credentials and endpoint URL to environment variables.

    :param moto_server: Moto server endpoint URL.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _aws_access_key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _aws_secret_access_key)
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)


@pytest.fixture
def s3_filesystem(mock_s3):
    """Create S3FileSystem configured via environment variables.

    :param mock_s3: Fixture configuring moto-backed S3.
    :return: S3FileSystem instance.
    :rtype: S3FileSystem
    """
    return S3FileSystem()


async def _create_bucket(filesystem: S3FileSystem) -> None:
    """Create the test bucket.

    :param filesystem: S3FileSystem instance.
    """
    client = await filesystem._get_client()
    await client.create_bucket(Bucket=_bucket_name)


async def _put_object(filesystem: S3FileSystem, key: str, body: bytes) -> None:
    """Put an object into the test bucket.

    :param filesystem: S3FileSystem instance.
    :param key: Object key to create.
    :param body: Object bytes to store.
    """
    client = await filesystem._get_client()
    await client.put_object(Bucket=_bucket_name, Key=key, Body=body)


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


async def test_smart_save_as_and_load_text(tmp_path):
    """Test smart_save_as writes stream content and smart_load_text reads it."""
    file_path = tmp_path / "stream.txt"
    buffer = io.BytesIO(b"stream")

    await smart_save_as(buffer, file_path)

    assert await smart_load_text(file_path) == "stream"


async def test_smart_save_content_and_text(tmp_path):
    """Test smart_save_content and smart_save_text write data."""
    bytes_path = tmp_path / "bytes.bin"
    text_path = tmp_path / "text.txt"

    await smart_save_content(bytes_path, b"abc")
    await smart_save_text(text_path, "hello")

    assert bytes_path.read_bytes() == b"abc"
    assert text_path.read_text() == "hello"


async def test_smart_scan_and_isabs_abspath(tmp_path):
    """Test smart_scan yields file paths and isabs/abspath behavior."""
    root = tmp_path / "scan_root"
    root.mkdir()
    file_a = root / "a.txt"
    file_b = root / "sub" / "b.txt"
    file_b.parent.mkdir()
    file_a.write_text("a")
    file_b.write_text("b")

    scanned = []
    async for path in smart_scan(root):
        scanned.append(path)

    assert str(file_a) in scanned
    assert str(file_b) in scanned

    abs_path = await smart_abspath(file_a)
    assert os.path.isabs(abs_path)
    assert await smart_isabs(abs_path) is True
    assert await smart_isabs("relative/path") is False
    assert await smart_isabs("s3://bucket/key") is True


async def test_smart_copy_file_callback(tmp_path):
    """Test smart_copy_file invokes callback with copied bytes."""
    src_path = tmp_path / "source.txt"
    src_path.write_text("callback")
    dst_path = tmp_path / "dest.txt"

    total = 0

    def callback(size: int) -> None:
        """Accumulate copied bytes."""
        nonlocal total
        total += size

    await smart_copy_file(src_path, dst_path, callback=callback)

    assert dst_path.read_text() == "callback"
    assert total == src_path.stat().st_size


async def test_smart_sync_callbacks(tmp_path):
    """Test smart_sync callbacks are invoked."""
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    dst_dir.mkdir()

    file_a = src_dir / "a.txt"
    file_b = src_dir / "b.txt"
    file_a.write_text("alpha")
    file_b.write_text("bravo")

    copied = {"bytes": 0, "count": 0}

    def callback(size: int) -> None:
        """Accumulate copied bytes."""
        copied["bytes"] += size

    def callback_after_copy_file(src_path: str, dst_path: str) -> None:
        """Count copied files."""
        copied["count"] += 1

    await smart_sync(
        src_dir,
        dst_dir,
        callback=callback,
        callback_after_copy_file=callback_after_copy_file,
    )

    assert copied["count"] == 2
    assert copied["bytes"] == file_a.stat().st_size + file_b.stat().st_size


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


async def test_smart_sync_fs_to_s3(tmp_path, s3_filesystem):
    """Test syncing from local filesystem to S3."""
    await _create_bucket(s3_filesystem)
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "alpha.txt").write_text("alpha")
    subdir = src_dir / "sub"
    subdir.mkdir()
    (subdir / "bravo.txt").write_text("bravo")

    dst_prefix = f"s3://{_bucket_name}/sync-dst"
    await smart_sync(src_dir, dst_prefix)

    assert await smart_load_content(f"{dst_prefix}/alpha.txt") == b"alpha"
    assert await smart_load_content(f"{dst_prefix}/sub/bravo.txt") == b"bravo"


async def test_smart_sync_fs_to_s3_mtime_overwrite_force(tmp_path, s3_filesystem):
    """Test fs->s3 sync with mtime, overwrite, and force behavior."""
    await _create_bucket(s3_filesystem)
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_file = src_dir / "data.txt"
    src_file.write_text("alpha")

    dst_prefix = f"s3://{_bucket_name}/sync-overwrite"
    dst_file = f"{dst_prefix}/data.txt"

    await smart_sync(src_dir, dst_prefix)
    assert await smart_load_content(dst_file) == b"alpha"

    src_file.write_text("bravo")
    s3_stat = await smart_stat(dst_file)
    older_time = s3_stat.st_mtime - 3600
    os.utime(src_file, (older_time, older_time))

    await smart_sync(src_dir, dst_prefix)
    assert await smart_load_content(dst_file) == b"alpha"

    src_file.write_text("candy")
    await smart_sync(src_dir, dst_prefix, overwrite=False)
    assert await smart_load_content(dst_file) == b"alpha"

    await smart_sync(src_dir, dst_prefix, force=True, overwrite=False)
    assert await smart_load_content(dst_file) == b"candy"


async def test_smart_sync_s3_to_fs(tmp_path, s3_filesystem):
    """Test syncing from S3 to local filesystem."""
    await _create_bucket(s3_filesystem)
    await _put_object(s3_filesystem, "sync-src/one.txt", b"one")
    await _put_object(s3_filesystem, "sync-src/two.txt", b"two")
    await _put_object(s3_filesystem, "sync-src/nested/three.txt", b"three")

    src_prefix = f"s3://{_bucket_name}/sync-src"
    dst_dir = tmp_path / "dst"
    await smart_sync(src_prefix, dst_dir)

    assert (dst_dir / "one.txt").read_text() == "one"
    assert (dst_dir / "two.txt").read_text() == "two"
    assert (dst_dir / "nested" / "three.txt").read_text() == "three"


async def test_smart_sync_s3_to_fs_mtime(tmp_path, s3_filesystem):
    """Test s3->fs sync honors mtime comparisons."""
    await _create_bucket(s3_filesystem)
    await _put_object(s3_filesystem, "sync-mtime/file.txt", b"alpha")

    src_prefix = f"s3://{_bucket_name}/sync-mtime"
    dst_dir = tmp_path / "dst"
    dst_file = dst_dir / "file.txt"

    await smart_sync(src_prefix, dst_dir)
    assert dst_file.read_text() == "alpha"

    src_stat = await smart_stat(f"{src_prefix}/file.txt")
    dst_file.write_text("bravo")
    older_time = src_stat.st_mtime - 3600
    os.utime(dst_file, (older_time, older_time))

    await smart_sync(src_prefix, dst_dir)
    assert dst_file.read_text() == "bravo"

    dst_file.write_text("candy")
    newer_time = src_stat.st_mtime + 3600
    os.utime(dst_file, (newer_time, newer_time))

    await smart_sync(src_prefix, dst_dir)
    assert dst_file.read_text() == "alpha"


async def test_smart_sync_partial_overlap(tmp_path):
    """Test sync with partially overlapping source and destination files."""
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    dst_dir.mkdir()

    (src_dir / "common.txt").write_text("same")
    (src_dir / "only_src.txt").write_text("new")

    common_dst = dst_dir / "common.txt"
    common_dst.write_text("same")
    (dst_dir / "only_dst.txt").write_text("keep")

    future_time = os.path.getmtime(common_dst) + 3600
    os.utime(common_dst, (future_time, future_time))

    await smart_sync(src_dir, dst_dir)

    assert (dst_dir / "only_src.txt").read_text() == "new"
    assert (dst_dir / "only_dst.txt").read_text() == "keep"
    assert os.path.getmtime(common_dst) == future_time


async def test_smart_sync_same_name_different_dirs(tmp_path):
    """Test sync handles same file names under different subdirectories."""
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    dst_dir.mkdir()

    src_a = src_dir / "a"
    src_b = src_dir / "b"
    dst_a = dst_dir / "a"
    dst_b = dst_dir / "b"
    src_a.mkdir()
    src_b.mkdir()
    dst_a.mkdir()
    dst_b.mkdir()

    (src_a / "file.txt").write_text("alpha")
    (src_b / "file.txt").write_text("bravo-new")

    a_dst_file = dst_a / "file.txt"
    a_dst_file.write_text("alpha")
    b_dst_file = dst_b / "file.txt"
    b_dst_file.write_text("old")

    future_time = os.path.getmtime(a_dst_file) + 3600
    os.utime(a_dst_file, (future_time, future_time))

    await smart_sync(src_dir, dst_dir)

    assert (dst_dir / "a" / "file.txt").read_text() == "alpha"
    assert os.path.getmtime(a_dst_file) == future_time
    assert (dst_dir / "b" / "file.txt").read_text() == "bravo-new"


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


async def test_smart_concat_concatenates_files(tmp_path):
    """Test smart_concat concatenates files in order."""
    first_path = tmp_path / "first.bin"
    second_path = tmp_path / "second.bin"
    first_path.write_bytes(b"hello-")
    second_path.write_bytes(b"world")

    dest_path = tmp_path / "combined.bin"
    await smart_concat([first_path, second_path], dest_path)

    assert dest_path.read_bytes() == b"hello-world"


async def test_smart_concat_empty_source_noop(tmp_path):
    """Test smart_concat does nothing when source list is empty."""
    dest_path = tmp_path / "combined.bin"

    await smart_concat([], dest_path)

    assert not dest_path.exists()
