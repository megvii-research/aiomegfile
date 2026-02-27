import aiofiles

from aiomegfile.utils.path import copyfileobj


async def test_copyfileobj_copies_bytes_and_calls_callback(tmp_path):
    """Test copyfileobj copies bytes and reports progress callbacks."""
    data = b"abcdefghijklmnopqrstuvwxyz"
    src_path = tmp_path / "src.bin"
    dst_path = tmp_path / "dst.bin"
    src_path.write_bytes(data)

    progress = []
    async with (
        aiofiles.open(src_path, "rb") as fsrc,
        aiofiles.open(dst_path, "wb") as fdst,
    ):
        await copyfileobj(fsrc, fdst, callback=progress.append, buffer=5)

    assert dst_path.read_bytes() == data
    assert sum(progress) == len(data)
    assert progress
    assert all(size <= 5 for size in progress)


async def test_copyfileobj_empty_source_noop(tmp_path):
    """Test copyfileobj handles empty source without callbacks."""
    src_path = tmp_path / "empty.bin"
    dst_path = tmp_path / "empty_out.bin"
    src_path.write_bytes(b"")

    progress = []
    async with (
        aiofiles.open(src_path, "rb") as fsrc,
        aiofiles.open(dst_path, "wb") as fdst,
    ):
        await copyfileobj(fsrc, fdst, callback=progress.append, buffer=8)

    assert dst_path.read_bytes() == b""
    assert progress == []
