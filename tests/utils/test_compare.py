from aiomegfile.interfaces import StatResult
from aiomegfile.utils.compare import compare_time, get_sync_type, is_same_file


def _stat(size: int, mtime: float, isdir: bool = False) -> StatResult:
    """Create a StatResult for test comparisons."""
    return StatResult(st_size=size, st_mtime=mtime, isdir=isdir)


def test_get_sync_type():
    assert get_sync_type("file", "s3") == "upload"
    assert get_sync_type("s3", "file") == "download"
    assert get_sync_type("file", "file") == "copy"


def test_compare_time_upload_copy():
    src = _stat(1, 10.0)
    dst_newer = _stat(1, 20.0)
    dst_older = _stat(1, 5.0)

    assert compare_time(src, dst_newer, "upload") is True
    assert compare_time(src, dst_older, "upload") is False
    assert compare_time(src, dst_newer, "copy") is True
    assert compare_time(src, dst_older, "copy") is False


def test_compare_time_download():
    src = _stat(1, 10.0)
    dst_newer = _stat(1, 20.0)
    dst_older = _stat(1, 5.0)

    assert compare_time(src, dst_older, "download") is True
    assert compare_time(src, dst_newer, "download") is False


def test_is_same_file():
    src = _stat(2, 10.0)
    dst_same = _stat(2, 10.0)
    dst_diff = _stat(3, 10.0)

    assert is_same_file(src, dst_same, "copy") is True
    assert is_same_file(src, dst_diff, "copy") is False
    assert is_same_file(src, _stat(2, 10.0, isdir=True), "copy") is False
