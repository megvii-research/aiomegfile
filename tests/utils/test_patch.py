import os
import sys
import types

from aiomegfile.utils.patch import (
    _compat,
    _run_coroutine,
    _SyncAsyncFile,
    _SyncAsyncProxy,
    _wrap_async_result,
    patch_megfile,
)


def _placeholder(*args, **kwargs):
    """Return a sentinel tuple for unpatched placeholder callables.

    :param args: Positional arguments.
    :param kwargs: Keyword arguments.
    :return: Placeholder payload.
    :rtype: tuple
    """
    return ("original", args, kwargs)


class _PlaceholderPath:
    """Placeholder path class used by fake megfile modules."""

    def __init__(self, path, *other_paths):
        """Initialize the placeholder path.

        :param path: Initial path.
        :param other_paths: Extra path segments.
        """
        self.path = str(path)
        self.other_paths = tuple(other_paths)


def _install_fake_megfile(monkeypatch):
    """Install a fake megfile package hierarchy into ``sys.modules``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Mapping of created module objects.
    :rtype: dict[str, types.ModuleType]
    """
    module_names = [
        "megfile",
        "megfile.smart",
        "megfile.smart_path",
        "megfile.fs_path",
        "megfile.hdfs_path",
        "megfile.http_path",
        "megfile.s3_path",
        "megfile.sftp_path",
        "megfile.stdio_path",
        "megfile.webdav_path",
    ]
    modules = {name: types.ModuleType(name) for name in module_names}

    megfile = modules["megfile"]
    megfile.__path__ = []
    megfile.__all__ = [
        "smart_access",
        "smart_cache",
        "smart_combine_open",
        "smart_concat",
        "smart_copy",
        "smart_exists",
        "smart_getmd5",
        "smart_getmtime",
        "smart_getsize",
        "smart_glob",
        "smart_glob_stat",
        "smart_iglob",
        "smart_isdir",
        "smart_isfile",
        "smart_islink",
        "smart_listdir",
        "smart_load_content",
        "smart_load_from",
        "smart_load_text",
        "smart_lstat",
        "smart_makedirs",
        "smart_move",
        "smart_open",
        "smart_path_join",
        "smart_readlink",
        "smart_realpath",
        "smart_remove",
        "smart_rename",
        "smart_save_as",
        "smart_save_content",
        "smart_save_text",
        "smart_scan",
        "smart_scan_stat",
        "smart_scandir",
        "smart_stat",
        "smart_symlink",
        "smart_sync",
        "smart_touch",
        "smart_unlink",
        "smart_walk",
        "FSPath",
        "HdfsPath",
        "HttpPath",
        "HttpsPath",
        "S3Path",
        "SftpPath",
        "SmartPath",
        "StdioPath",
        "WebdavPath",
        "fs_copy",
        "is_fs",
        "is_hdfs",
        "is_http",
        "is_s3",
        "is_sftp",
        "is_stdio",
        "is_webdav",
        "s3_buffered_open",
        "s3_cached_open",
        "s3_concat",
        "s3_copy",
        "s3_download",
        "s3_load_content",
        "s3_memory_open",
        "s3_open",
        "s3_pipe_open",
        "s3_prefetch_open",
        "s3_share_cache_open",
        "s3_upload",
        "sftp_add_host_key",
        "sftp_concat",
        "sftp_copy",
        "sftp_download",
        "sftp_upload",
        "stdio_open",
    ]

    smart_module = modules["megfile.smart"]
    for name in [
        "SmartPath",
        "get_traditional_path",
        "register_copy_func",
        "smart_access",
        "smart_cache",
        "smart_combine_open",
        "smart_concat",
        "smart_copy",
        "smart_exists",
        "smart_getmd5",
        "smart_getmtime",
        "smart_getsize",
        "smart_glob",
        "smart_glob_stat",
        "smart_iglob",
        "smart_isdir",
        "smart_isfile",
        "smart_islink",
        "smart_listdir",
        "smart_load_content",
        "smart_load_from",
        "smart_load_text",
        "smart_lstat",
        "smart_makedirs",
        "smart_move",
        "smart_open",
        "smart_path_join",
        "smart_readlink",
        "smart_realpath",
        "smart_remove",
        "smart_rename",
        "smart_save_as",
        "smart_save_content",
        "smart_save_text",
        "smart_scan",
        "smart_scan_stat",
        "smart_scandir",
        "smart_stat",
        "smart_symlink",
        "smart_sync",
        "smart_sync_with_progress",
        "smart_touch",
        "smart_unlink",
        "smart_walk",
    ]:
        setattr(smart_module, name, _placeholder)

    smart_path_module = modules["megfile.smart_path"]
    smart_path_module.SmartPath = _PlaceholderPath
    smart_path_module.get_traditional_path = _placeholder

    fs_module = modules["megfile.fs_path"]
    fs_module.FSPath = _PlaceholderPath
    fs_module.is_fs = _placeholder
    fs_module.fs_copy = _placeholder

    hdfs_module = modules["megfile.hdfs_path"]
    hdfs_module.HdfsPath = _PlaceholderPath
    hdfs_module.is_hdfs = _placeholder

    http_module = modules["megfile.http_path"]
    http_module.HttpPath = _PlaceholderPath
    http_module.HttpsPath = _PlaceholderPath
    http_module.is_http = _placeholder

    s3_module = modules["megfile.s3_path"]
    for name in [
        "S3Path",
        "is_s3",
        "s3_buffered_open",
        "s3_cached_open",
        "s3_concat",
        "s3_copy",
        "s3_download",
        "s3_load_content",
        "s3_memory_open",
        "s3_open",
        "s3_pipe_open",
        "s3_prefetch_open",
        "s3_share_cache_open",
        "s3_upload",
    ]:
        setattr(s3_module, name, _PlaceholderPath if name == "S3Path" else _placeholder)

    sftp_module = modules["megfile.sftp_path"]
    for name in [
        "SftpPath",
        "is_sftp",
        "sftp_add_host_key",
        "sftp_concat",
        "sftp_copy",
        "sftp_download",
        "sftp_upload",
    ]:
        setattr(
            sftp_module, name, _PlaceholderPath if name == "SftpPath" else _placeholder
        )

    stdio_module = modules["megfile.stdio_path"]
    stdio_module.StdioPath = _PlaceholderPath
    stdio_module.is_stdio = _placeholder

    webdav_module = modules["megfile.webdav_path"]
    webdav_module.WebdavPath = _PlaceholderPath
    webdav_module.is_webdav = _placeholder

    megfile.smart = smart_module
    megfile.smart_path = smart_path_module
    megfile.fs_path = fs_module
    megfile.hdfs_path = hdfs_module
    megfile.http_path = http_module
    megfile.s3_path = s3_module
    megfile.sftp_path = sftp_module
    megfile.stdio_path = stdio_module
    megfile.webdav_path = webdav_module

    for module in modules.values():
        monkeypatch.setitem(sys.modules, module.__name__, module)

    for name in megfile.__all__:
        if hasattr(smart_module, name):
            setattr(megfile, name, getattr(smart_module, name))
            continue
        for submodule in (
            fs_module,
            hdfs_module,
            http_module,
            s3_module,
            sftp_module,
            stdio_module,
            webdav_module,
            smart_path_module,
        ):
            if hasattr(submodule, name):
                setattr(megfile, name, getattr(submodule, name))
                break

    return modules


def test_patch_megfile_smart_exists(tmp_path, monkeypatch):
    """Replace megfile smart_exists with aiomegfile's implementation.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patched = patch_megfile()
    assert "smart_exists" in patched

    file_path = tmp_path / "exists.txt"
    file_path.write_text("ok", encoding="utf-8")

    import megfile.smart as meg_smart

    assert meg_smart.smart_exists(str(file_path)) is True


def test_patch_megfile_top_level_smart_open(tmp_path, monkeypatch):
    """Expose a sync smart_open on megfile top-level.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile

    file_path = tmp_path / "data.txt"
    with megfile.smart_open(str(file_path), "w") as handle:
        handle.write("hello")

    with megfile.smart_open(str(file_path), "r") as handle:
        assert handle.read() == "hello"


def test_patch_megfile_smart_scandir(tmp_path, monkeypatch):
    """Expose a sync scandir iterator.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "b.txt").write_text("b", encoding="utf-8")

    import megfile.smart as meg_smart

    entries = list(meg_smart.smart_scandir(str(tmp_path)))
    names = sorted(entry.name for entry in entries)
    assert names == ["a.txt", "b.txt"]

    with meg_smart.smart_scandir(str(tmp_path)) as iterator:
        names = sorted(entry.name for entry in iterator)
        assert names == ["a.txt", "b.txt"]


def test_patch_megfile_sync_smart_path_and_top_level_classes(tmp_path, monkeypatch):
    """Patch SmartPath and top-level protocol path classes.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile
    import megfile.s3_path as meg_s3_path
    import megfile.smart_path as meg_smart_path

    file_path = tmp_path / "demo.txt"
    file_path.write_text("payload", encoding="utf-8")

    path_obj = megfile.SmartPath(str(file_path))
    assert meg_smart_path.SmartPath is megfile.SmartPath
    assert path_obj.exists() is True
    assert path_obj.read_text() == "payload"
    assert path_obj.parent.name == tmp_path.name

    mkdir_target = megfile.FSPath(str(tmp_path / "nested" / "dir"))
    mkdir_target.makedirs(exist_ok=True)
    assert os.path.isdir(str(tmp_path / "nested" / "dir"))
    assert megfile.is_fs(str(file_path)) is True

    s3_path = megfile.S3Path("bucket/key.txt")
    assert meg_s3_path.S3Path is megfile.S3Path
    assert str(s3_path) == "s3://bucket/key.txt"
    assert s3_path.path == "bucket/key.txt"
    assert megfile.is_s3("s3://bucket/key.txt") is True
    assert meg_smart_path.get_traditional_path(s3_path) == "bucket/key.txt"

    sftp_path = megfile.SftpPath("example.com/home/demo.txt")
    assert str(sftp_path) == "sftp://example.com/home/demo.txt"
    assert sftp_path.path == "example.com/home/demo.txt"
    assert megfile.is_sftp("sftp://example.com/home/demo.txt") is True


def test_patch_megfile_patches_protocol_helpers(monkeypatch):
    """Patch protocol-specific top-level helpers.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile
    import megfile.s3_path as meg_s3_path

    sentinel = object()

    def fake_protocol_open(name, path, protocols, mode, followlinks, kwargs):
        """Return a sentinel for protocol open testing.

        :param name: Wrapper name.
        :param path: Input path.
        :param protocols: Accepted protocols.
        :param mode: File mode.
        :param followlinks: Followlinks flag.
        :param kwargs: Extra keyword arguments.
        :return: Sentinel object.
        :rtype: object
        """
        assert name == "s3_open"
        assert path == "s3://bucket/key"
        assert protocols == ("s3",)
        assert mode == "rb"
        assert followlinks is False
        assert kwargs == {}
        return sentinel

    monkeypatch.setattr(_compat, "_protocol_open", fake_protocol_open)
    assert megfile.s3_open("s3://bucket/key", "rb") is sentinel
    assert meg_s3_path.s3_open("s3://bucket/key", "rb") is sentinel


def test_patch_megfile_register_copy_func_overrides_smart_copy(monkeypatch):
    """Use registered copy functions before delegating to aiomegfile.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile.smart as meg_smart

    called = {}

    def fake_copy(src_path, dst_path, callback, followlinks, overwrite):
        """Capture copy invocation details.

        :param src_path: Source path.
        :param dst_path: Destination path.
        :param callback: Callback value.
        :param followlinks: Followlinks flag.
        :param overwrite: Overwrite flag.
        """
        called["args"] = (src_path, dst_path, callback, followlinks, overwrite)

    meg_smart.register_copy_func("file", "file", fake_copy)
    meg_smart.smart_copy("src.txt", "dst.txt", followlinks=True, overwrite=False)

    assert called["args"] == ("src.txt", "dst.txt", None, True, False)


def test_patch_megfile_adds_missing_stdio_open(monkeypatch):
    """Add stdio_open even when fake top-level megfile did not define it.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile

    assert hasattr(megfile, "stdio_open")
    assert megfile.stdio_open is _compat.stdio_open


def test_patch_megfile_sftp_add_host_key(tmp_path, monkeypatch):
    """Write a known_hosts entry through the compatibility helper.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile

    known_hosts = tmp_path / "known_hosts"

    class FakeKey:
        """Fake SSH key object."""

        def export_public_key(self, format_name: str = "openssh") -> bytes:
            """Return fake public key payload.

            :param format_name: Requested format name.
            :return: Fake public key bytes.
            :rtype: bytes
            """
            assert format_name == "openssh"
            return b"ssh-ed25519 AAAATESTKEY"

    async def fake_get_server_host_key(host: str, port: int):
        """Return a fake SSH host key.

        :param host: Host name.
        :param port: Port number.
        :return: Fake key instance.
        :rtype: FakeKey
        """
        assert host == "example.com"
        assert port == 2200
        return FakeKey()

    monkeypatch.setattr(
        "aiomegfile.utils.patch._compat.asyncssh.get_server_host_key",
        fake_get_server_host_key,
    )

    megfile.sftp_add_host_key(
        "example.com",
        port=2200,
        host_key_path=str(known_hosts),
    )

    content = known_hosts.read_text(encoding="utf-8")
    assert "[example.com]:2200 ssh-ed25519 AAAATESTKEY" in content


def test_patch_megfile_smart_ismount(monkeypatch):
    """Patch smart_ismount through the compatibility SmartPath wrapper.

    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)
    patch_megfile()

    import megfile.smart as meg_smart

    assert meg_smart.smart_ismount("/") is os.path.ismount("/")


async def test_run_coroutine_inside_loop():
    """Run coroutine via thread when an event loop is active."""

    async def sample() -> str:
        """Return a sentinel value.

        :return: Sentinel value.
        :rtype: str
        """
        return "ok"

    result = _run_coroutine(sample())
    assert result == "ok"


def test_wrap_async_result_with_async_iterator():
    """Wrap async iterator into a sync iterator."""

    class AsyncIter:
        """Simple async iterator for testing."""

        def __init__(self) -> None:
            """Initialize the iterator."""
            self._items = iter([1, 2])

        def __aiter__(self):
            """Return self as an async iterator.

            :return: Async iterator instance.
            :rtype: AsyncIter
            """
            return self

        async def __anext__(self) -> int:
            """Return the next item or raise StopAsyncIteration.

            :return: Next integer item.
            :rtype: int
            """
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    wrapped = _wrap_async_result(AsyncIter())
    assert list(wrapped) == [1, 2]


def test_wrap_async_result_with_async_context_manager():
    """Wrap async context manager and proxy async methods."""

    class AsyncValue:
        """Async value provider."""

        async def read(self) -> str:
            """Return a payload string.

            :return: Payload string.
            :rtype: str
            """
            return "payload"

    class AsyncManager:
        """Async context manager returning AsyncValue."""

        async def __aenter__(self) -> AsyncValue:
            """Enter the async context.

            :return: AsyncValue instance.
            :rtype: AsyncValue
            """
            return AsyncValue()

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            """Exit the async context.

            :return: None
            :rtype: None
            """
            return None

    wrapped = _wrap_async_result(AsyncManager())
    with wrapped as handle:
        assert handle.read() == "payload"


def test_sync_async_proxy_iterates_async_iterator():
    """Sync proxy should iterate async iterators."""

    class AsyncIter:
        """Async iterator for proxy testing."""

        def __init__(self) -> None:
            """Initialize iterator."""
            self._items = iter(["a", "b"])

        def __aiter__(self):
            """Return self as an async iterator.

            :return: Async iterator instance.
            :rtype: AsyncIter
            """
            return self

        async def __anext__(self) -> str:
            """Return next value or raise StopAsyncIteration.

            :return: Next string item.
            :rtype: str
            """
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    proxy = _SyncAsyncProxy(AsyncIter())
    assert list(proxy) == ["a", "b"]


def test_sync_async_file_close_idempotent():
    """Sync async file should close once without error."""

    class AsyncFile:
        """Async file-like object."""

        def __init__(self) -> None:
            """Initialize file state."""
            self.closed = False

        async def read(self) -> str:
            """Return file content.

            :return: File content.
            :rtype: str
            """
            return "data"

        async def close(self) -> None:
            """Close the file."""
            self.closed = True

    class AsyncFileManager:
        """Async context manager for AsyncFile."""

        def __init__(self) -> None:
            """Initialize the manager."""
            self.file = AsyncFile()

        async def __aenter__(self) -> AsyncFile:
            """Enter the async context.

            :return: AsyncFile instance.
            :rtype: AsyncFile
            """
            return self.file

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            """Exit the async context."""
            self.file.closed = True

    manager = AsyncFileManager()
    sync_file = _SyncAsyncFile(manager)
    assert sync_file.read() == "data"

    sync_file.close()
    sync_file.close()
    assert manager.file.closed is True
