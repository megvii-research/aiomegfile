"""Tests for ``WebdavFileSystem`` and ``is_webdav``."""

import pytest

from aiomegfile.filesystem import webdav as webdav_module
from aiomegfile.filesystem.webdav import (
    WEBDAV_PASSWORD_ENV,
    WEBDAV_TOKEN_COMMAND_ENV,
    WEBDAV_USERNAME_ENV,
    WebdavFileSystem,
    WebdavsFileSystem,
    is_webdav,
)
from aiomegfile.interfaces import Access, get_filesystem_by_uri
from aiomegfile.lib.cacher import AioFileCacher
from aiomegfile.lib.prefetch_reader.webdav_prefetch_reader import (
    AioWebdavPrefetchReader,
)
from tests.utils.fake_webdav import FakeWebdavClient


@pytest.fixture
def filesystem(monkeypatch):
    """Create WebDAV filesystem with fake async WebDAV backend.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[WebdavFileSystem, FakeWebdavClient]
    """
    client = FakeWebdavClient()
    client.dirs.update({"/data", "/data/sub"})
    client.files["/data/file.txt"] = b"line1\nline2\n"
    client.files["/data/sub/nested.txt"] = b"nested"

    filesystem = WebdavFileSystem(
        host="example.com",
        port=8080,
        username="demo",
        password="secret",
    )

    async def _fake_create_client():
        """Return fixture client through async filesystem factory."""
        return client

    monkeypatch.setattr(filesystem, "_create_client", _fake_create_client)
    return filesystem, client


class TestWebdavFileSystem:
    """Test cases for ``WebdavFileSystem``."""

    async def test_is_webdav_and_get_filesystem_by_uri(self):
        """Test WebDAV URI detection and registry lookup."""
        assert is_webdav("webdav://example.com/file.txt") is True
        assert is_webdav("webdavs://example.com/file.txt") is True
        assert is_webdav("http://example.com/file.txt") is False

        webdav_fs = get_filesystem_by_uri("webdav://example.com/file.txt")
        webdavs_fs = get_filesystem_by_uri("webdavs://example.com/file.txt")
        assert isinstance(webdav_fs, WebdavFileSystem)
        assert isinstance(webdavs_fs, WebdavsFileSystem)

    async def test_parse_and_build_uri(self):
        """Test URI parse/build roundtrip."""
        filesystem = WebdavFileSystem.from_uri(
            "webdav://demo:secret@example.com:8080/data.txt"
        )

        assert (
            filesystem.parse_uri("webdav://demo:secret@example.com:8080/data.txt")
            == "/data.txt"
        )
        assert (
            filesystem.build_uri("/data.txt")
            == "webdav://demo:secret@example.com:8080/data.txt"
        )
        assert (
            filesystem.build_uri("data.txt")
            == "webdav://demo:secret@example.com:8080/data.txt"
        )

    async def test_from_uri_build_uri_does_not_leak_env_password(self, monkeypatch):
        """Test URI rendering keeps credential visibility from input URI."""
        monkeypatch.setenv(WEBDAV_USERNAME_ENV, "env-user")
        monkeypatch.setenv(WEBDAV_PASSWORD_ENV, "env-password")

        filesystem = WebdavFileSystem.from_uri("webdav://demo@example.com/data.txt")
        assert filesystem._endpoint.username == "demo"
        assert filesystem._endpoint.password == "env-password"
        assert filesystem.build_uri("/data.txt") == "webdav://demo@example.com/data.txt"

        filesystem_no_user = WebdavFileSystem.from_uri("webdav://example.com/data.txt")
        assert filesystem_no_user._endpoint.username == "env-user"
        assert (
            filesystem_no_user.build_uri("/data.txt") == "webdav://example.com/data.txt"
        )

    async def test_from_uri_loads_token_command_from_env(self, monkeypatch):
        """Test URI constructor reads ``WEBDAV_TOKEN_COMMAND`` from environment."""
        monkeypatch.setenv(WEBDAV_TOKEN_COMMAND_ENV, "echo test-token")
        filesystem = WebdavFileSystem.from_uri("webdav://example.com/data.txt")
        assert filesystem._endpoint.token_command == "echo test-token"

    async def test_create_client_uses_shared_get_webdav_client(self, monkeypatch):
        """Test ``_create_client`` delegates to shared cached client helper."""
        filesystem = WebdavFileSystem(
            host="example.com",
            username="demo",
            password="secret",
            token_command="echo test-token",
        )
        fake_client = object()
        captured: dict[str, object] = {}

        async def _fake_get_webdav_client(**kwargs):
            captured.update(kwargs)
            return fake_client

        monkeypatch.setattr(webdav_module, "get_webdav_client", _fake_get_webdav_client)

        assert await filesystem._create_client() is fake_client
        assert captured["hostname"] == "http://example.com"
        assert captured["username"] == "demo"
        assert captured["password"] == "secret"
        assert captured["token_command"] == "echo test-token"

    async def test_exists_is_file_is_dir_stat(self, filesystem):
        """Test existence and stat operations."""
        fs, _ = filesystem

        assert await fs.exists("/data/file.txt") is True
        assert await fs.exists("/not-found.txt") is False
        assert await fs.is_file("/data/file.txt") is True
        assert await fs.is_file("/data") is False
        assert await fs.is_dir("/data") is True
        assert await fs.is_dir("/data/file.txt") is False

        stat_result = await fs.stat("/data/file.txt")
        assert stat_result.st_size == len(b"line1\nline2\n")
        assert stat_result.isdir is False
        assert stat_result.islnk is False

    async def test_open_read_modes(self, filesystem):
        """Test read open modes use WebDAV prefetch reader."""
        fs, _ = filesystem

        async with fs.open("/data/file.txt", "rb") as file_obj:
            assert isinstance(file_obj, AioWebdavPrefetchReader)
            assert await file_obj.read() == b"line1\nline2\n"

        async with fs.open("/data/file.txt", "r", encoding="utf-8") as file_obj:
            line = await file_obj.readline()
            assert line == "line1\n"
            await file_obj.seek(0)
            assert await file_obj.read(5) == "line1"

    async def test_open_write_append_and_x(self, filesystem):
        """Test writing, appending, and exclusive create."""
        fs, _ = filesystem

        async with fs.open("/data/new.txt", "w") as file_obj:
            assert await file_obj.write("abc") == 3

        async with fs.open("/data/new.txt", "ab") as file_obj:
            assert isinstance(file_obj, AioFileCacher)
            assert await file_obj.write(b"123") == 3

        async with fs.open("/data/new.txt", "rb") as file_obj:
            assert await file_obj.read() == b"abc123"

        with pytest.raises(FileExistsError):
            async with fs.open("/data/new.txt", "x"):
                pass

    async def test_scandir_and_scanfile(self, filesystem):
        """Test directory scanning and recursive file scanning."""
        fs, _ = filesystem

        entries = []
        async with fs.scandir("/data") as scanner:
            async for entry in scanner:
                entries.append(entry.name)

        assert sorted(entries) == ["file.txt", "sub"]

        files = []
        async with fs.scanfile("/data") as scanner:
            async for entry in scanner:
                files.append(entry.path)

        assert sorted(files) == ["/data/file.txt", "/data/sub/nested.txt"]

    async def test_copy_move_remove_and_mkdir(self, filesystem):
        """Test copy, move, remove, and mkdir operations."""
        fs, client = filesystem

        copied_path = await fs.copy("/data/file.txt", "/data/copied.txt")
        assert copied_path == "/data/copied.txt"
        assert client.files["/data/copied.txt"] == b"line1\nline2\n"

        moved_path = await fs.move("/data/copied.txt", "/data/moved.txt")
        assert moved_path == "/data/moved.txt"
        assert "/data/copied.txt" not in client.files
        assert client.files["/data/moved.txt"] == b"line1\nline2\n"

        await fs.remove("/data/moved.txt")
        assert await fs.exists("/data/moved.txt") is False

        await fs.mkdir("/created/deep/dir", parents=True, exist_ok=True)
        assert await fs.is_dir("/created/deep/dir") is True

    async def test_copy_same_path_raises(self, filesystem):
        """Test copy raises when source and destination are the same."""
        fs, _ = filesystem
        with pytest.raises(OSError):
            await fs.copy("/data/file.txt", "/data/file.txt")

    async def test_move_overwrite_false_raises(self, filesystem):
        """Test move raises when destination exists and overwrite is False."""
        fs, _ = filesystem
        with pytest.raises(FileExistsError):
            await fs.move("/data/file.txt", "/data/sub/nested.txt", overwrite=False)

    async def test_remove_missing_ok(self, filesystem):
        """Test remove supports missing_ok behavior."""
        fs, _ = filesystem
        await fs.remove("/not-exists.txt", missing_ok=True)
        with pytest.raises(FileNotFoundError):
            await fs.remove("/not-exists.txt", missing_ok=False)

    async def test_upload_and_download(self, filesystem, tmp_path):
        """Test upload and download operations with callbacks."""
        fs, client = filesystem

        source = tmp_path / "upload.bin"
        source.write_bytes(b"upload-content")
        upload_progress = []
        await fs.upload(
            str(source),
            "/remote/upload.bin",
            callback=upload_progress.append,
        )
        assert client.files["/remote/upload.bin"] == b"upload-content"
        assert sum(upload_progress) == len(b"upload-content")

        destination = tmp_path / "download" / "output.bin"
        download_progress = []
        await fs.download(
            "/remote/upload.bin",
            str(destination),
            callback=download_progress.append,
        )
        assert destination.read_bytes() == b"upload-content"
        assert sum(download_progress) == len(b"upload-content")

    async def test_upload_and_download_directory_error(self, filesystem, tmp_path):
        """Test upload/download raise on directory inputs."""
        fs, client = filesystem

        local_dir = tmp_path / "local-dir"
        local_dir.mkdir()
        with pytest.raises(IsADirectoryError):
            await fs.upload(str(local_dir), "/remote/dir")

        client.dirs.add("/remote-dir")
        with pytest.raises(IsADirectoryError):
            await fs.download("/remote-dir", str(tmp_path / "out.txt"))

    async def test_misc_methods(self, filesystem):
        """Test helper methods like absolute, samefile, and access."""
        fs, _ = filesystem

        assert await fs.absolute("data/file.txt") == "/data/file.txt"
        assert await fs.samefile("/data/file.txt", "/data/file.txt") is True
        assert await fs.samefile("/data/file.txt", "/data/sub/nested.txt") is False
        assert await fs.is_absolute("/data/file.txt") is True
        assert await fs.is_absolute("data/file.txt") is False

        assert await fs.access("/data/file.txt", mode=Access.READ) is True
        assert await fs.access("/data/file.txt", mode=Access.WRITE) is True
        assert await fs.access("/not-found.txt", mode=Access.READ) is False
        with pytest.raises(TypeError):
            await fs.access("/data/file.txt", mode="bad")  # type: ignore[arg-type]

    async def test_scanfile_on_single_file(self, filesystem):
        """Test scanfile returns one entry when input path is a file."""
        fs, _ = filesystem
        entries = []
        async with fs.scanfile("/data/file.txt") as scanner:
            async for entry in scanner:
                entries.append(entry.path)
        assert entries == ["/data/file.txt"]

    async def test_open_invalid_mode_raises(self, filesystem):
        """Test invalid open mode raises ValueError."""
        fs, _ = filesystem
        with pytest.raises(ValueError):
            fs.open("/data/file.txt", "r+")

    async def test_same_endpoint(self):
        """Test same_endpoint compares endpoint settings."""
        fs1 = WebdavFileSystem(host="same-host", username="demo", password="secret")
        fs2 = WebdavFileSystem(host="same-host", username="demo", password="secret")
        fs3 = WebdavFileSystem(host="other-host", username="demo", password="secret")
        assert fs1.same_endpoint(fs2) is True
        assert fs1.same_endpoint(fs3) is False

    async def test_missing_aiodav_dependency_hint(self, monkeypatch):
        """Test missing optional dependency raises install hint."""
        fs = WebdavFileSystem(host="example.com", username="demo", password="secret")

        def _raise_missing_dependency(**kwargs):
            _ = kwargs
            raise ModuleNotFoundError("""
                Failed to import aiodav, the following steps show you how to install it:

                    pip3 install 'aiomegfile[webdav]'
                """)

        monkeypatch.setattr(
            webdav_module,
            "get_webdav_client",
            _raise_missing_dependency,
        )

        with pytest.raises(ModuleNotFoundError, match="aiomegfile\\[webdav\\]"):
            await fs.exists("/data/file.txt")
