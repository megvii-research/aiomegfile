"""Tests for SftpFileSystem and is_sftp."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from aiomegfile.filesystem import sftp as sftp_module
from aiomegfile.filesystem.sftp import SftpFileSystem, get_sftp_client, is_sftp
from aiomegfile.interfaces import Access, get_filesystem_by_uri
from tests.utils.fake_sftp import FakeSFTPClient, FakeSSHConnection


@pytest.fixture
def filesystem(monkeypatch):
    """Create SftpFileSystem with fake async SFTP backend.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[SftpFileSystem, FakeSFTPClient]
    """
    client = FakeSFTPClient()
    client.files["/abs.txt"] = b"absolute"
    client.files["/home/test/rel.txt"] = b"relative"
    client.files["/lines.txt"] = b"line1\nline2\n"

    filesystem = SftpFileSystem(
        host="example.com",
        port=2222,
        username="demo",
        password="secret",
    )

    async def _open_client():
        return FakeSSHConnection(), client

    monkeypatch.setattr(filesystem, "_open_client", _open_client)

    return filesystem, client


class TestSftpFileSystem:
    """Test cases for SftpFileSystem."""

    async def test_is_sftp_and_get_filesystem_by_uri(self):
        """Test SFTP URI detection and registry lookup."""
        assert is_sftp("sftp://user@example.com//file.txt") is True
        assert is_sftp("file:///tmp/file.txt") is False

        filesystem = get_filesystem_by_uri("sftp://user@example.com//file.txt")
        assert isinstance(filesystem, SftpFileSystem)

    async def test_parse_and_build_uri(self):
        """Test URI parse/build roundtrip."""
        filesystem = SftpFileSystem.from_uri(
            "sftp://demo:secret@example.com:2222//data.txt"
        )

        assert (
            filesystem.parse_uri("sftp://demo:secret@example.com:2222//data.txt")
            == "/data.txt"
        )
        assert (
            filesystem.build_uri("/data.txt")
            == "sftp://demo:secret@example.com:2222//data.txt"
        )
        assert (
            filesystem.build_uri("//data.txt")
            == "sftp://demo:secret@example.com:2222//data.txt"
        )
        assert (
            filesystem.parse_uri("sftp://demo:secret@example.com:2222/data.txt")
            == "data.txt"
        )

    async def test_from_uri_build_uri_does_not_leak_env_password(self, monkeypatch):
        """Test URI rendering keeps credential visibility from input URI."""
        monkeypatch.setenv("SFTP_USERNAME", "env-user")
        monkeypatch.setenv("SFTP_PASSWORD", "env-password")

        filesystem = SftpFileSystem.from_uri("sftp://demo@example.com//data.txt")
        assert filesystem._endpoint.username == "demo"
        assert filesystem._endpoint.password == "env-password"
        assert filesystem.build_uri("/data.txt") == "sftp://demo@example.com//data.txt"

        filesystem_no_user = SftpFileSystem.from_uri("sftp://example.com//data.txt")
        assert filesystem_no_user._endpoint.username == "env-user"
        assert (
            filesystem_no_user.build_uri("/data.txt") == "sftp://example.com//data.txt"
        )

    def test_get_keepalive_interval_from_env(self, monkeypatch):
        """Test keepalive interval value is loaded from environment."""
        monkeypatch.setenv("SFTP_KEEPALIVE_INTERVAL", "7.5")
        assert sftp_module._get_keepalive_interval() == pytest.approx(7.5)

    async def test_open_read_accepts_compatibility_kwargs(self, filesystem):
        """Test SFTP read open ignores legacy prefetch kwargs compatibly."""
        fs, _ = filesystem

        async with fs.open(
            "//abs.txt",
            "rb",
            block_size=1,
            max_buffer_size=1,
            block_forward=8,
            max_retries=99,
        ) as reader:
            assert await reader.read(3) == b"abs"
            assert await reader.tell() == 3

    async def test_open_read_absolute_and_relative(self, filesystem):
        """Test opening and reading absolute and home-relative files."""
        fs, _ = filesystem

        async with fs.open("/abs.txt", "rb") as reader:
            assert await reader.read() == b"absolute"

        async with fs.open("rel.txt", "rb") as reader:
            assert await reader.read() == b"relative"

    async def test_open_readline_and_seek(self, filesystem):
        """Test binary and text reads via native AsyncSSH wrappers."""
        fs, _ = filesystem

        async with fs.open("//lines.txt", "rb") as reader:
            assert await reader.readline() == b"line1\n"
            assert await reader.tell() == 6
            assert await reader.read() == b"line2\n"

        async with fs.open("//lines.txt", "r") as reader:
            assert await reader.readline() == "line1\n"
            await reader.seek(0)
            assert await reader.read(5) == "line1"

    async def test_open_write_and_append(self, filesystem):
        """Test writing and appending binary content."""
        fs, client = filesystem

        async with fs.open("//write.txt", "wb") as writer:
            assert await writer.write(b"abc") == 3

        async with fs.open("//write.txt", "ab") as writer:
            assert await writer.write(b"123") == 3

        assert client.files["/write.txt"] == b"abc123"

    async def test_scandir_and_scanfile(self, filesystem):
        """Test directory scanning and recursive file scanning."""
        fs, client = filesystem

        client.files["/dir/a.txt"] = b"A"
        client.files["/dir/sub/b.txt"] = b"B"
        client._ensure_parent_dirs("/dir/a.txt")
        client._ensure_parent_dirs("/dir/sub/b.txt")

        entries = []
        async with fs.scandir("//dir") as scanner:
            async for entry in scanner:
                entries.append(entry)

        names = sorted(entry.name for entry in entries)
        assert names == ["a.txt", "sub"]

        files = []
        async with fs.scanfile("//dir") as scanner:
            async for entry in scanner:
                files.append(entry.path)

        assert sorted(files) == ["/dir/a.txt", "/dir/sub/b.txt"]

    async def test_copy_move_remove_and_symlink(self, filesystem):
        """Test copy, move, remove, symlink, and readlink operations."""
        fs, client = filesystem

        copied_path = await fs.copy("//abs.txt", "//copied.txt")
        assert copied_path == "//copied.txt"
        assert client.files["/copied.txt"] == b"absolute"
        assert client.copy_calls == 1

        moved_path = await fs.move("//copied.txt", "//moved.txt")
        assert moved_path == "//moved.txt"
        assert "/copied.txt" not in client.files
        assert client.files["/moved.txt"] == b"absolute"

        await fs.symlink("//moved.txt", "//moved.link")
        assert await fs.is_symlink("//moved.link") is True
        assert await fs.readlink("//moved.link") == "/moved.txt"

        await fs.remove("//moved.txt")
        assert await fs.exists("//moved.txt") is False

    async def test_copy_same_path_raises(self, filesystem):
        """Test copy raises when source and destination are the same."""
        fs, _ = filesystem
        with pytest.raises(OSError):
            await fs.copy("//abs.txt", "//abs.txt")

    async def test_move_overwrite_false_raises(self, filesystem):
        """Test move raises when destination exists and overwrite is False."""
        fs, client = filesystem
        client.files["/exists.txt"] = b"existing"
        with pytest.raises(FileExistsError):
            await fs.move("//abs.txt", "//exists.txt", overwrite=False)

    async def test_remove_missing_ok(self, filesystem):
        """Test remove supports missing_ok behavior."""
        fs, _ = filesystem
        await fs.remove("//not-exists.txt", missing_ok=True)
        with pytest.raises(FileNotFoundError):
            await fs.remove("//not-exists.txt", missing_ok=False)

    async def test_upload_and_download(self, filesystem, tmp_path):
        """Test upload and download operations with callbacks."""
        fs, client = filesystem

        source = tmp_path / "upload.bin"
        source.write_bytes(b"upload-content")
        upload_progress = []
        await fs.upload(
            str(source),
            "//remote/upload.bin",
            callback=upload_progress.append,
        )
        assert client.put_calls == 1
        assert client.files["/remote/upload.bin"] == b"upload-content"
        assert sum(upload_progress) == len(b"upload-content")

        destination = tmp_path / "download" / "output.bin"
        download_progress = []
        await fs.download(
            "//remote/upload.bin",
            str(destination),
            callback=download_progress.append,
        )
        assert client.get_calls == 1
        assert destination.read_bytes() == b"upload-content"
        assert sum(download_progress) == len(b"upload-content")

    async def test_upload_and_download_directory_error(self, filesystem, tmp_path):
        """Test upload/download raise on directory inputs."""
        fs, client = filesystem

        local_dir = tmp_path / "local-dir"
        local_dir.mkdir()
        with pytest.raises(IsADirectoryError):
            await fs.upload(str(local_dir), "//remote/dir")

        client._ensure_parent_dirs("/remote-dir/file.txt")
        client.files["/remote-dir/file.txt"] = b"content"
        client.dirs.add("/remote-dir")
        with pytest.raises(IsADirectoryError):
            await fs.download("//remote-dir", str(tmp_path / "out.txt"))

    async def test_misc_methods(self, filesystem):
        """Test helper methods like mkdir, absolute, samefile, and access."""
        fs, client = filesystem
        _ = client

        await fs.mkdir("//new/dir", parents=True, exist_ok=True)
        assert await fs.is_dir("//new/dir") is True

        assert await fs.absolute("rel.txt") == "/home/test/rel.txt"
        assert await fs.is_absolute("/abs.txt") is True
        assert await fs.is_absolute("rel.txt") is False
        assert await fs.samefile("//abs.txt", "//abs.txt") is True
        assert await fs.samefile("//abs.txt", "//notfound.txt") is False

        assert await fs.access("//abs.txt", mode=Access.READ) is True
        assert await fs.access("//abs.txt", mode=Access.WRITE) is True
        assert await fs.access("//notfound.txt", mode=Access.READ) is False
        with pytest.raises(TypeError):
            await fs.access("//abs.txt", mode="bad")  # type: ignore[arg-type]

    async def test_scanfile_on_single_file(self, filesystem):
        """Test scanfile returns one entry when input path is a file."""
        fs, _ = filesystem
        entries = []
        async with fs.scanfile("//abs.txt") as scanner:
            async for entry in scanner:
                entries.append(entry.path)
        assert entries == ["//abs.txt"]

    async def test_open_invalid_mode_raises(self, filesystem):
        """Test invalid open mode raises ValueError."""
        fs, _ = filesystem
        with pytest.raises(ValueError):
            fs.open("//abs.txt", "r+")


class TestSftpClientCache:
    """Test cases for cached ``get_sftp_client``."""

    async def test_get_sftp_client_cache_and_recreate(self, monkeypatch):
        """Test client cache hit and recreation after close."""
        sftp_module._SFTP_CLIENT_CACHE.clear()
        sftp_module._SFTP_CLIENT_LOCKS.clear()

        class _Conn:
            def __init__(self) -> None:
                self._closed = False

            def is_closed(self) -> bool:
                return self._closed

            def close(self) -> None:
                self._closed = True

            async def wait_closed(self) -> None:
                return None

        class _Client:
            def exit(self) -> None:
                return None

        calls = {"count": 0}

        async def _fake_get(endpoint):
            _ = endpoint
            calls["count"] += 1
            return _Conn(), _Client()

        monkeypatch.setattr(sftp_module, "_get_sftp_client", _fake_get)

        pair1 = await get_sftp_client("cache-host", username="demo")
        pair2 = await get_sftp_client("cache-host", username="demo")
        assert pair1 is pair2
        assert calls["count"] == 1

        pair1[0].close()
        pair3 = await get_sftp_client("cache-host", username="demo")
        assert pair3 is not pair1
        assert calls["count"] == 2

    async def test_same_endpoint_ignores_uri_visibility_flags(self):
        """Test same_endpoint compares connection endpoint only."""
        fs1 = SftpFileSystem(
            host="same-host",
            username="demo",
            password="secret",
            show_password_in_uri=False,
        )
        fs2 = SftpFileSystem(
            host="same-host",
            username="demo",
            password="secret",
            show_password_in_uri=True,
        )
        assert fs1.same_endpoint(fs2) is True

    def test_build_sftp_connect_lock_path_uses_random_slot(self, monkeypatch):
        """Test lock path uses host-port and randomized slot."""
        endpoint_a = sftp_module._SftpEndpoint(
            host="example.com",
            port=22,
            username="u1",
            password="p1",
        )
        endpoint_b = sftp_module._SftpEndpoint(
            host="example.com",
            port=22,
            username="u2",
            password="p2",
        )
        endpoint_c = sftp_module._SftpEndpoint(
            host="example.com",
            port=2222,
            username="u1",
            password="p1",
        )

        calls = []

        monkeypatch.setattr(sftp_module, "_get_sftp_max_unauth_connections", lambda: 4)

        def _fake_randint(start: int, end: int) -> int:
            calls.append((start, end))
            return 2

        monkeypatch.setattr(sftp_module.random, "randint", _fake_randint)

        lock_path_a = sftp_module._build_sftp_connect_lock_path(endpoint_a)
        lock_path_b = sftp_module._build_sftp_connect_lock_path(endpoint_b)
        lock_path_c = sftp_module._build_sftp_connect_lock_path(endpoint_c)

        assert lock_path_a == lock_path_b
        assert lock_path_a != lock_path_c
        assert lock_path_a.endswith(".lock")
        assert calls == [(1, 4), (1, 4), (1, 4)]

    def test_build_sftp_connect_lock_path_slot_changes(self, monkeypatch):
        """Test different random slots produce different lock files."""
        endpoint = sftp_module._SftpEndpoint(host="example.com", port=22)
        monkeypatch.setattr(sftp_module, "_get_sftp_max_unauth_connections", lambda: 4)

        slots = iter([1, 2])
        monkeypatch.setattr(
            sftp_module.random,
            "randint",
            lambda start, end: next(slots),
        )

        lock_path_slot_1 = sftp_module._build_sftp_connect_lock_path(endpoint)
        lock_path_slot_2 = sftp_module._build_sftp_connect_lock_path(endpoint)
        assert lock_path_slot_1 != lock_path_slot_2

    def test_acquire_lock_file_uses_valid_text_mode(self, tmp_path: Path) -> None:
        """Test lock file acquisition opens a writable file descriptor."""
        lock_path = tmp_path / "connect.lock"

        lock_file = sftp_module._acquire_lock_file(str(lock_path))
        try:
            assert lock_path.exists() is True
            assert isinstance(lock_file, int)
            os.write(lock_file, b"locked")
            os.fsync(lock_file)
            assert lock_path.read_text(encoding="utf-8") == "locked"
        finally:
            sftp_module._release_lock_file(lock_file)

    def test_get_sftp_max_unauth_connections_from_env(self, monkeypatch):
        """Test env parsing for max unauthenticated connection slots."""
        monkeypatch.setenv("SFTP_MAX_UNAUTH_CONNECTIONS", "8")
        assert sftp_module._get_sftp_max_unauth_connections() == 8

        monkeypatch.setenv("SFTP_MAX_UNAUTH_CONNECTIONS", "0")
        assert sftp_module._get_sftp_max_unauth_connections() == 1

        monkeypatch.setenv("SFTP_MAX_UNAUTH_CONNECTIONS", "invalid")
        assert (
            sftp_module._get_sftp_max_unauth_connections()
            == sftp_module.SFTP_DEFAULT_MAX_UNAUTH_CONNECTIONS
        )

    async def test_get_sftp_client_creates_connection_under_file_lock(
        self, monkeypatch
    ):
        """Test `_get_sftp_client` wraps connection creation with file lock."""
        events = []
        captured_kwargs = {}

        class _Client:
            def exit(self) -> None:
                return None

        class _Connection:
            def __init__(self) -> None:
                self._closed = False

            def is_closed(self) -> bool:
                return self._closed

            def close(self) -> None:
                self._closed = True

            async def wait_closed(self) -> None:
                return None

            async def start_sftp_client(self):
                events.append("start_sftp_client")
                return _Client()

        async def _fake_connect(**kwargs):
            captured_kwargs.update(kwargs)
            events.append("connect")
            return _Connection()

        @asynccontextmanager
        async def _fake_file_lock(endpoint):
            _ = endpoint
            events.append("lock_enter")
            try:
                yield
            finally:
                events.append("lock_exit")

        monkeypatch.setattr(sftp_module.asyncssh, "connect", _fake_connect)
        monkeypatch.setattr(sftp_module, "_sftp_connect_file_lock", _fake_file_lock)

        monkeypatch.setenv("SFTP_KEEPALIVE_INTERVAL", "7.5")
        endpoint = sftp_module._SftpEndpoint(
            host="locked-host",
            port=22,
        )
        connection, client = await sftp_module._get_sftp_client(endpoint, max_retries=1)

        assert connection is not None
        assert client is not None
        assert events == [
            "lock_enter",
            "connect",
            "lock_exit",
            "start_sftp_client",
        ]
        assert captured_kwargs["keepalive_interval"] == pytest.approx(7.5)
