"""Tests for HdfsFileSystem and HDFS helpers."""

from __future__ import annotations

import types

import pytest

from aiomegfile.errors.hdfs import (
    HdfsConfigError,
    HdfsFileExistsError,
    HdfsFileNotFoundError,
    HdfsInvalidError,
    HdfsIsADirectoryError,
    HdfsNotADirectoryError,
    HdfsSameFileError,
    HdfsUnsupportedError,
)
from aiomegfile.filesystem.hdfs import (
    HdfsFileSystem,
    get_hdfs_client,
    get_hdfs_config,
    is_hdfs,
)
from aiomegfile.interfaces import Access, get_filesystem_by_uri
from tests.utils.fake_hdfs import FakeHdfsClient, FakeHdfsError


@pytest.fixture
def fake_hdfs_api():
    """Return a fake ``hdfs`` module namespace for config/client tests.

    :return: Fake HDFS API namespace.
    :rtype: types.SimpleNamespace
    """

    class FakeInsecureClient:
        """Fake insecure client constructor."""

        def __init__(self, **kwargs) -> None:
            """Store init kwargs for assertions.

            :param kwargs: Client kwargs.
            """
            self.kwargs = kwargs

    class FakeTokenClient(FakeInsecureClient):
        """Fake token client constructor."""

    return types.SimpleNamespace(
        HdfsError=FakeHdfsError,
        InsecureClient=FakeInsecureClient,
        TokenClient=FakeTokenClient,
    )


@pytest.fixture
def filesystem(monkeypatch):
    """Create an HdfsFileSystem backed by a fake client.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of filesystem and fake client.
    :rtype: tuple[HdfsFileSystem, FakeHdfsClient]
    """
    client = FakeHdfsClient(root="/workspace")
    client._store_file("docs/readme.txt", b"hello\nworld\n")
    client._store_file("docs/data.json", b'{"k": 1}')
    client.makedirs("docs/sub")
    client._store_file("docs/sub/a.txt", b"A")

    filesystem = HdfsFileSystem()
    monkeypatch.setattr(
        "aiomegfile.filesystem.hdfs.get_hdfs_client",
        lambda _=None: client,
    )
    return filesystem, client


class TestHdfsHelpers:
    """Test helper functions for HDFS configuration and detection."""

    def test_is_hdfs(self) -> None:
        """Test HDFS URI detection."""
        assert is_hdfs("hdfs://data/file.txt") is True
        assert is_hdfs("hdfs+prod://data/file.txt") is True
        assert is_hdfs("file:///tmp/file.txt") is False

    def test_get_hdfs_config_from_env(self, monkeypatch) -> None:
        """Test HDFS config loading from environment variables.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        monkeypatch.setenv("HDFS_USER", "alice")
        monkeypatch.setenv("HDFS_URL", "http://localhost:9870")
        monkeypatch.setenv("HDFS_ROOT", "/data")
        monkeypatch.setenv("HDFS_TIMEOUT", "8")
        monkeypatch.setenv("HDFS_TOKEN", "token")

        assert get_hdfs_config() == {
            "user": "alice",
            "url": "http://localhost:9870",
            "root": "/data",
            "timeout": 8,
            "token": "token",
        }

    def test_get_hdfs_config_from_file(self, tmp_path, monkeypatch) -> None:
        """Test HDFS config loading from config file fallback.

        :param tmp_path: Pytest temporary path fixture.
        :param monkeypatch: Pytest monkeypatch fixture.
        """
        config_path = tmp_path / "hdfscli.cfg"
        config_path.write_text(
            "[global]\n"
            "default.alias = default\n\n"
            "[default.alias]\n"
            "user = bob\n"
            "url = http://localhost:9870\n"
            "root = /warehouse\n"
            "timeout = 12\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("HDFS_CONFIG_PATH", str(config_path))

        assert get_hdfs_config() == {
            "user": "bob",
            "url": "http://localhost:9870",
            "root": "/warehouse",
            "timeout": 12,
            "token": None,
        }

    def test_get_hdfs_config_profile_and_error(self, tmp_path, monkeypatch) -> None:
        """Test profile-based config loading and missing-config error branch.

        :param tmp_path: Pytest temporary path fixture.
        :param monkeypatch: Pytest monkeypatch fixture.
        """
        config_path = tmp_path / "hdfscli.cfg"
        monkeypatch.setenv("HDFS_CONFIG_PATH", str(config_path))

        monkeypatch.setenv("DEMO__HDFS_URL", "http://localhost:9870")
        monkeypatch.setenv("DEMO__HDFS_ROOT", "/profile")
        assert get_hdfs_config("demo")["root"] == "/profile"

        monkeypatch.delenv("DEMO__HDFS_URL")
        monkeypatch.delenv("DEMO__HDFS_ROOT")
        with pytest.raises(HdfsConfigError):
            get_hdfs_config("demo")

    def test_get_hdfs_client(self, monkeypatch, fake_hdfs_api) -> None:
        """Test HDFS client selection from config.

        :param monkeypatch: Pytest monkeypatch fixture.
        :param fake_hdfs_api: Fake HDFS API namespace.
        """
        monkeypatch.setenv("HDFS_URL", "http://localhost:9870")
        monkeypatch.setenv("HDFS_USER", "alice")
        monkeypatch.setenv("HDFS_ROOT", "/data")
        monkeypatch.setattr("aiomegfile.filesystem.hdfs.hdfs_api", fake_hdfs_api)
        get_hdfs_client.cache_clear()

        insecure_client = get_hdfs_client()
        assert insecure_client.kwargs["user"] == "alice"

        monkeypatch.setenv("HDFS_TOKEN", "secret")
        get_hdfs_client.cache_clear()
        token_client = get_hdfs_client()
        assert "user" not in token_client.kwargs
        assert token_client.kwargs["token"] == "secret"

    def test_get_hdfs_client_missing_dependency(self, monkeypatch) -> None:
        """Test missing HDFS dependency raises HDFS config error.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        monkeypatch.setattr("aiomegfile.filesystem.hdfs.hdfs_api", None)
        get_hdfs_client.cache_clear()
        with pytest.raises(HdfsConfigError):
            get_hdfs_client()

    async def test_get_filesystem_by_uri(self) -> None:
        """Test filesystem registry lookup for HDFS URIs."""
        filesystem = get_filesystem_by_uri("hdfs://data/file.txt")
        assert isinstance(filesystem, HdfsFileSystem)


class TestHdfsFileSystem:
    """Test cases for HdfsFileSystem."""

    async def test_parse_and_build_uri(self) -> None:
        """Test HDFS URI parse/build roundtrip."""
        filesystem = HdfsFileSystem.from_uri("hdfs+demo://dir/file.txt")

        assert filesystem.parse_uri("hdfs://dir/file.txt") == "dir/file.txt"
        assert filesystem.parse_uri("hdfs:///dir/file.txt") == "/dir/file.txt"
        assert filesystem.build_uri("dir/file.txt") == "hdfs+demo://dir/file.txt"
        assert filesystem.build_uri("/dir/file.txt") == "hdfs+demo:///dir/file.txt"

        with pytest.raises(HdfsInvalidError):
            filesystem.parse_uri("file:///tmp/file.txt")

        with pytest.raises(HdfsInvalidError):
            HdfsFileSystem.from_uri("file:///tmp/file.txt")

        assert filesystem.parse_uri("relative/file.txt") == "relative/file.txt"

    async def test_open_read_and_scandir(self, filesystem) -> None:
        """Test opening and reading content plus directory scanning.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, _client = filesystem

        async with fs.open("docs/readme.txt", "rb") as reader:
            assert await reader.read() == b"hello\nworld\n"

        names = []
        async with fs.scandir("docs") as scanner:
            async for entry in scanner:
                names.append(entry.name)
        assert names == ["data.json", "readme.txt", "sub"]

    async def test_open_write_append_and_scanfile(self, filesystem) -> None:
        """Test writing, appending, and recursive scanfile.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, client = filesystem

        async with fs.open("docs/output.txt", "wb") as writer:
            assert await writer.write(b"abc") == 3

        async with fs.open("docs/output.txt", "ab") as writer:
            assert await writer.write(b"123") == 3

        assert client.files["/workspace/docs/output.txt"] == b"abc123"

        files = []
        async with fs.scanfile("docs") as scanner:
            async for entry in scanner:
                files.append(entry.path)
        assert files == [
            "/workspace/docs/data.json",
            "/workspace/docs/output.txt",
            "/workspace/docs/readme.txt",
            "/workspace/docs/sub/a.txt",
        ]

    async def test_scanfile_single_file_and_scandir_errors(self, filesystem) -> None:
        """Test single-file scan and directory validation errors.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, _client = filesystem

        entries = []
        async with fs.scanfile("docs/readme.txt") as scanner:
            async for entry in scanner:
                entries.append(entry.path)
        assert entries == ["docs/readme.txt"]

        with pytest.raises(HdfsNotADirectoryError):
            async with fs.scandir("docs/readme.txt") as scanner:
                async for _ in scanner:
                    pass

    async def test_copy_move_remove(self, filesystem) -> None:
        """Test copy, move, and remove operations.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, client = filesystem

        copied = await fs.copy("docs/readme.txt", "archive/copied.txt")
        assert copied == "archive/copied.txt"
        assert client.files["/workspace/archive/copied.txt"] == b"hello\nworld\n"

        moved = await fs.move("archive/copied.txt", "archive/moved.txt")
        assert moved == "archive/moved.txt"
        assert "/workspace/archive/copied.txt" not in client.files
        assert client.files["/workspace/archive/moved.txt"] == b"hello\nworld\n"

        await fs.remove("archive/moved.txt")
        assert await fs.exists("archive/moved.txt") is False

    async def test_remove_missing_and_samefile_fallback(self, filesystem) -> None:
        """Test missing_ok removal and samefile on missing targets.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, _client = filesystem

        await fs.remove("docs/not-found.txt", missing_ok=True)
        with pytest.raises(HdfsFileNotFoundError):
            await fs.remove("docs/not-found.txt", missing_ok=False)

        assert await fs.samefile("docs/readme.txt", "docs/not-found.txt") is False
        assert await fs.is_symlink("docs/readme.txt") is False

    async def test_hdfs_specific_errors(self, filesystem) -> None:
        """Test HDFS operations raise HDFS-specific exception types.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, client = filesystem
        _ = client

        with pytest.raises(HdfsSameFileError):
            await fs.copy("docs/readme.txt", "docs/readme.txt")

        with pytest.raises(HdfsFileNotFoundError):
            await fs.copy("docs/missing.txt", "archive/missing.txt")

        with pytest.raises(HdfsIsADirectoryError):
            await fs.copy("docs", "archive/docs")

        with pytest.raises(HdfsFileExistsError):
            await fs.mkdir("docs", exist_ok=False)

        with pytest.raises(HdfsFileExistsError):
            await fs.move("docs/readme.txt", "docs/data.json", overwrite=False)

    async def test_upload_download_and_misc(self, filesystem, tmp_path) -> None:
        """Test upload/download and helper methods.

        :param filesystem: Fake HDFS filesystem fixture.
        :param tmp_path: Pytest temporary path fixture.
        """
        fs, client = filesystem
        source = tmp_path / "local.bin"
        source.write_bytes(b"payload")

        uploaded = []
        await fs.upload(str(source), "uploads/local.bin", callback=uploaded.append)
        assert client.files["/workspace/uploads/local.bin"] == b"payload"
        assert sum(uploaded) == len(b"payload")

        target = tmp_path / "downloads" / "out.bin"
        downloaded = []
        await fs.download("uploads/local.bin", str(target), callback=downloaded.append)
        assert target.read_bytes() == b"payload"
        assert sum(downloaded) == len(b"payload")

        with pytest.raises(HdfsIsADirectoryError):
            await fs.download("docs", str(tmp_path / "dir.bin"))

        await fs.mkdir("nested/dir", parents=True, exist_ok=True)
        assert await fs.is_dir("nested/dir") is True
        assert await fs.absolute("docs/readme.txt") == "/workspace/docs/readme.txt"
        assert await fs.is_absolute("/workspace/docs/readme.txt") is True
        assert await fs.is_absolute("docs/readme.txt") is False
        assert (
            await fs.samefile("docs/readme.txt", "/workspace/docs/readme.txt") is True
        )
        assert await fs.is_dir("docs/not-found.txt") is False
        assert await fs.is_file("docs/not-found.txt") is False
        assert await fs.md5("docs") == "8d37b2ee0fc0641c66c735b778084765"
        assert await fs.md5("uploads/local.bin") == "321c3cf486ed509164edec1e1981fec8"
        assert await fs.access("uploads/local.bin", mode=Access.READ) is True
        with pytest.raises(HdfsInvalidError):
            await fs.access("uploads/local.bin", mode="bad")  # type: ignore[arg-type]

    async def test_open_text_and_writer_helpers(self, filesystem) -> None:
        """Test text writer helpers and edge branches.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, client = filesystem

        writer = await fs.open("docs/text.txt", "w").__aenter__()
        assert writer.name == "hdfs://docs/text.txt"
        assert writer.mode == "w"
        assert await writer.write("abc") == 3
        await writer.flush()
        assert await writer.tell() == 3
        await writer.close()
        assert client.files["/workspace/docs/text.txt"] == b"abc"

        writer = await fs.open("docs/bin.txt", "wb").__aenter__()
        with pytest.raises(HdfsInvalidError):
            await writer.write("bad")  # type: ignore[arg-type]

        class DummyBinaryWriter:
            """Dummy writer object for edge-case branches."""

            def write(self, data: bytes) -> None:
                """Pretend to write data and return ``None``.

                :param data: Bytes to write.
                """
                _ = data
                return None

        writer._file = DummyBinaryWriter()
        assert await writer.write(b"xy") == 2
        writer._file = object()
        assert await writer.flush() is None
        assert await writer.tell() == 2
        await writer.close()

        writer = fs.open("docs/closed.txt", "wb")
        with pytest.raises(IOError):
            await writer.write(b"x")

    def test_same_endpoint_branches(self, monkeypatch) -> None:
        """Test HDFS endpoint comparison branches.

        :param monkeypatch: Pytest monkeypatch fixture.
        """
        assert HdfsFileSystem("demo").same_endpoint(HdfsFileSystem("demo")) is True
        assert HdfsFileSystem("demo").same_endpoint(object()) is False

        fs1 = HdfsFileSystem("a")
        fs2 = HdfsFileSystem("b")
        monkeypatch.setattr(
            "aiomegfile.filesystem.hdfs.get_hdfs_config",
            lambda profile_name=None: {"url": f"http://{profile_name}"},
        )
        assert fs1.same_endpoint(fs2) is False

        monkeypatch.setattr(
            "aiomegfile.filesystem.hdfs.get_hdfs_config",
            lambda profile_name=None: {"url": "http://same"},
        )
        assert fs1.same_endpoint(fs2) is True

        def _raise(*args, **kwargs):
            """Raise an error for suppressed branch coverage."""
            _ = args, kwargs
            raise RuntimeError("boom")

        monkeypatch.setattr("aiomegfile.filesystem.hdfs.get_hdfs_config", _raise)
        assert fs1.same_endpoint(fs2) is False

    async def test_open_invalid_mode_raises(self, filesystem) -> None:
        """Test invalid open mode raises ``HdfsInvalidError``.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, _client = filesystem
        with pytest.raises(HdfsInvalidError):
            fs.open("docs/readme.txt", "r+")

    async def test_unsupported_operations_raise_hdfs_errors(self, filesystem) -> None:
        """Test unsupported HDFS operations raise HDFS-specific errors.

        :param filesystem: Fake HDFS filesystem fixture.
        """
        fs, _client = filesystem

        with pytest.raises(HdfsUnsupportedError):
            await fs.symlink("docs/readme.txt", "docs/link.txt")

        with pytest.raises(HdfsUnsupportedError):
            await fs.readlink("docs/link.txt")
