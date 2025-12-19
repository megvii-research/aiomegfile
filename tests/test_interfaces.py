import stat
import typing as T

import pytest

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.interfaces import (
    BaseFileSystem,
    FileEntry,
    StatResult,
    get_filesystem_by_uri,
)


@pytest.fixture
def filesystem_registry_snapshot():
    from aiomegfile.interfaces import FILE_SYSTEMS

    snapshot = dict(FILE_SYSTEMS)
    yield snapshot
    FILE_SYSTEMS.clear()
    FILE_SYSTEMS.update(snapshot)


class DummyExtra:
    st_mode = 0o123
    st_ino = 42
    st_dev = 7
    st_nlink = 3
    st_uid = 1000
    st_gid = 1001
    st_atime = 1.5
    st_atime_ns = 1_500_000_000
    st_mtime_ns = 2_500_000_000
    st_ctime_ns = 3_500_000_000


def test_statresult_uses_extra_attributes():
    sr = StatResult(extra=DummyExtra())
    assert sr.st_mode == DummyExtra.st_mode
    assert sr.st_ino == DummyExtra.st_ino
    assert sr.st_dev == DummyExtra.st_dev
    assert sr.st_nlink == DummyExtra.st_nlink
    assert sr.st_uid == DummyExtra.st_uid
    assert sr.st_gid == DummyExtra.st_gid
    assert sr.st_atime == DummyExtra.st_atime
    assert sr.st_atime_ns == DummyExtra.st_atime_ns
    assert sr.st_mtime_ns == DummyExtra.st_mtime_ns
    assert sr.st_ctime_ns == DummyExtra.st_ctime_ns


def test_statresult_fallback_modes_and_etag_inode():
    assert StatResult(islnk=True).st_mode == stat.S_IFLNK
    assert StatResult(isdir=True).st_mode == stat.S_IFDIR
    assert StatResult().st_mode == stat.S_IFREG

    sr = StatResult(extra={"ETag": '"ff"'})
    assert sr.st_ino == 255


def test_fileentry_helpers():
    fe_file = FileEntry(name="f", path="/f", stat=StatResult())
    assert fe_file.inode() == 0
    assert fe_file.is_file()
    assert not fe_file.is_dir()
    assert not fe_file.is_symlink()

    fe_dir = FileEntry(name="d", path="/d", stat=StatResult(isdir=True))
    assert fe_dir.is_dir()
    assert not fe_dir.is_file()

    fe_link = FileEntry(name="l", path="/l", stat=StatResult(islnk=True))
    assert fe_link.is_symlink()
    assert fe_link.is_file()
    assert not fe_link.is_dir()


def test_basefilesystem_subclass_validation(filesystem_registry_snapshot):
    with pytest.raises(ValueError):
        type("NoProtoFS", (BaseFileSystem,), {"protocol": ""})

    with pytest.raises(ValueError):
        type("DupProtoFS", (BaseFileSystem,), {"protocol": "file"})


async def test_basefilesystem_default_methods_raise(filesystem_registry_snapshot):
    from aiomegfile.lib.url import split_uri

    class MinimalFS(BaseFileSystem):
        protocol = "minfs"

        def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
            return True

        def parse_uri(self, uri: str) -> str:
            _, path, _ = split_uri(uri)
            return path

        def build_uri(self, path: str) -> str:
            return super().build_uri(path)

        @classmethod
        def from_uri(cls: T.Type["MinimalFS"], uri: str) -> "MinimalFS":
            return cls()

    fs = MinimalFS()
    with pytest.raises(NotImplementedError):
        await fs.is_dir("x")
    with pytest.raises(NotImplementedError):
        await fs.is_file("x")
    with pytest.raises(NotImplementedError):
        await fs.exists("x")
    with pytest.raises(NotImplementedError):
        await fs.stat("x")
    with pytest.raises(NotImplementedError):
        await fs.unlink("x")
    with pytest.raises(NotImplementedError):
        await fs.rmdir("x")
    with pytest.raises(NotImplementedError):
        await fs.mkdir("x")
    with pytest.raises(NotImplementedError):
        fs.open("x")
    with pytest.raises(NotImplementedError):
        fs.scandir("x")
    with pytest.raises(NotImplementedError):
        await fs.upload("a", "b")
    with pytest.raises(NotImplementedError):
        await fs.download("a", "b")
    with pytest.raises(NotImplementedError):
        await fs.copy("a", "b")
    with pytest.raises(NotImplementedError):
        await fs.move("a", "b")
    with pytest.raises(NotImplementedError):
        await fs.symlink("a", "b")
    with pytest.raises(NotImplementedError):
        await fs.readlink("a")
    with pytest.raises(NotImplementedError):
        await fs.is_symlink("a")
    with pytest.raises(NotImplementedError):
        await fs.absolute("a")
    with pytest.raises(NotImplementedError):
        await fs.samefile("a", "b")


def test_get_filesystem_by_uri_protocol_not_found():
    with pytest.raises(ProtocolNotFoundError):
        get_filesystem_by_uri("unknown://bucket/key")
