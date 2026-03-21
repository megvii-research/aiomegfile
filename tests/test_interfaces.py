import stat
import typing as T

import pytest

from aiomegfile.errors import ProtocolNotFoundError
from aiomegfile.interfaces import (
    AioClosable,
    AioReadable,
    AioWritable,
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
    from aiomegfile.utils.path import split_uri

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
        await fs.remove("x")
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


def test_get_filesystem_by_uri_protocol_not_found():
    with pytest.raises(ProtocolNotFoundError):
        get_filesystem_by_uri("unknown://bucket/key")


class DummyClosable(AioClosable):
    """Closable test double."""

    def __init__(self) -> None:
        """Initialize close counter.

        :return: None
        :rtype: None
        """
        self.close_calls = 0

    async def close(self) -> None:
        """Record close calls.

        :return: None
        :rtype: None
        """
        self.close_calls += 1


class DummyReadable(AioReadable[bytes]):
    """Readable test double."""

    def __init__(self, data: bytes, mode: str) -> None:
        """Initialize with data and mode.

        :param data: Data to expose.
        :param mode: File mode string.
        :return: None
        :rtype: None
        """
        self._data = data
        self._pos = 0
        self._mode = mode

    @property
    def name(self) -> str:
        """Return file name.

        :return: File name.
        :rtype: str
        """
        return "dummy"

    @property
    def mode(self) -> str:
        """Return file mode.

        :return: Mode string.
        :rtype: str
        """
        return self._mode

    async def read(self, size: T.Optional[int] = None) -> bytes:
        """Read bytes from the buffer.

        :param size: Max size to read.
        :return: Bytes read.
        :rtype: bytes
        """
        if size is None or size < 0:
            size = len(self._data) - self._pos
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk

    async def readline(self, size: T.Optional[int] = None) -> bytes:
        """Read one line from the buffer.

        :param size: Max bytes to read.
        :return: Line bytes.
        :rtype: bytes
        """
        if self._pos >= len(self._data):
            return b""
        remaining = self._data[self._pos :]
        newline_idx = remaining.find(b"\n")
        if newline_idx == -1:
            return await self.read(size)
        line_end = self._pos + newline_idx + 1
        if size is not None:
            line_end = min(line_end, self._pos + size)
        chunk = self._data[self._pos : line_end]
        self._pos = line_end
        return chunk

    async def tell(self) -> int:
        """Return current offset.

        :return: Current offset.
        :rtype: int
        """
        return self._pos

    async def close(self) -> None:
        """Close the readable.

        :return: None
        :rtype: None
        """
        return None


class DummyWritable(AioWritable[str]):
    """Writable test double."""

    def __init__(self) -> None:
        """Initialize buffer.

        :return: None
        :rtype: None
        """
        self.items: list[str] = []

    @property
    def name(self) -> str:
        """Return file name.

        :return: File name.
        :rtype: str
        """
        return "dummy"

    @property
    def mode(self) -> str:
        """Return file mode.

        :return: Mode string.
        :rtype: str
        """
        return "w"

    async def write(self, data: str) -> int:
        """Write data into buffer.

        :param data: Data to store.
        :return: Number of characters written.
        :rtype: int
        """
        self.items.append(data)
        return len(data)

    async def tell(self) -> int:
        """Return current stream position.

        :return: Current position.
        :rtype: int
        """
        return sum(len(item) for item in self.items)

    async def close(self) -> None:
        """Close the writable.

        :return: None
        :rtype: None
        """
        return None


async def test_aioclosable_close_is_idempotent():
    """AioClosable should wrap close to be idempotent.

    :return: None
    :rtype: None
    """
    closable = DummyClosable()
    await closable.close()
    await closable.close()
    assert closable.close_calls == 1
    assert closable.closed is True


async def test_aioreadable_readinto_binary():
    """AioReadable.readinto should fill buffer in binary mode.

    :return: None
    :rtype: None
    """
    reader = DummyReadable(b"abc", "rb")
    buffer = bytearray(3)
    size = await reader.readinto(buffer)
    assert size == 3
    assert bytes(buffer) == b"abc"


async def test_aioreadable_readinto_text_mode_raises():
    """AioReadable.readinto should reject text mode.

    :return: None
    :rtype: None
    """
    reader = DummyReadable(b"abc", "r")
    with pytest.raises(OSError):
        await reader.readinto(bytearray(3))


async def test_aiowritable_writelines():
    """AioWritable.writelines should write all items.

    :return: None
    :rtype: None
    """
    writer = DummyWritable()
    await writer.writelines(["a", "b", "c"])
    assert "".join(writer.items) == "abc"
