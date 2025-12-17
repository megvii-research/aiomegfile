import os

import aiofiles
import pytest

from aiomegfile.interfaces import BaseFileSystem
from aiomegfile.lib.url import split_uri
from aiomegfile.smart_path import SmartPath, URIPathParents


@pytest.fixture
def filesystem_registry_snapshot():
    from aiomegfile.interfaces import FILE_SYSTEMS

    snapshot = dict(FILE_SYSTEMS)
    yield snapshot
    FILE_SYSTEMS.clear()
    FILE_SYSTEMS.update(snapshot)


def _register_dummy_filesystem():
    class DummyFileSystem(BaseFileSystem):
        protocol = "dummy"

        def __init__(self, protocol_in_path: bool = True):
            self.protocol_in_path = protocol_in_path

        def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
            return False

        def get_path_from_uri(self, uri: str) -> str:
            _, path, _ = split_uri(uri)
            return path

        def generate_uri(self, path: str) -> str:
            if not self.protocol_in_path:
                return path
            return super().generate_uri(path)

        @classmethod
        def from_uri(cls, uri: str) -> "DummyFileSystem":
            return cls(protocol_in_path="dummy://" in str(uri))

        def open(
            self,
            path: str,
            mode: str = "r",
            buffering: int = -1,
            encoding=None,
            errors=None,
            newline=None,
        ):
            return aiofiles.open(
                path,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )

    return DummyFileSystem


def test_uri_path_parents_with_file_prefix():
    p = SmartPath("file://foo/bar/baz")
    parents = URIPathParents(p)
    assert len(parents) == 3
    assert str(parents[0]) == "file://foo/bar"
    assert str(parents[1]) == "file://foo"
    assert str(parents[2]) == "file://"

    single = SmartPath("file://foo")
    single_parents = URIPathParents(single)
    assert len(single_parents) == 1
    assert str(single_parents[0]) == "file://"
    assert str(single.parent) == "file://"

    absolute = SmartPath("/absolute/path")
    absolute_parents = URIPathParents(absolute)
    assert len(absolute_parents) == 2
    assert str(absolute_parents[0]) == "/absolute"
    assert str(absolute_parents[1]) == "/"

    absolute_with_protocol = SmartPath("file:///absolute/path")
    absolute_with_protocol_parents = URIPathParents(absolute_with_protocol)
    assert len(absolute_with_protocol_parents) == 2
    assert str(absolute_with_protocol_parents[0]) == "file:///absolute"
    assert str(absolute_with_protocol_parents[1]) == "file:///"

    relative = SmartPath("relative/path/to/file")
    relative_parents = URIPathParents(relative)
    assert len(relative_parents) == 4
    assert str(relative_parents[0]) == "relative/path/to"
    assert str(relative_parents[1]) == "relative/path"
    assert str(relative_parents[2]) == "relative"
    assert str(relative_parents[3]) == ""

    empty = SmartPath("")
    empty_parents = URIPathParents(empty)
    assert len(empty_parents) == 0

    with pytest.raises(IndexError):
        empty_parents[0]

    empty_with_protocol = SmartPath("file://")
    empty_with_protocol_parents = URIPathParents(empty_with_protocol)
    assert len(empty_with_protocol_parents) == 0
    with pytest.raises(IndexError):
        empty_with_protocol_parents[0]

    root_with_protocol = SmartPath("file:///")
    root_with_protocol_parents = URIPathParents(root_with_protocol)
    assert len(root_with_protocol_parents) == 0
    with pytest.raises(IndexError):
        root_with_protocol_parents[0]

    root_without_protocol = SmartPath("/")
    root_without_protocol_parents = URIPathParents(root_without_protocol)
    assert len(root_without_protocol_parents) == 0
    with pytest.raises(IndexError):
        root_without_protocol_parents[0]


async def test_comparisons_between_registered_protocols_raise_typeerror(
    filesystem_registry_snapshot,
):
    _register_dummy_filesystem()

    p_file = SmartPath("file://foo")
    p_dummy = SmartPath("dummy://bar")

    with pytest.raises(TypeError):
        _ = p_file == p_dummy
    with pytest.raises(TypeError):
        _ = p_file < p_dummy
    with pytest.raises(TypeError):
        _ = p_file <= p_dummy
    with pytest.raises(TypeError):
        _ = p_file > p_dummy
    with pytest.raises(TypeError):
        _ = p_file >= p_dummy
    with pytest.raises(TypeError):
        _ = p_file / p_dummy


async def test_relative_to_error_branches(filesystem_registry_snapshot):
    _register_dummy_filesystem()

    p_file = SmartPath("file://foo/bar")
    with pytest.raises(ValueError, match="other is required"):
        await p_file.relative_to("")

    p_dummy = SmartPath("dummy://foo/bar")
    with pytest.raises(ValueError, match="relative_to"):
        await p_file.relative_to(p_dummy)

    p_dummy2 = SmartPath("dummy://other/bar")
    with pytest.raises(ValueError, match="different endpoints"):
        await p_dummy.relative_to(p_dummy2)


async def test_resolve_strict_self_referential_symlink_raises(tmp_path):
    link_path = tmp_path / "loop"
    os.symlink(str(link_path), str(link_path))

    p = SmartPath(str(link_path))
    with pytest.raises(OSError):
        await p.resolve(strict=True)
    resolved = await p.resolve(strict=False)
    assert os.path.isabs(str(resolved))


async def test_samefile_with_different_protocol_returns_false(
    filesystem_registry_snapshot, tmp_path
):
    _register_dummy_filesystem()

    src = tmp_path / "a.txt"
    src.write_text("x")
    p_src = SmartPath(str(src))
    assert await p_src.samefile("dummy://whatever") is False


async def test_touch_exist_ok_false_raises(tmp_path):
    file_path = tmp_path / "exists.txt"
    file_path.write_text("data")
    p = SmartPath(str(file_path))
    with pytest.raises(FileExistsError):
        await p.touch(exist_ok=False)


async def test_iglob_raises_not_implemented(tmp_path):
    p = SmartPath(str(tmp_path))
    agen = p.iglob("*.txt")
    with pytest.raises(NotImplementedError):
        await agen.__anext__()


async def test_glob_collects_from_iglob(monkeypatch, tmp_path):
    async def fake_iglob(self, pattern):
        yield self.from_uri("/tmp/a")
        yield self.from_uri("/tmp/b")

    monkeypatch.setattr(SmartPath, "iglob", fake_iglob)
    p = SmartPath(str(tmp_path))
    results = await p.glob("*.txt")
    assert [str(r) for r in results] == ["/tmp/a", "/tmp/b"]


async def test_rglob_prefixes_pattern(monkeypatch, tmp_path):
    called = []

    async def fake_glob(self, pattern):
        called.append(pattern)
        return []

    monkeypatch.setattr(SmartPath, "glob", fake_glob)
    p = SmartPath(str(tmp_path))
    await p.rglob("")
    await p.rglob("/foo.txt")
    assert called == ["**/", "**/foo.txt"]


async def test_copy_cross_protocol_fallback_streaming(
    filesystem_registry_snapshot, tmp_path
):
    _register_dummy_filesystem()

    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst = tmp_path / "dst.txt"

    p_src = SmartPath(str(src))
    p_dst = SmartPath(f"dummy://{dst}")

    await p_src._copy_file(target=p_dst)
    assert dst.read_text() == "hello"

    # reverse direction hits the download branch before falling back
    dst2 = tmp_path / "dst2.txt"
    p_src_dummy = SmartPath(f"dummy://{src}")
    p_dst_file = SmartPath(str(dst2))
    await p_src_dummy._copy_file(target=p_dst_file)
    assert dst2.read_text() == "hello"


async def test_copy_follow_symlinks_resolves_link(tmp_path):
    src = tmp_path / "real.txt"
    src.write_text("content")
    link = tmp_path / "link.txt"
    os.symlink(src, link)
    dst = tmp_path / "copied.txt"

    p_link = SmartPath(str(link))
    await p_link.copy(str(dst), follow_symlinks=True)
    assert dst.read_text() == "content"


async def test_move_and_move_into(tmp_path):
    src = tmp_path / "a.txt"
    src.write_text("x")
    dst = tmp_path / "b.txt"

    p_src = SmartPath(str(src))
    moved = await p_src.move(str(dst))
    assert str(moved) == str(dst)
    assert dst.exists()
    assert not src.exists()

    src2 = tmp_path / "c.txt"
    src2.write_text("y")
    p_src2 = SmartPath(str(src2))
    target_dir = tmp_path / "subdir"
    moved_into = await p_src2.move_into(str(target_dir))
    assert str(moved_into) == str(target_dir / "c.txt")
    assert (target_dir / "c.txt").exists()
    assert not src2.exists()


async def test_hardlink_to_non_file_protocol_raises(
    filesystem_registry_snapshot, tmp_path
):
    _register_dummy_filesystem()
    p = SmartPath(f"dummy://{tmp_path / 'x'}")
    with pytest.raises(NotImplementedError):
        await p.hardlink_to("dummy://target")


async def test_full_match_case_sensitive_true(tmp_path):
    p = SmartPath("file://Foo.txt")
    assert await p.full_match("file://Foo.txt", case_sensitive=True)
    assert not await p.full_match("file://foo.txt", case_sensitive=True)
