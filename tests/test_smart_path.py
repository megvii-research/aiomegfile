import os

import pytest

from aiomegfile.interfaces import StatResult
from aiomegfile.smart_path import SmartPath, URIPathParents, fspath


class TestFspath:
    """Tests for the fspath function."""

    def test_fspath_with_string(self):
        assert fspath("/path/to/file") == "/path/to/file"

    def test_fspath_with_bytes_path_like(self):
        """Test fspath with a bytes path-like object."""

        class BytesPathLike:
            def __fspath__(self):
                return b"/path/to/file"

        assert fspath(BytesPathLike()) == "/path/to/file"


class TestSmartPathBasic:
    """Basic tests for SmartPath initialization and properties."""

    def test_init_with_string(self):
        p = SmartPath("/tmp/test.txt")
        assert p.path == "/tmp/test.txt"
        assert p.filesystem.protocol == "file"

    def test_init_with_smart_path(self):
        p1 = SmartPath("/tmp/test.txt")
        p2 = SmartPath(p1)
        assert p2.path == p1.path

    def test_str_repr_bytes(self):
        p = SmartPath("/tmp/test.txt")
        assert str(p) == "/tmp/test.txt"
        assert repr(p) == "SmartPath('/tmp/test.txt')"
        assert bytes(p) == b"/tmp/test.txt"

    def test_fspath_protocol(self):
        p = SmartPath("/tmp/test.txt")
        assert os.fspath(p) == "/tmp/test.txt"

    def test_hash(self):
        p1 = SmartPath("/tmp/test.txt")
        p2 = SmartPath("/tmp/test.txt")
        assert hash(p1) == hash(p2)


class TestSmartPathProtocolParsing:
    """Tests for protocol parsing."""

    def test_file_protocol_implicit(self):
        p = SmartPath("/tmp/test.txt")
        assert p.filesystem.protocol == "file"
        assert p.filesystem.path_without_protocol == "/tmp/test.txt"

    def test_file_protocol_explicit(self):
        p = SmartPath("file:///tmp/test.txt")
        assert p.filesystem.protocol == "file"
        assert p.filesystem.path_without_protocol == "/tmp/test.txt"

    def test_path_with_protocol(self):
        p = SmartPath("/bucket/dir/file.txt")
        assert p.filesystem.path_with_protocol == "file:///bucket/dir/file.txt"

    def test_path_with_protocol_already_has_protocol(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.filesystem.path_with_protocol == "file:///bucket/dir/file.txt"

    def test_protocol_with_profile(self):
        # Test that profile_name is correctly parsed
        # This requires a registered protocol; file protocol doesn't use profiles
        # but we can test the parsing logic
        protocol, profile, path = SmartPath._split_path("s3+myprofile://bucket/key")
        assert protocol == "s3"
        assert profile == "myprofile"
        assert path == "bucket/key"


class TestSmartPathComparisons:
    """Tests for comparison operators."""

    def test_eq_with_same_path(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.touch()
        p1 = SmartPath(str(test_file))
        p2 = SmartPath(str(test_file))
        assert p1 == p2

    def test_eq_with_string(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.touch()
        p = SmartPath(str(test_file))
        assert p == str(test_file)

    def test_lt(self):
        p1 = SmartPath("/tmp/a.txt")
        p2 = SmartPath("/tmp/b.txt")
        assert p1 < p2

    def test_le(self):
        p1 = SmartPath("/tmp/a.txt")
        p2 = SmartPath("/tmp/b.txt")
        assert p1 <= p2
        assert p1 <= SmartPath("/tmp/a.txt")

    def test_gt(self):
        p1 = SmartPath("/tmp/b.txt")
        p2 = SmartPath("/tmp/a.txt")
        assert p1 > p2

    def test_ge(self):
        p1 = SmartPath("/tmp/b.txt")
        p2 = SmartPath("/tmp/a.txt")
        assert p1 >= p2
        assert p1 >= SmartPath("/tmp/b.txt")

    def test_comparison_different_protocols_raises(self, tmp_path):
        from aiomegfile.errors import ProtocolNotFoundError

        test_file = tmp_path / "a.txt"
        test_file.touch()
        p1 = SmartPath(str(test_file))

        # s3 protocol is not registered, so this should raise ProtocolNotFoundError
        with pytest.raises(ProtocolNotFoundError):
            p1 < "s3://bucket/key"


class TestSmartPathParts:
    """Tests for path component properties."""

    def test_parts(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.parts == ("file://", "bucket", "dir", "file.txt")

    def test_parts_root_only(self):
        p = SmartPath("file:///")
        assert p.parts == ("file://",)

    def test_name(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.name == "file.txt"

    def test_name_root(self):
        p = SmartPath("file://")
        assert p.name == ""

    def test_suffix(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.suffix == ".txt"

    def test_suffix_no_extension(self):
        p = SmartPath("file:///bucket/dir/file")
        assert p.suffix == ""

    def test_suffixes(self):
        p = SmartPath("file:///bucket/dir/file.tar.gz")
        assert p.suffixes == [".tar", ".gz"]

    def test_suffixes_trailing_dot(self):
        p = SmartPath("file:///bucket/dir/file.")
        assert p.suffixes == []

    def test_stem(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.stem == "file"

    def test_stem_no_extension(self):
        p = SmartPath("file:///bucket/dir/file")
        assert p.stem == "file"

    def test_root(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.root == "file://"

    def test_anchor(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert p.anchor == "file://"


class TestSmartPathParents:
    """Tests for parent and parents properties."""

    def test_parents(self):
        p = SmartPath("file:///bucket/dir/sub/file")
        parents = p.parents
        assert len(parents) == 3
        assert str(parents[0]) == "file://bucket/dir/sub"
        assert str(parents[1]) == "file://bucket/dir"
        assert str(parents[2]) == "file://bucket"

    def test_parents_root(self):
        p = SmartPath("file:///")
        parents = p.parents
        assert len(parents) == 0

    def test_parent(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert str(p.parent) == str(p.parents[0])

    def test_parent_root(self):
        p = SmartPath("file:///")
        # Parent of root is itself (returns file:///)
        assert str(p.parent) == "file:///"


class TestURIPathParents:
    """Tests for URIPathParents class."""

    def test_len(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        parents = URIPathParents(p)
        assert len(parents) == 2

    def test_getitem(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        parents = URIPathParents(p)
        assert str(parents[0]) == "file://bucket/dir"
        assert str(parents[1]) == "file://bucket"

    def test_getitem_out_of_range(self):
        p = SmartPath("file:///bucket/file.txt")
        parents = URIPathParents(p)
        with pytest.raises(IndexError):
            parents[10]


class TestSmartPathAsync:
    """Tests for async methods."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for testing."""
        tmpdir = tmp_path / "test_dir"
        tmpdir.mkdir()
        return str(tmpdir)

    async def test_as_uri(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert await p.as_uri() == "file:///bucket/dir/file.txt"

    async def test_as_posix(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert await p.as_posix() == "file:///bucket/dir/file.txt"

    async def test_joinpath(self):
        p = SmartPath("file:///bucket/dir")
        joined = await p.joinpath("sub", "file.txt")
        assert str(joined) == "file:///bucket/dir/sub/file.txt"

    async def test_joinpath_empty(self):
        p = SmartPath("file:///bucket/dir")
        joined = await p.joinpath()
        assert str(joined) == str(p)

    async def test_joinpath_with_leading_slash(self):
        p = SmartPath("file:///bucket/dir")
        joined = await p.joinpath("/sub/", "/file.txt")
        assert "sub" in str(joined)
        assert "file.txt" in str(joined)

    async def test_relative_to(self):
        p = SmartPath("file:///bucket/dir/sub/file.txt")
        rel = await p.relative_to("file:///bucket/dir")
        assert str(rel) == "sub/file.txt"

    async def test_relative_to_raises_on_no_args(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        with pytest.raises(TypeError):
            await p.relative_to()

    async def test_relative_to_raises_on_not_relative(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        with pytest.raises(ValueError):
            await p.relative_to("file:///other/path")

    async def test_is_relative_to(self):
        p = SmartPath("file:///bucket/dir/sub/file.txt")
        assert await p.is_relative_to("file:///bucket/dir")
        assert not await p.is_relative_to("file:///other/path")

    async def test_with_name(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        new_p = await p.with_name("newfile.txt")
        assert str(new_p) == "file:///bucket/dir/newfile.txt"

    async def test_with_stem(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        new_p = await p.with_stem("newfile")
        assert str(new_p) == "file:///bucket/dir/newfile.txt"

    async def test_with_suffix(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        new_p = await p.with_suffix(".md")
        assert str(new_p) == "file:///bucket/dir/file.md"

    async def test_match(self):
        p = SmartPath("file:///bucket/dir/sub/file.txt")
        assert await p.match("*.txt")
        assert await p.match("**/file.txt")
        assert not await p.match("*.md")

    async def test_samefile(self, temp_dir):
        test_file = os.path.join(temp_dir, "samefile.txt")
        with open(test_file, "w") as f:
            f.write("test")
        p1 = SmartPath(test_file)
        p2 = SmartPath(test_file)
        assert await p1.samefile(p2)

    async def test_full_match(self):
        p = SmartPath("file:///bucket/dir/file.txt")
        assert await p.full_match("**/file.txt")


class TestSmartPathFileOperations:
    """Tests for file operations with real filesystem."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for testing."""
        tmpdir = tmp_path / "test_dir"
        tmpdir.mkdir()
        return str(tmpdir)

    async def test_exists(self, temp_dir):
        # Create a test file
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        p = SmartPath(test_file)
        assert await p.exists()

        p_not_exists = SmartPath(os.path.join(temp_dir, "not_exists.txt"))
        assert not await p_not_exists.exists()

    async def test_is_file(self, temp_dir):
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        p = SmartPath(test_file)
        assert await p.is_file()

    async def test_is_dir(self, temp_dir):
        p = SmartPath(temp_dir)
        assert await p.is_dir()

    async def test_mkdir(self, temp_dir):
        new_dir = os.path.join(temp_dir, "new_dir")
        p = SmartPath(new_dir)
        await p.mkdir()
        assert os.path.isdir(new_dir)

    async def test_mkdir_parents(self, temp_dir):
        new_dir = os.path.join(temp_dir, "parent", "child")
        p = SmartPath(new_dir)
        await p.mkdir(parents=True)
        assert os.path.isdir(new_dir)

    async def test_touch(self, temp_dir):
        test_file = os.path.join(temp_dir, "touched.txt")
        p = SmartPath(test_file)
        await p.touch()
        assert os.path.exists(test_file)

    async def test_read_write_bytes(self, temp_dir):
        test_file = os.path.join(temp_dir, "bytes.bin")
        p = SmartPath(test_file)
        data = b"hello world"
        await p.write_bytes(data)
        assert await p.read_bytes() == data

    async def test_read_write_text(self, temp_dir):
        test_file = os.path.join(temp_dir, "text.txt")
        p = SmartPath(test_file)
        data = "hello world"
        await p.write_text(data)
        assert await p.read_text() == data

    async def test_unlink(self, temp_dir):
        test_file = os.path.join(temp_dir, "to_unlink.txt")
        with open(test_file, "w") as f:
            f.write("delete me")

        p = SmartPath(test_file)
        await p.unlink()
        assert not os.path.exists(test_file)

    async def test_unlink_directory_raises(self, temp_dir):
        p = SmartPath(temp_dir)
        with pytest.raises(IsADirectoryError):
            await p.unlink()

    async def test_rmdir(self, temp_dir):
        new_dir = os.path.join(temp_dir, "to_remove_dir")
        os.mkdir(new_dir)
        p = SmartPath(new_dir)
        await p.rmdir()
        assert not os.path.exists(new_dir)

    async def test_rmdir_file_raises(self, temp_dir):
        test_file = os.path.join(temp_dir, "file.txt")
        with open(test_file, "w") as f:
            f.write("test")

        p = SmartPath(test_file)
        with pytest.raises(NotADirectoryError):
            await p.rmdir()

    async def test_stat(self, temp_dir):
        test_file = os.path.join(temp_dir, "stat.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        p = SmartPath(test_file)
        stat_result = await p.stat()
        assert isinstance(stat_result, StatResult)
        assert stat_result.size > 0

    async def test_lstat(self, temp_dir):
        test_file = os.path.join(temp_dir, "lstat.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        p = SmartPath(test_file)
        stat_result = await p.lstat()
        assert isinstance(stat_result, StatResult)

    async def test_iterdir(self, temp_dir):
        # Create some test files
        for i in range(3):
            with open(os.path.join(temp_dir, f"file{i}.txt"), "w") as f:
                f.write(f"content {i}")

        p = SmartPath(temp_dir)
        items = []
        async for item in p.iterdir():
            items.append(item)
        assert len(items) == 3

    async def test_walk(self, temp_dir):
        # Create a directory structure
        subdir = os.path.join(temp_dir, "subdir")
        os.mkdir(subdir)
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("file1")
        with open(os.path.join(subdir, "file2.txt"), "w") as f:
            f.write("file2")

        p = SmartPath(temp_dir)
        results = []
        async for root, dirs, files in p.walk():
            results.append((root, dirs, files))
        assert len(results) >= 1

    async def test_glob(self, temp_dir):
        # Create some test files
        for i in range(3):
            with open(os.path.join(temp_dir, f"file{i}.txt"), "w") as f:
                f.write(f"content {i}")
        with open(os.path.join(temp_dir, "other.md"), "w") as f:
            f.write("markdown")

        p = SmartPath(temp_dir)
        results = await p.glob("*.txt")
        assert len(results) == 3

    async def test_rglob(self, temp_dir):
        # Create a directory structure with files
        subdir = os.path.join(temp_dir, "subdir")
        os.mkdir(subdir)
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("file1")
        with open(os.path.join(subdir, "file2.txt"), "w") as f:
            f.write("file2")

        p = SmartPath(temp_dir)
        results = await p.rglob("*.txt")
        assert len(results) == 2

    async def test_rename(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        dst_file = os.path.join(temp_dir, "dst.txt")
        with open(src_file, "w") as f:
            f.write("content")

        p = SmartPath(src_file)
        new_p = await p.rename(dst_file)
        assert not os.path.exists(src_file)
        assert os.path.exists(dst_file)
        assert str(new_p) == dst_file

    async def test_replace(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        dst_file = os.path.join(temp_dir, "dst.txt")
        with open(src_file, "w") as f:
            f.write("content")

        p = SmartPath(src_file)
        await p.replace(dst_file)
        assert not os.path.exists(src_file)
        assert os.path.exists(dst_file)

    async def test_absolute(self, temp_dir):
        p = SmartPath(temp_dir)
        abs_p = await p.absolute()
        assert os.path.isabs(str(abs_p))


class TestSmartPathSymlinks:
    """Tests for symlink operations."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for testing."""
        tmpdir = tmp_path / "test_dir"
        tmpdir.mkdir()
        return str(tmpdir)

    async def test_symlink_to(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        link_file = os.path.join(temp_dir, "link2.txt")

        with open(src_file, "w") as f:
            f.write("content")

        p_link = SmartPath(link_file)
        await p_link.symlink_to(src_file)
        assert await p_link.is_symlink()

    async def test_readlink(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        link_file = os.path.join(temp_dir, "link3.txt")

        with open(src_file, "w") as f:
            f.write("content")

        os.symlink(src_file, link_file)

        p_link = SmartPath(link_file)
        target = await p_link.readlink()
        assert str(target) == src_file

    async def test_resolve(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        link_file = os.path.join(temp_dir, "link4.txt")

        with open(src_file, "w") as f:
            f.write("content")

        os.symlink(src_file, link_file)

        p_link = SmartPath(link_file)
        resolved = await p_link.resolve()
        # Should resolve to absolute path
        assert os.path.isabs(str(resolved))


class TestSmartPathTrueDiv:
    """Tests for truediv (/) operator."""

    def test_truediv_with_string(self):
        p = SmartPath("/tmp")
        result = p / "subdir" / "file.txt"
        assert "subdir" in str(result)
        assert "file.txt" in str(result)

    def test_truediv_with_pathlike(self):
        import pathlib

        p = SmartPath("/tmp")
        result = p / pathlib.PurePosixPath("subdir")
        assert "subdir" in str(result)

    def test_truediv_with_invalid_type_raises(self):
        p = SmartPath("/tmp")
        with pytest.raises(TypeError):
            p / 123


class TestSmartPathFromMethods:
    """Tests for from_path and from_uri class methods."""

    def test_from_uri(self):
        p = SmartPath.from_uri("file:///tmp/test.txt")
        assert isinstance(p, SmartPath)
        assert p.filesystem.protocol == "file"


class TestSmartPathErrors:
    """Tests for error handling."""

    def test_protocol_not_found_error(self):
        from aiomegfile.errors import ProtocolNotFoundError

        with pytest.raises(ProtocolNotFoundError):
            SmartPath("unknown://bucket/key")

    def test_split_path_non_string_raises(self):
        from aiomegfile.errors import ProtocolNotFoundError

        with pytest.raises(ProtocolNotFoundError):
            SmartPath._split_path(123)


class TestSmartPathHardlink:
    """Tests for hardlink operations."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        tmpdir = tmp_path / "test_dir"
        tmpdir.mkdir()
        return str(tmpdir)

    async def test_hardlink_to(self, temp_dir):
        src_file = os.path.join(temp_dir, "src.txt")
        link_file = os.path.join(temp_dir, "hardlink.txt")

        with open(src_file, "w") as f:
            f.write("content")

        p_src = SmartPath(src_file)
        await p_src.hardlink_to(link_file)

        assert os.path.exists(link_file)
        # Check they point to same inode
        assert os.stat(src_file).st_ino == os.stat(link_file).st_ino
