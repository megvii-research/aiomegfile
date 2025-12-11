"""Tests for LocalFileSystem."""

import os

import pytest

from aiomegfile.filesystem.local import LocalFileSystem


class TestLocalFileSystem:
    """Test cases for LocalFileSystem."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for testing."""
        tmpdir = tmp_path / "test_dir"
        tmpdir.mkdir()
        return str(tmpdir)

    @pytest.fixture
    def temp_file(self, temp_dir):
        """Create a temporary file for testing."""
        file_path = os.path.join(temp_dir, "test_file.txt")
        with open(file_path, "w") as f:
            f.write("Hello, World!")
        return file_path

    def _create_protocol(self) -> LocalFileSystem:
        """Create a LocalFileSystem instance for testing."""
        return LocalFileSystem(protocol_in_path=False)

    async def test_exists_file(self, temp_file):
        """Test exists method for existing file."""
        protocol = self._create_protocol()
        assert await protocol.exists(temp_file) is True

    async def test_exists_dir(self, temp_dir):
        """Test exists method for existing directory."""
        protocol = self._create_protocol()
        assert await protocol.exists(temp_dir) is True

    async def test_exists_not_found(self, temp_dir):
        """Test exists method for non-existing path."""
        protocol = self._create_protocol()
        assert await protocol.exists(os.path.join(temp_dir, "not_exist")) is False

    async def test_is_file(self, temp_file):
        """Test is_file method."""
        protocol = self._create_protocol()
        assert await protocol.is_file(temp_file) is True

    async def test_is_file_on_dir(self, temp_dir):
        """Test is_file method on directory."""
        protocol = self._create_protocol()
        assert await protocol.is_file(temp_dir) is False

    async def test_is_dir(self, temp_dir):
        """Test is_dir method."""
        protocol = self._create_protocol()
        assert await protocol.is_dir(temp_dir) is True

    async def test_is_dir_on_file(self, temp_file):
        """Test is_dir method on file."""
        protocol = self._create_protocol()
        assert await protocol.is_dir(temp_file) is False

    async def test_stat_file(self, temp_file):
        """Test stat method on file."""
        protocol = self._create_protocol()
        stat_result = await protocol.stat(temp_file)
        assert stat_result.st_size == 13  # len("Hello, World!")
        assert stat_result.isdir is False
        assert stat_result.islnk is False

    async def test_stat_dir(self, temp_dir):
        """Test stat method on directory."""
        protocol = self._create_protocol()
        stat_result = await protocol.stat(temp_dir)
        assert stat_result.isdir is True
        assert stat_result.islnk is False

    async def test_open_read(self, temp_file):
        """Test open method for reading."""
        protocol = self._create_protocol()
        async with protocol.open(temp_file, "r") as f:
            content = await f.read()
        assert content == "Hello, World!"

    async def test_open_write(self, temp_dir):
        """Test open method for writing."""
        file_path = os.path.join(temp_dir, "new_file.txt")
        protocol = self._create_protocol()
        async with protocol.open(file_path, "w") as f:
            await f.write("New content")

        with open(file_path) as f:
            assert f.read() == "New content"

    async def test_mkdir(self, temp_dir):
        """Test mkdir method."""
        new_dir = os.path.join(temp_dir, "new_dir")
        protocol = self._create_protocol()
        await protocol.mkdir(new_dir)
        assert os.path.isdir(new_dir)

    async def test_mkdir_parents(self, temp_dir):
        """Test mkdir method with parents=True."""
        new_dir = os.path.join(temp_dir, "parent", "child")
        protocol = self._create_protocol()
        await protocol.mkdir(new_dir, parents=True)
        assert os.path.isdir(new_dir)

    async def test_mkdir_exist_ok(self, temp_dir):
        """Test mkdir method with exist_ok=True."""
        protocol = self._create_protocol()
        # Should not raise
        await protocol.mkdir(temp_dir, exist_ok=True)

    async def test_unlink(self, temp_file):
        """Test unlink method."""
        protocol = self._create_protocol()
        await protocol.unlink(temp_file)
        assert not os.path.exists(temp_file)

    async def test_unlink_missing_ok(self, temp_dir):
        """Test unlink method with missing_ok=True."""
        file_path = os.path.join(temp_dir, "not_exist")
        protocol = self._create_protocol()
        # Should not raise
        await protocol.unlink(file_path, missing_ok=True)

    async def test_unlink_missing_raises(self, temp_dir):
        """Test unlink method raises FileNotFoundError."""
        file_path = os.path.join(temp_dir, "not_exist")
        protocol = self._create_protocol()
        with pytest.raises(FileNotFoundError):
            await protocol.unlink(file_path, missing_ok=False)

    async def test_move(self, temp_file, temp_dir):
        """Test move method."""
        dst_path = os.path.join(temp_dir, "moved_file.txt")
        protocol = self._create_protocol()
        result = await protocol.move(temp_file, dst_path)
        assert result == dst_path
        assert os.path.exists(dst_path)
        assert not os.path.exists(temp_file)

    async def test_move_no_overwrite(self, temp_file, temp_dir):
        """Test move method with overwrite=False."""
        dst_path = os.path.join(temp_dir, "existing_file.txt")
        with open(dst_path, "w") as f:
            f.write("existing")

        protocol = self._create_protocol()
        with pytest.raises(FileExistsError):
            await protocol.move(temp_file, dst_path, overwrite=False)

    async def test_symlink_and_readlink(self, temp_file, temp_dir):
        """Test symlink and readlink methods."""
        link_path = os.path.join(temp_dir, "link_to_file")
        protocol = self._create_protocol()
        await protocol.symlink(temp_file, link_path)

        assert os.path.islink(link_path)

        link_protocol = self._create_protocol()
        target = await link_protocol.readlink(link_path)
        assert target == temp_file

    async def test_copy_file(self, temp_file, temp_dir):
        """Test copy method for single file."""
        dst_path = os.path.join(temp_dir, "copied_file.txt")
        protocol = self._create_protocol()

        result = await protocol.copy(temp_file, dst_path)

        assert result == dst_path
        assert os.path.exists(temp_file)
        with open(dst_path) as f:
            assert f.read() == "Hello, World!"

    async def test_copy_directory_raises(self, temp_dir):
        """Test copy method raises on directory input."""
        src_dir = os.path.join(temp_dir, "dir_src")
        os.makedirs(src_dir)
        dst_path = os.path.join(temp_dir, "dir_dst")

        protocol = self._create_protocol()
        with pytest.raises(IsADirectoryError):
            await protocol.copy(src_dir, dst_path)

    async def test_iterdir(self, temp_dir):
        """Test iterdir method."""
        # Create some files
        for name in ["c.txt", "a.txt", "b.txt"]:
            with open(os.path.join(temp_dir, name), "w") as f:
                f.write(name)

        protocol = self._create_protocol()
        entries = []
        async for entry in protocol.iterdir(temp_dir):
            entries.append(entry)

        # Should be sorted
        expected = [
            os.path.join(temp_dir, "a.txt"),
            os.path.join(temp_dir, "b.txt"),
            os.path.join(temp_dir, "c.txt"),
        ]
        assert entries == expected

    async def test_walk(self, temp_dir):
        """Test walk method."""
        # Create directory structure
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("file1")
        with open(os.path.join(subdir, "file2.txt"), "w") as f:
            f.write("file2")

        protocol = self._create_protocol()
        results = []
        async for root, dirs, files in protocol.walk(temp_dir):
            results.append((root, dirs, files))

        assert len(results) == 2

    async def test_absolute(self, temp_file):
        """Test absolute method."""
        protocol = self._create_protocol()
        abs_path = await protocol.absolute(temp_file)
        assert os.path.isabs(abs_path)
        assert abs_path == os.path.abspath(temp_file)
