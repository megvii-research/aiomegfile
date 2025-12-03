"""Tests for FSProtocol."""

import os
import tempfile

import pytest

from aiomegfile.protocol.fs import FSProtocol


class TestFSProtocol:
    """Test cases for FSProtocol."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def temp_file(self, temp_dir):
        """Create a temporary file for testing."""
        file_path = os.path.join(temp_dir, "test_file.txt")
        with open(file_path, "w") as f:
            f.write("Hello, World!")
        return file_path

    def _create_protocol(self, path: str) -> FSProtocol:
        """Create a FSProtocol instance for testing."""
        return FSProtocol(path_without_protocol=path)

    @pytest.mark.asyncio
    async def test_exists_file(self, temp_file):
        """Test exists method for existing file."""
        protocol = self._create_protocol(temp_file)
        assert await protocol.exists() is True

    @pytest.mark.asyncio
    async def test_exists_dir(self, temp_dir):
        """Test exists method for existing directory."""
        protocol = self._create_protocol(temp_dir)
        assert await protocol.exists() is True

    @pytest.mark.asyncio
    async def test_exists_not_found(self, temp_dir):
        """Test exists method for non-existing path."""
        protocol = self._create_protocol(os.path.join(temp_dir, "not_exist"))
        assert await protocol.exists() is False

    @pytest.mark.asyncio
    async def test_is_file(self, temp_file):
        """Test is_file method."""
        protocol = self._create_protocol(temp_file)
        assert await protocol.is_file() is True

    @pytest.mark.asyncio
    async def test_is_file_on_dir(self, temp_dir):
        """Test is_file method on directory."""
        protocol = self._create_protocol(temp_dir)
        assert await protocol.is_file() is False

    @pytest.mark.asyncio
    async def test_is_dir(self, temp_dir):
        """Test is_dir method."""
        protocol = self._create_protocol(temp_dir)
        assert await protocol.is_dir() is True

    @pytest.mark.asyncio
    async def test_is_dir_on_file(self, temp_file):
        """Test is_dir method on file."""
        protocol = self._create_protocol(temp_file)
        assert await protocol.is_dir() is False

    @pytest.mark.asyncio
    async def test_stat_file(self, temp_file):
        """Test stat method on file."""
        protocol = self._create_protocol(temp_file)
        stat_result = await protocol.stat()
        assert stat_result.size == 13  # len("Hello, World!")
        assert stat_result.isdir is False
        assert stat_result.islnk is False

    @pytest.mark.asyncio
    async def test_stat_dir(self, temp_dir):
        """Test stat method on directory."""
        protocol = self._create_protocol(temp_dir)
        stat_result = await protocol.stat()
        assert stat_result.isdir is True
        assert stat_result.islnk is False

    @pytest.mark.asyncio
    async def test_open_read(self, temp_file):
        """Test open method for reading."""
        protocol = self._create_protocol(temp_file)
        cm = await protocol.open("r")
        async with cm as f:
            content = await f.read()
        assert content == "Hello, World!"

    @pytest.mark.asyncio
    async def test_open_write(self, temp_dir):
        """Test open method for writing."""
        file_path = os.path.join(temp_dir, "new_file.txt")
        protocol = self._create_protocol(file_path)
        cm = await protocol.open("w")
        async with cm as f:
            await f.write("New content")

        with open(file_path) as f:
            assert f.read() == "New content"

    @pytest.mark.asyncio
    async def test_mkdir(self, temp_dir):
        """Test mkdir method."""
        new_dir = os.path.join(temp_dir, "new_dir")
        protocol = self._create_protocol(new_dir)
        await protocol.mkdir()
        assert os.path.isdir(new_dir)

    @pytest.mark.asyncio
    async def test_mkdir_parents(self, temp_dir):
        """Test mkdir method with parents=True."""
        new_dir = os.path.join(temp_dir, "parent", "child")
        protocol = self._create_protocol(new_dir)
        await protocol.mkdir(parents=True)
        assert os.path.isdir(new_dir)

    @pytest.mark.asyncio
    async def test_mkdir_exist_ok(self, temp_dir):
        """Test mkdir method with exist_ok=True."""
        protocol = self._create_protocol(temp_dir)
        # Should not raise
        await protocol.mkdir(exist_ok=True)

    @pytest.mark.asyncio
    async def test_remove(self, temp_file):
        """Test remove method."""
        protocol = self._create_protocol(temp_file)
        await protocol.remove()
        assert not os.path.exists(temp_file)

    @pytest.mark.asyncio
    async def test_remove_missing_ok(self, temp_dir):
        """Test remove method with missing_ok=True."""
        file_path = os.path.join(temp_dir, "not_exist")
        protocol = self._create_protocol(file_path)
        # Should not raise
        await protocol.remove(missing_ok=True)

    @pytest.mark.asyncio
    async def test_remove_missing_raises(self, temp_dir):
        """Test remove method raises FileNotFoundError."""
        file_path = os.path.join(temp_dir, "not_exist")
        protocol = self._create_protocol(file_path)
        with pytest.raises(FileNotFoundError):
            await protocol.remove(missing_ok=False)

    @pytest.mark.asyncio
    async def test_rename(self, temp_file, temp_dir):
        """Test rename method."""
        dst_path = os.path.join(temp_dir, "renamed_file.txt")
        protocol = self._create_protocol(temp_file)
        result = await protocol.rename(dst_path)
        assert result == dst_path
        assert os.path.exists(dst_path)
        assert not os.path.exists(temp_file)

    @pytest.mark.asyncio
    async def test_rename_no_overwrite(self, temp_file, temp_dir):
        """Test rename method with overwrite=False."""
        dst_path = os.path.join(temp_dir, "existing_file.txt")
        with open(dst_path, "w") as f:
            f.write("existing")

        protocol = self._create_protocol(temp_file)
        with pytest.raises(FileExistsError):
            await protocol.rename(dst_path, overwrite=False)

    @pytest.mark.asyncio
    async def test_symlink_and_readlink(self, temp_file, temp_dir):
        """Test symlink and readlink methods."""
        link_path = os.path.join(temp_dir, "link_to_file")
        protocol = self._create_protocol(temp_file)
        await protocol.symlink(link_path)

        assert os.path.islink(link_path)

        link_protocol = self._create_protocol(link_path)
        target = await link_protocol.readlink()
        assert target == temp_file

    @pytest.mark.asyncio
    async def test_iterdir(self, temp_dir):
        """Test iterdir method."""
        # Create some files
        for name in ["c.txt", "a.txt", "b.txt"]:
            with open(os.path.join(temp_dir, name), "w") as f:
                f.write(name)

        protocol = self._create_protocol(temp_dir)
        entries = []
        async for entry in protocol.iterdir():
            entries.append(entry)

        # Should be sorted
        expected = [
            os.path.join(temp_dir, "a.txt"),
            os.path.join(temp_dir, "b.txt"),
            os.path.join(temp_dir, "c.txt"),
        ]
        assert entries == expected

    @pytest.mark.asyncio
    async def test_walk(self, temp_dir):
        """Test walk method."""
        # Create directory structure
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("file1")
        with open(os.path.join(subdir, "file2.txt"), "w") as f:
            f.write("file2")

        protocol = self._create_protocol(temp_dir)
        results = []
        async for root, dirs, files in protocol.walk():
            results.append((root, dirs, files))

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_absolute(self, temp_file):
        """Test absolute method."""
        protocol = self._create_protocol(temp_file)
        abs_path = await protocol.absolute()
        assert os.path.isabs(abs_path)
        assert abs_path == os.path.abspath(temp_file)

    @pytest.mark.asyncio
    async def test_chmod(self, temp_file):
        """Test chmod method."""
        protocol = self._create_protocol(temp_file)
        await protocol.chmod(0o644)
        stat_result = os.stat(temp_file)
        # Check file permission bits (ignore file type bits)
        assert stat_result.st_mode & 0o777 == 0o644
