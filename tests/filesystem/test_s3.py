import asyncio

import pytest
from moto.server import ThreadedMotoServer

from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3NameTooLongError,
    S3NotALinkError,
    SameFileError,
)
from aiomegfile.filesystem.s3 import S3FileSystem, get_s3_client, is_s3, parse_s3_path

_aws_access_key_id = "testing"
_aws_secret_access_key = "testing"
_bucket_name = "test-bucket"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer()
    try:
        server.start()
        host, port = server.get_host_and_port()
        if host == "0.0.0.0":
            host = "localhost"
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def mock_s3(moto_server, monkeypatch):
    """Mock AWS credentials and endpoint URL to environment variables."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _aws_access_key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _aws_secret_access_key)
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)


class TestS3FileSystem:
    @pytest.fixture
    def filesystem(self, mock_s3):  # noqa: ARG002
        """Create S3FileSystem that reads credentials from environment."""
        return S3FileSystem()

    async def _create_bucket(self, filesystem: S3FileSystem):
        """Helper to create test bucket."""
        client = await filesystem._get_client()
        try:
            await client.create_bucket(Bucket=_bucket_name)
        except Exception:
            pass  # Bucket may already exist

    async def _put_object(self, filesystem: S3FileSystem, key: str, body: bytes = b"0"):
        """Helper to put object in test bucket."""
        client = await filesystem._get_client()
        await client.put_object(Bucket=_bucket_name, Key=key, Body=body)

    async def _head_object(self, filesystem: S3FileSystem, key: str):
        """Helper to head object in test bucket."""
        client = await filesystem._get_client()
        return await client.head_object(Bucket=_bucket_name, Key=key)

    async def test_is_file(self, filesystem):
        await self._create_bucket(filesystem)

        filename = "0.txt"
        await self._put_object(filesystem, filename)
        assert await filesystem.is_file(f"{_bucket_name}/{filename}") is True
        assert await filesystem.is_file(f"{_bucket_name}") is False
        assert await filesystem.is_file(f"{_bucket_name}/") is False
        assert await filesystem.is_file(f"{_bucket_name}/null.txt") is False

    async def test_is_dir(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "subdir"
        filename = "0.txt"
        await self._put_object(filesystem, f"{subdir}/{filename}")
        assert await filesystem.is_dir(f"{_bucket_name}") is True
        assert await filesystem.is_dir(f"{_bucket_name}/") is True
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}") is True
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}/") is True
        assert await filesystem.is_dir(f"{_bucket_name}/null") is False
        assert await filesystem.is_dir(f"{_bucket_name}/null/") is False
        assert await filesystem.is_dir(f"{_bucket_name}/{subdir}/{filename}") is False

    async def test_exists(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "subdir"
        filename = "0.txt"
        await self._put_object(filesystem, f"{subdir}/{filename}")
        assert await filesystem.exists(f"{_bucket_name}") is True
        assert await filesystem.exists(f"{_bucket_name}/") is True
        assert await filesystem.exists(f"{_bucket_name}/{subdir}") is True
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/") is True
        assert await filesystem.exists(f"{_bucket_name}/null") is False
        assert await filesystem.exists(f"{_bucket_name}/null/") is False
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/{filename}") is True

    async def test_remove_file(self, filesystem):
        await self._create_bucket(filesystem)

        filename = "remove_file.txt"
        await self._put_object(filesystem, filename)

        assert await filesystem.exists(f"{_bucket_name}/{filename}") is True
        await filesystem.remove(f"{_bucket_name}/{filename}")
        assert await filesystem.exists(f"{_bucket_name}/{filename}") is False

    async def test_remove_directory(self, filesystem):
        await self._create_bucket(filesystem)

        subdir = "remove_subdir"
        filename1 = "1.txt"
        filename2 = "2.txt"
        await self._put_object(filesystem, f"{subdir}/{filename1}")
        await self._put_object(filesystem, f"{subdir}/{filename2}")

        await filesystem.remove(f"{_bucket_name}/{subdir}")
        assert await filesystem.exists(f"{_bucket_name}/{subdir}/{filename1}") is False
        assert await filesystem.exists(f"{_bucket_name}/{subdir}") is False
        assert await filesystem.exists(f"{_bucket_name}") is True

    async def test_remove_missing_ok(self, filesystem):
        """Test remove with missing_ok flag."""
        await self._create_bucket(filesystem)

        # Should not raise when missing_ok=True
        await filesystem.remove(f"{_bucket_name}/nonexistent_file", missing_ok=True)

        # Should raise when missing_ok=False
        with pytest.raises(S3FileNotFoundError):
            await filesystem.remove(
                f"{_bucket_name}/nonexistent_file", missing_ok=False
            )

    async def test_remove_errors(self, filesystem):
        """Test remove error handling."""
        await self._create_bucket(filesystem)

        # Empty bucket name
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.remove("/key")

        # Empty key (bucket root)
        with pytest.raises(S3IsADirectoryError):
            await filesystem.remove(f"{_bucket_name}")

    async def test_stat_file(self, filesystem):
        """Test stat on a file."""
        await self._create_bucket(filesystem)

        content = b"hello world"
        await self._put_object(filesystem, "stat_file.txt", content)

        stat_result = await filesystem.stat(f"{_bucket_name}/stat_file.txt")
        assert stat_result.st_size == len(content)
        assert stat_result.st_mtime > 0
        assert stat_result.isdir is False
        assert stat_result.islnk is False

    async def test_stat_directory(self, filesystem):
        """Test stat on a directory."""
        await self._create_bucket(filesystem)

        # Create files in a directory
        await self._put_object(filesystem, "stat_dir/file1.txt", b"content1")
        await self._put_object(filesystem, "stat_dir/file2.txt", b"content2")

        stat_result = await filesystem.stat(f"{_bucket_name}/stat_dir")
        assert stat_result.st_size == len(b"content1") + len(b"content2")
        assert stat_result.st_mtime > 0
        assert stat_result.isdir is True

    async def test_stat_not_found(self, filesystem):
        """Test stat on non-existent path."""
        await self._create_bucket(filesystem)

        with pytest.raises(S3FileNotFoundError):
            await filesystem.stat(f"{_bucket_name}/nonexistent")

    async def test_stat_empty_bucket(self, filesystem):
        """Test stat with empty bucket raises error."""
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.stat("/path")

    async def test_scandir(self, filesystem):
        """Test scandir returns correct entries."""
        await self._create_bucket(filesystem)

        # Create test files and subdirectory
        await self._put_object(filesystem, "scandir_test/file1.txt", b"content1")
        await self._put_object(filesystem, "scandir_test/file2.txt", b"content2")
        await self._put_object(filesystem, "scandir_test/subdir/file3.txt", b"content3")

        entries = []
        async with filesystem.scandir(f"{_bucket_name}/scandir_test") as scanner:
            async for entry in scanner:
                entries.append(entry)

        # Should have 2 files and 1 directory
        assert len(entries) == 3
        names = [e.name for e in entries]
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "subdir" in names

        # Verify file entries
        for entry in entries:
            if entry.name in ("file1.txt", "file2.txt"):
                assert entry.is_file() is True
                assert entry.is_dir() is False
            elif entry.name == "subdir":
                assert entry.is_dir() is True

    async def test_scandir_empty_bucket(self, filesystem):
        """Test scandir on empty bucket returns no entries."""
        await self._create_bucket(filesystem)

        entries = []
        async with filesystem.scandir(f"{_bucket_name}/empty_dir") as scanner:
            async for entry in scanner:
                entries.append(entry)

        assert len(entries) == 0

    async def test_scanfile(self, filesystem):
        """Test scanfile returns only files recursively."""
        await self._create_bucket(filesystem)

        # Create test files in nested structure
        await self._put_object(filesystem, "scanfile_test/file1.txt", b"content1")
        await self._put_object(
            filesystem, "scanfile_test/subdir/file2.txt", b"content2"
        )
        await self._put_object(
            filesystem, "scanfile_test/subdir/nested/file3.txt", b"content3"
        )

        entries = []
        async with filesystem.scanfile(f"{_bucket_name}/scanfile_test") as scanner:
            async for entry in scanner:
                entries.append(entry)

        # Should have 3 files (all files recursively)
        assert len(entries) == 3
        names = [e.name for e in entries]
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "file3.txt" in names

        # All should be files
        for entry in entries:
            assert entry.stat.isdir is False

    async def test_mkdir(self, filesystem):
        """Test mkdir creates directory marker."""
        await self._create_bucket(filesystem)

        # Should succeed with exist_ok=True
        await filesystem.mkdir(f"{_bucket_name}/new_dir", exist_ok=True)

        # Create a file in the directory to make it exist
        await self._put_object(filesystem, "existing_dir/file.txt", b"content")

        # Should raise when directory exists and exist_ok=False
        with pytest.raises(S3FileExistsError):
            await filesystem.mkdir(f"{_bucket_name}/existing_dir", exist_ok=False)

        # Should succeed when exist_ok=True
        await filesystem.mkdir(f"{_bucket_name}/existing_dir", exist_ok=True)

    async def test_mkdir_errors(self, filesystem):
        """Test mkdir error cases."""
        await self._create_bucket(filesystem)

        # Empty bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.mkdir("/key")

        # Empty key
        with pytest.raises(S3FileNotFoundError):
            await filesystem.mkdir(f"{_bucket_name}")

    async def test_copy(self, filesystem):
        """Test copy single file."""
        await self._create_bucket(filesystem)

        content = b"copy content"
        await self._put_object(filesystem, "copy_src.txt", content)

        result = await filesystem.copy(
            f"{_bucket_name}/copy_src.txt",
            f"{_bucket_name}/copy_dst.txt",
        )

        assert result == f"{_bucket_name}/copy_dst.txt"
        assert await filesystem.exists(f"{_bucket_name}/copy_dst.txt") is True

        # Verify content
        head = await self._head_object(filesystem, "copy_dst.txt")
        assert head["ContentLength"] == len(content)

    async def test_copy_same_file_error(self, filesystem):
        """Test copy raises error when source and destination are same."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "same_file.txt", b"content")

        with pytest.raises(SameFileError):
            await filesystem.copy(
                f"{_bucket_name}/same_file.txt",
                f"{_bucket_name}/same_file.txt",
            )

    async def test_copy_errors(self, filesystem):
        """Test copy error cases."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "src.txt", b"content")

        # Empty source bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.copy("/src", f"{_bucket_name}/dst")

        # Empty destination bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.copy(f"{_bucket_name}/src.txt", "/dst")

        # Source is directory
        with pytest.raises(S3IsADirectoryError):
            await filesystem.copy(f"{_bucket_name}/", f"{_bucket_name}/dst")

        # Destination is directory
        with pytest.raises(S3IsADirectoryError):
            await filesystem.copy(f"{_bucket_name}/src.txt", f"{_bucket_name}/")

    async def test_move(self, filesystem):
        """Test move file."""
        await self._create_bucket(filesystem)

        content = b"move content"
        await self._put_object(filesystem, "move_src.txt", content)

        result = await filesystem.move(
            f"{_bucket_name}/move_src.txt",
            f"{_bucket_name}/move_dst.txt",
        )

        assert result == f"{_bucket_name}/move_dst.txt"
        assert await filesystem.exists(f"{_bucket_name}/move_dst.txt") is True
        assert await filesystem.exists(f"{_bucket_name}/move_src.txt") is False

    async def test_move_directory(self, filesystem):
        """Test move directory."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "move_dir/file1.txt", b"content1")
        await self._put_object(filesystem, "move_dir/file2.txt", b"content2")

        await filesystem.move(
            f"{_bucket_name}/move_dir",
            f"{_bucket_name}/moved_dir",
        )

        assert await filesystem.exists(f"{_bucket_name}/moved_dir/file1.txt") is True
        assert await filesystem.exists(f"{_bucket_name}/moved_dir/file2.txt") is True
        assert await filesystem.exists(f"{_bucket_name}/move_dir") is False

    async def test_move_overwrite(self, filesystem):
        """Test move with overwrite flag."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "move_overwrite_src.txt", b"source")
        await self._put_object(filesystem, "move_overwrite_dst.txt", b"destination")

        # Should succeed with overwrite=True (default)
        await filesystem.move(
            f"{_bucket_name}/move_overwrite_src.txt",
            f"{_bucket_name}/move_overwrite_dst.txt",
            overwrite=True,
        )

        # Create new source for next test
        await self._put_object(filesystem, "move_overwrite_src2.txt", b"source2")

        # Should fail with overwrite=False when destination exists
        with pytest.raises(S3FileExistsError):
            await filesystem.move(
                f"{_bucket_name}/move_overwrite_src2.txt",
                f"{_bucket_name}/move_overwrite_dst.txt",
                overwrite=False,
            )

    @pytest.mark.skip(
        reason="moto ThreadedMotoServer does not support returning user metadata"
    )
    async def test_symlink_and_readlink(self, filesystem):
        """Test symlink creation and reading."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "symlink_src.txt", b"content")

        # Create symlink
        await filesystem.symlink(
            f"{_bucket_name}/symlink_src.txt",
            f"{_bucket_name}/symlink_dst.txt",
        )

        # Verify symlink exists
        assert await filesystem.exists(f"{_bucket_name}/symlink_dst.txt") is True

        # Read symlink
        target = await filesystem.readlink(f"{_bucket_name}/symlink_dst.txt")
        assert target == f"{_bucket_name}/symlink_src.txt"

    @pytest.mark.skip(
        reason="moto ThreadedMotoServer does not support returning user metadata"
    )
    async def test_is_symlink(self, filesystem):
        """Test is_symlink method."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "regular_file.txt", b"content")
        await filesystem.symlink(
            f"{_bucket_name}/regular_file.txt",
            f"{_bucket_name}/link_file.txt",
        )

        assert await filesystem.is_symlink(f"{_bucket_name}/link_file.txt") is True
        assert await filesystem.is_symlink(f"{_bucket_name}/regular_file.txt") is False
        assert await filesystem.is_symlink(f"{_bucket_name}/nonexistent.txt") is False

    async def test_readlink_not_a_link(self, filesystem):
        """Test readlink raises error for non-symlink."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "regular.txt", b"content")

        with pytest.raises(S3NotALinkError):
            await filesystem.readlink(f"{_bucket_name}/regular.txt")

    async def test_symlink_errors(self, filesystem):
        """Test symlink error cases."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "src.txt", b"content")

        # Empty source bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.symlink("/src", f"{_bucket_name}/dst")

        # Empty destination bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.symlink(f"{_bucket_name}/src.txt", "/dst")

        # Destination is directory
        with pytest.raises(S3IsADirectoryError):
            await filesystem.symlink(f"{_bucket_name}/src.txt", f"{_bucket_name}/dst/")

        # Name too long
        long_path = f"{_bucket_name}/" + "a" * 1024
        with pytest.raises(S3NameTooLongError):
            await filesystem.symlink(f"{_bucket_name}/src.txt", long_path)

    async def test_readlink_errors(self, filesystem):
        """Test readlink error cases."""
        await self._create_bucket(filesystem)

        # Empty bucket
        with pytest.raises(S3BucketNotFoundError):
            await filesystem.readlink("/path")

        # Directory path
        with pytest.raises(S3IsADirectoryError):
            await filesystem.readlink(f"{_bucket_name}/path/")

    async def test_build_uri(self, filesystem):
        """Test build_uri method."""
        assert filesystem.build_uri("bucket/key") == "s3://bucket/key"

    async def test_build_uri_with_profile(self, mock_s3):  # noqa: ARG002
        """Test build_uri with profile name."""
        fs = S3FileSystem(profile_name="myprofile")
        assert fs.build_uri("bucket/key") == "s3+myprofile://bucket/key"

    async def test_parse_uri(self, filesystem):
        """Test parse_uri method."""
        assert filesystem.parse_uri("s3://bucket/key") == "bucket/key"
        assert filesystem.parse_uri("s3+profile://bucket/key") == "bucket/key"

    async def test_from_uri(self, mock_s3):  # noqa: ARG002
        """Test from_uri class method."""
        fs = S3FileSystem.from_uri("s3://bucket/key")
        assert fs._profile_name is None

        fs_profile = S3FileSystem.from_uri("s3+myprofile://bucket/key")
        assert fs_profile._profile_name == "myprofile"

    async def test_same_endpoint(self, mock_s3):  # noqa: ARG002
        """Test same_endpoint method."""
        fs1 = S3FileSystem()
        fs2 = S3FileSystem()
        fs3 = S3FileSystem(profile_name="different")

        assert fs1.same_endpoint(fs2) is True
        assert fs1.same_endpoint(fs3) is False

    async def test_open_read_binary(self, filesystem):
        """Test open in binary read mode."""
        await self._create_bucket(filesystem)

        content = b"hello world"
        key = "test_read.txt"
        await self._put_object(filesystem, key, content)

        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            result = await f.read()
            assert result == content

    async def test_open_read_text(self, filesystem):
        """Test open in text read mode."""
        await self._create_bucket(filesystem)

        content = "hello world"
        key = "test_read_text.txt"
        await self._put_object(filesystem, key, content.encode())

        async with filesystem.open(f"{_bucket_name}/{key}", mode="r") as f:
            result = await f.read()
            assert result == content

    async def test_open_read_with_encoding(self, filesystem):
        """Test open in text mode with specific encoding."""
        await self._create_bucket(filesystem)

        content = "你好世界"
        key = "test_read_encoding.txt"
        await self._put_object(filesystem, key, content.encode("utf-8"))

        async with filesystem.open(
            f"{_bucket_name}/{key}", mode="r", encoding="utf-8"
        ) as f:
            result = await f.read()
            assert result == content

    async def test_open_write_binary(self, filesystem):
        """Test open in binary write mode."""
        await self._create_bucket(filesystem)

        content = b"write test content"
        key = "test_write.txt"

        async with filesystem.open(f"{_bucket_name}/{key}", mode="wb") as f:
            await f.write(content)

        # Verify content was written
        head = await self._head_object(filesystem, key)
        assert head["ContentLength"] == len(content)

    async def test_open_write_text(self, filesystem):
        """Test open in text write mode."""
        await self._create_bucket(filesystem)

        content = "write test content"
        key = "test_write_text.txt"

        async with filesystem.open(f"{_bucket_name}/{key}", mode="w") as f:
            await f.write(content)

        # Verify content was written
        head = await self._head_object(filesystem, key)
        assert head["ContentLength"] == len(content.encode())

    async def test_open_write_with_encoding(self, filesystem):
        """Test open in text write mode with specific encoding."""
        await self._create_bucket(filesystem)

        content = "你好世界"
        key = "test_write_encoding.txt"

        async with filesystem.open(
            f"{_bucket_name}/{key}", mode="w", encoding="utf-8"
        ) as f:
            await f.write(content)

        # Verify content was written
        head = await self._head_object(filesystem, key)
        assert head["ContentLength"] == len(content.encode("utf-8"))

    @pytest.mark.skip(
        reason="S3FileSystem._download_fileobj and _upload_fileobj not implemented"
    )
    async def test_open_append_binary(self, filesystem):
        """Test open in binary append mode."""
        await self._create_bucket(filesystem)

        initial_content = b"initial content"
        append_content = b" appended"
        key = "test_append.txt"
        await self._put_object(filesystem, key, initial_content)

        async with filesystem.open(f"{_bucket_name}/{key}", mode="ab") as f:
            await f.write(append_content)

        # Read back to verify
        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            result = await f.read()
            assert result == initial_content + append_content

    @pytest.mark.skip(
        reason="S3FileSystem._download_fileobj and _upload_fileobj not implemented"
    )
    async def test_open_append_text(self, filesystem):
        """Test open in text append mode."""
        await self._create_bucket(filesystem)

        initial_content = "initial content"
        append_content = " appended"
        key = "test_append_text.txt"
        await self._put_object(filesystem, key, initial_content.encode())

        async with filesystem.open(f"{_bucket_name}/{key}", mode="a") as f:
            await f.write(append_content)

        # Read back to verify
        async with filesystem.open(f"{_bucket_name}/{key}", mode="r") as f:
            result = await f.read()
            assert result == initial_content + append_content

    @pytest.mark.skip(
        reason="S3FileSystem._download_fileobj and _upload_fileobj not implemented"
    )
    async def test_open_append_plus_binary(self, filesystem):
        """Test open in binary append+ mode (read and write)."""
        await self._create_bucket(filesystem)

        initial_content = b"initial content"
        key = "test_append_plus.txt"
        await self._put_object(filesystem, key, initial_content)

        async with filesystem.open(f"{_bucket_name}/{key}", mode="ab+") as f:
            # Read existing content
            await f.seek(0)
            result = await f.read()
            assert result == initial_content

            # Append new content
            await f.write(b" appended")

        # Read back to verify
        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            result = await f.read()
            assert result == initial_content + b" appended"

    @pytest.mark.skip(
        reason="S3FileSystem._download_fileobj and _upload_fileobj not implemented"
    )
    async def test_open_append_plus_text(self, filesystem):
        """Test open in text append+ mode (read and write)."""
        await self._create_bucket(filesystem)

        initial_content = "initial content"
        key = "test_append_plus_text.txt"
        await self._put_object(filesystem, key, initial_content.encode())

        async with filesystem.open(f"{_bucket_name}/{key}", mode="a+") as f:
            # Read existing content
            await f.seek(0)
            result = await f.read()
            assert result == initial_content

            # Append new content
            await f.write(" appended")

        # Read back to verify
        async with filesystem.open(f"{_bucket_name}/{key}", mode="r") as f:
            result = await f.read()
            assert result == initial_content + " appended"

    async def test_open_invalid_mode_x(self, filesystem):
        """Test open with unsupported 'x' mode raises ValueError."""
        await self._create_bucket(filesystem)

        with pytest.raises(ValueError, match="unacceptable 'x' mode"):
            filesystem.open(f"{_bucket_name}/test.txt", mode="x")

    async def test_open_empty_bucket(self, filesystem):
        """Test open with empty bucket raises error."""
        await self._create_bucket(filesystem)

        with pytest.raises(S3BucketNotFoundError):
            filesystem.open("/key.txt", mode="r")

    async def test_open_directory_path(self, filesystem):
        """Test open with directory path raises error."""
        await self._create_bucket(filesystem)

        with pytest.raises(S3IsADirectoryError):
            filesystem.open(f"{_bucket_name}/", mode="r")

        with pytest.raises(S3IsADirectoryError):
            filesystem.open(f"{_bucket_name}/dir/", mode="w")

    async def test_open_file_not_found_read(self, filesystem):
        """Test open non-existent file in read mode raises error."""
        await self._create_bucket(filesystem)

        # Opening non-existent file in read mode should raise when entering context
        with pytest.raises(S3FileNotFoundError):
            async with filesystem.open(f"{_bucket_name}/nonexistent.txt", mode="rb"):
                pass

    async def test_open_read_multiline(self, filesystem):
        """Test reading multiline content."""
        await self._create_bucket(filesystem)

        content = b"line1\nline2\nline3"
        key = "test_multiline.txt"
        await self._put_object(filesystem, key, content)

        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            line1 = await f.readline()
            assert line1 == b"line1\n"
            line2 = await f.readline()
            assert line2 == b"line2\n"
            line3 = await f.readline()
            assert line3 == b"line3"

    async def test_open_write_multiple_chunks(self, filesystem):
        """Test writing multiple chunks."""
        await self._create_bucket(filesystem)

        key = "test_chunks.txt"

        async with filesystem.open(f"{_bucket_name}/{key}", mode="wb") as f:
            await f.write(b"chunk1 ")
            await f.write(b"chunk2 ")
            await f.write(b"chunk3")

        # Read back to verify
        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            result = await f.read()
            assert result == b"chunk1 chunk2 chunk3"

    async def test_open_name_property(self, filesystem):
        """Test that opened file has correct name property."""
        await self._create_bucket(filesystem)

        key = "test_name_property.txt"
        await self._put_object(filesystem, key, b"test")

        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            assert f.name == f"s3://{_bucket_name}/{key}"

    async def test_open_mode_property(self, filesystem):
        """Test that opened file has correct mode property."""
        await self._create_bucket(filesystem)

        key = "test_mode_property.txt"
        await self._put_object(filesystem, key, b"test")

        async with filesystem.open(f"{_bucket_name}/{key}", mode="rb") as f:
            assert f.mode == "rb"

        async with filesystem.open(f"{_bucket_name}/{key}", mode="r") as f:
            assert f.mode == "r"


class TestHelperFunctions:
    """Test helper functions."""

    def test_is_s3(self):
        """Test is_s3 function."""
        # Valid S3 paths
        assert is_s3("s3://bucket/key") is True
        assert is_s3("s3://bucket") is True
        assert is_s3("s3://") is True
        assert is_s3("s3+profile://bucket") is True

        # Invalid paths
        assert is_s3("") is False
        assert is_s3("s3") is False
        assert is_s3("s3:/") is False
        assert is_s3("s3:/bucket") is False
        assert is_s3("/local/path") is False
        assert is_s3("http://example.com") is False

    def test_parse_s3_path(self):
        """Test parse_s3_path function."""
        # Basic cases
        assert parse_s3_path("bucket/key") == ("bucket", "key")
        assert parse_s3_path("bucket") == ("bucket", "")
        assert parse_s3_path("bucket/prefix/key") == ("bucket", "prefix/key")
        assert parse_s3_path("bucket/") == ("bucket", "")
        assert parse_s3_path("") == ("", "")

    def test_get_s3_client_cache_per_loop(self, mock_s3, moto_server):  # noqa: ARG002
        """Test get_s3_client cache is scoped per event loop."""
        client1 = None
        loop1 = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop1)
            client1 = loop1.run_until_complete(get_s3_client(endpoint_url=moto_server))
            client1_again = loop1.run_until_complete(
                get_s3_client(endpoint_url=moto_server)
            )
            assert client1 is client1_again
            loop1.run_until_complete(client1.__aexit__(None, None, None))
        finally:
            loop1.close()

        loop2 = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop2)
            client2 = loop2.run_until_complete(get_s3_client(endpoint_url=moto_server))
            assert client1 is not client2
            loop2.run_until_complete(client2.__aexit__(None, None, None))
        finally:
            loop2.close()
            asyncio.set_event_loop(None)
