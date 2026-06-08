import asyncio
import io

import pytest
from moto.server import ThreadedMotoServer

import aiomegfile.filesystem.s3 as s3_module
from aiomegfile.errors import (
    S3BucketNotFoundError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3UnsupportedError,
    SameFileError,
)
from aiomegfile.filesystem.s3 import (
    MultiPartWriter,
    S3FileSystem,
    _become_prefix,
    _group_s3path_by_prefix,
    _parse_s3_path_ignore_brace,
    _s3_entry_name,
    _s3_split_magic,
    _s3_split_magic_ignore_brace,
    get_access_token,
    get_endpoint_url,
    get_env_var,
    get_s3_client,
    is_s3,
    parse_s3_path,
)
from aiomegfile.interfaces import Access

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

    async def _get_object_content(self, filesystem: S3FileSystem, key: str) -> bytes:
        """Return object content from the test bucket.

        :param filesystem: S3FileSystem instance.
        :param key: Object key to read.
        :return: Object content.
        :rtype: bytes
        """
        client = await filesystem._get_client()
        resp = await client.get_object(Bucket=_bucket_name, Key=key)
        return await resp["Body"].read()

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

    async def test_access_read_write(self, filesystem):
        """Test access reports read and write permissions."""
        await self._create_bucket(filesystem)
        key = "access/file.txt"
        await self._put_object(filesystem, key, b"data")

        path = f"{_bucket_name}/{key}"
        assert await filesystem.access(path, mode=Access.READ) is True
        assert await filesystem.access(path, mode=Access.WRITE) is True

    async def test_access_missing_returns_false(self, filesystem):
        """Test access returns False for missing objects."""
        await self._create_bucket(filesystem)
        missing_path = f"{_bucket_name}/missing.txt"
        assert await filesystem.access(missing_path, mode=Access.READ) is False

    async def test_access_invalid_mode_raises(self, filesystem):
        """Test access raises TypeError for unsupported modes."""
        await self._create_bucket(filesystem)
        with pytest.raises(TypeError):
            await filesystem.access(f"{_bucket_name}/invalid.txt", mode="invalid")  # type: ignore[arg-type]

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

    async def test_scandir_root_lists_buckets(self, filesystem):
        """Test scandir on root lists buckets.

        :return: None
        :rtype: None
        """
        await self._create_bucket(filesystem)

        entries = []
        async with filesystem.scandir("") as scanner:
            async for entry in scanner:
                entries.append(entry)

        names = [entry.name for entry in entries]
        assert _bucket_name in names

        bucket_entry = next(entry for entry in entries if entry.name == _bucket_name)
        assert bucket_entry.is_dir() is True
        assert bucket_entry.stat.st_ctime > 0

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

    async def test_scanfile_file_path(self, filesystem):
        """Test scanfile returns the file when path is a file."""
        await self._create_bucket(filesystem)

        filename = "scanfile_single.txt"
        await self._put_object(filesystem, filename, b"content")

        entries = []
        async with filesystem.scanfile(f"{_bucket_name}/{filename}") as scanner:
            async for entry in scanner:
                entries.append(entry)

        assert len(entries) == 1
        entry = entries[0]
        assert entry.name == filename
        assert entry.path == f"{_bucket_name}/{filename}"
        assert entry.stat.isdir is False

    async def test_scanfile_sort_false_uses_fast_list(self, filesystem, monkeypatch):
        """Test scanfile defaults to the fast recursive S3 listing path."""
        await self._create_bucket(filesystem)

        prefix = "scanfile_fast_default"
        await self._put_object(filesystem, f"{prefix}/a.txt", b"a")
        await self._put_object(filesystem, f"{prefix}/nested/b.txt", b"b")

        called = False

        async def fake_fast_list(
            client, bucket: str, key_prefix: str, *, error_path: str
        ):
            nonlocal called
            called = True
            _ = error_path
            yield await client.list_objects_v2(
                Bucket=bucket,
                Prefix=key_prefix,
                MaxKeys=s3_module.MAX_KEYS,
            )

        monkeypatch.setattr(
            s3_module, "_s3_fast_list_objects_recursive", fake_fast_list
        )

        entries = []
        async with filesystem.scanfile(f"{_bucket_name}/{prefix}") as scanner:
            async for entry in scanner:
                entries.append(entry)

        assert called is True
        assert sorted(entry.path for entry in entries) == [
            f"{_bucket_name}/{prefix}/a.txt",
            f"{_bucket_name}/{prefix}/nested/b.txt",
        ]

    async def test_scanfile_sort_true_skips_fast_list(self, filesystem, monkeypatch):
        """Test scanfile(sort=True) keeps the ordered listing path."""
        await self._create_bucket(filesystem)

        prefix = "scanfile_sorted"
        await self._put_object(filesystem, f"{prefix}/a.txt", b"a")
        await self._put_object(filesystem, f"{prefix}/nested/b.txt", b"b")

        async def fail_fast_list(*args, **kwargs):
            raise AssertionError("fast list should not be used when sort=True")
            yield

        monkeypatch.setattr(
            s3_module, "_s3_fast_list_objects_recursive", fail_fast_list
        )

        entries = []
        async with filesystem.scanfile(
            f"{_bucket_name}/{prefix}", sort=True
        ) as scanner:
            async for entry in scanner:
                entries.append(entry)

        assert sorted(entry.path for entry in entries) == [
            f"{_bucket_name}/{prefix}/a.txt",
            f"{_bucket_name}/{prefix}/nested/b.txt",
        ]

    async def test_glob_stat_non_recursive(self, filesystem):
        """Test glob_stat matches non-recursive patterns."""
        await self._create_bucket(filesystem)

        prefix = "glob_stat_non_recursive"
        await self._put_object(filesystem, f"{prefix}/file1.txt", b"1")
        await self._put_object(filesystem, f"{prefix}/file2.log", b"2")
        await self._put_object(filesystem, f"{prefix}/subdir/file3.txt", b"3")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{prefix}/*.txt"):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == [f"{_bucket_name}/{prefix}/file1.txt"]

    async def test_glob_stat_recursive(self, filesystem):
        """Test glob_stat matches recursive patterns."""
        await self._create_bucket(filesystem)

        prefix = "glob_stat_recursive"
        await self._put_object(filesystem, f"{prefix}/file1.txt", b"1")
        await self._put_object(filesystem, f"{prefix}/subdir/file2.txt", b"2")
        await self._put_object(
            filesystem,
            f"{prefix}/subdir/nested/file3.txt",
            b"3",
        )
        await self._put_object(filesystem, f"{prefix}/file4.log", b"4")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{prefix}/**/*.txt"):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == sorted(
            [
                f"{_bucket_name}/{prefix}/file1.txt",
                f"{_bucket_name}/{prefix}/subdir/file2.txt",
                f"{_bucket_name}/{prefix}/subdir/nested/file3.txt",
            ]
        )

    async def test_glob_stat_directory_pattern(self, filesystem):
        """Test glob_stat matches directory patterns ending with slash."""
        await self._create_bucket(filesystem)

        prefix = "glob_stat_dir_pattern"
        await self._put_object(filesystem, f"{prefix}/subdir/file.txt", b"1")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{prefix}/*/"):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == [f"{_bucket_name}/{prefix}/subdir/"]
        for entry in entries:
            assert entry.stat.isdir is True

    async def test_glob_stat_missing_ok_false(self, filesystem):
        """Test glob_stat raises when missing_ok is False and no matches."""
        await self._create_bucket(filesystem)

        with pytest.raises(S3FileNotFoundError):
            async for _ in filesystem.glob_stat(
                f"{_bucket_name}/glob_stat_missing/*.txt",
                missing_ok=False,
            ):
                pass

    async def test_glob_stat_bucket_wildcard(self, filesystem):
        """Test glob_stat expands bucket wildcard patterns."""
        client = await filesystem._get_client()
        bucket_one = "globstat-bucket-1"
        bucket_two = "globstat-bucket-2"
        await client.create_bucket(Bucket=bucket_one)
        await client.create_bucket(Bucket=bucket_two)
        await client.put_object(Bucket=bucket_one, Key="data.txt", Body=b"1")
        await client.put_object(Bucket=bucket_two, Key="data.txt", Body=b"2")

        entries = []
        async for entry in filesystem.glob_stat("globstat-bucket-*/data.txt"):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == sorted([f"{bucket_one}/data.txt", f"{bucket_two}/data.txt"])

    async def test_glob_stat_file_and_dir_same_name(self, filesystem):
        """Test glob_stat returns both file and directory for same name.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        name = "same_name"
        await self._put_object(filesystem, name, b"file")
        await self._put_object(filesystem, f"{name}/", b"")
        await self._put_object(filesystem, f"{name}/child.txt", b"child")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{name}"):
            entries.append(entry)

        pairs = sorted((entry.path, entry.stat.isdir) for entry in entries)
        assert pairs == sorted(
            [
                (f"{_bucket_name}/{name}", False),
                (f"{_bucket_name}/{name}", True),
            ]
        )

    async def test_glob_stat_dir_marker_trailing_slash(self, filesystem):
        """Test glob_stat matches directory marker with trailing slash.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        name = "marker_only"
        await self._put_object(filesystem, f"{name}/", b"")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{name}/"):
            entries.append(entry)

        pairs = sorted((entry.path, entry.stat.isdir) for entry in entries)
        assert pairs == [(f"{_bucket_name}/{name}/", True)]

    async def test_glob_stat_wildcard_with_file_and_dir(self, filesystem):
        """Test glob_stat wildcard includes file and directory of same prefix.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "alpha", b"file")
        await self._put_object(filesystem, "alpha/", b"")
        await self._put_object(filesystem, "alpha/child.txt", b"child")
        await self._put_object(filesystem, "alpha_beta", b"file")
        await self._put_object(filesystem, "alpha-beta/", b"")
        await self._put_object(filesystem, "alpha-beta/nested/file.txt", b"nested")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/alpha*"):
            entries.append(entry)

        pairs = sorted((entry.path, entry.stat.isdir) for entry in entries)
        expected = sorted(
            [
                (f"{_bucket_name}/alpha", False),
                (f"{_bucket_name}/alpha", True),
                (f"{_bucket_name}/alpha_beta", False),
                (f"{_bucket_name}/alpha-beta", True),
            ]
        )
        assert pairs == expected

    async def test_glob_stat_deep_leaf_txt(self, filesystem):
        """Test glob_stat finds leaf txt files in deep directory trees.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        prefix = "deep_tree"
        keys = [
            f"{prefix}/l1/l2/l3/l4/leaf.txt",
            f"{prefix}/l1/l2/l3/l4/leaf2.txt",
            f"{prefix}/l1/l2/l3/other.md",
            f"{prefix}/l1/other.txt",
            f"{prefix}/l1/l2/l3/l4/leaf.log",
        ]
        for key in keys:
            await self._put_object(filesystem, key, b"data")

        entries = []
        async for entry in filesystem.glob_stat(f"{_bucket_name}/{prefix}/**/*.txt"):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == sorted(
            [
                f"{_bucket_name}/{prefix}/l1/l2/l3/l4/leaf.txt",
                f"{_bucket_name}/{prefix}/l1/l2/l3/l4/leaf2.txt",
                f"{_bucket_name}/{prefix}/l1/other.txt",
            ]
        )
        assert all(entry.stat.isdir is False for entry in entries)

    async def test__glob_stat_single_path_non_magic_file_and_dir(self, filesystem):
        """Test _glob_stat_single_path returns both file and directory entries.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        name = "single_path_same"
        await self._put_object(filesystem, name, b"file")
        await self._put_object(filesystem, f"{name}/child.txt", b"child")

        entries = []
        async for entry in filesystem._glob_stat_single_path(f"{_bucket_name}/{name}"):
            entries.append(entry)

        pairs = sorted((entry.path, entry.stat.isdir) for entry in entries)
        assert pairs == sorted(
            [
                (f"{_bucket_name}/{name}", False),
                (f"{_bucket_name}/{name}", True),
            ]
        )

    async def test__glob_stat_single_path_non_recursive_double_star(self, filesystem):
        """Test _glob_stat_single_path treats ** as * when recursive is False.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        prefix = "single_path_non_recursive"
        await self._put_object(filesystem, f"{prefix}/a/file.txt", b"1")
        await self._put_object(filesystem, f"{prefix}/a/b/file.txt", b"2")

        entries = []
        async for entry in filesystem._glob_stat_single_path(
            f"{_bucket_name}/{prefix}/**/file.txt",
            recursive=False,
        ):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == [f"{_bucket_name}/{prefix}/a/file.txt"]

    async def test__glob_stat_single_path_directory_pattern(self, filesystem):
        """Test _glob_stat_single_path matches directories with trailing slash.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        prefix = "single_path_dir_pattern"
        await self._put_object(filesystem, f"{prefix}/one/file.txt", b"1")
        await self._put_object(filesystem, f"{prefix}/two/nested/file.txt", b"2")

        entries = []
        async for entry in filesystem._glob_stat_single_path(
            f"{_bucket_name}/{prefix}/*/"
        ):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == sorted(
            [
                f"{_bucket_name}/{prefix}/one/",
                f"{_bucket_name}/{prefix}/two/",
            ]
        )
        assert all(entry.stat.isdir is True for entry in entries)

    async def test__glob_stat_single_path_deep_leaf_txt(self, filesystem):
        """Test _glob_stat_single_path finds leaf txt files in deep trees.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        prefix = "single_path_deep_tree"
        keys = [
            f"{prefix}/a/b/c/d/leaf.txt",
            f"{prefix}/a/b/c/d/leaf2.txt",
            f"{prefix}/a/b/c/d/leaf.log",
            f"{prefix}/a/b/other.txt",
        ]
        for key in keys:
            await self._put_object(filesystem, key, b"data")

        entries = []
        async for entry in filesystem._glob_stat_single_path(
            f"{_bucket_name}/{prefix}/**/*.txt"
        ):
            entries.append(entry)

        paths = sorted(entry.path for entry in entries)
        assert paths == sorted(
            [
                f"{_bucket_name}/{prefix}/a/b/c/d/leaf.txt",
                f"{_bucket_name}/{prefix}/a/b/c/d/leaf2.txt",
                f"{_bucket_name}/{prefix}/a/b/other.txt",
            ]
        )
        assert all(entry.stat.isdir is False for entry in entries)

    async def test__glob_stat_single_path_double_star_slash_star(self, filesystem):
        """Test _glob_stat_single_path with "**/*" matches dirs and files.

        :param filesystem: S3FileSystem instance.
        """
        await self._create_bucket(filesystem)

        prefix = "single_path_starstar"
        keys = [
            f"{prefix}/dir1/file1.txt",
            f"{prefix}/dir1/subdir/file2.txt",
            f"{prefix}/dir2/file3.log",
            f"{prefix}/root.txt",
            f"{prefix}/dir2/",
        ]
        for key in keys:
            await self._put_object(filesystem, key, b"data")

        entries = []
        async for entry in filesystem._glob_stat_single_path(
            f"{_bucket_name}/{prefix}/**/*"
        ):
            entries.append(entry)

        pairs = sorted((entry.path, entry.stat.isdir) for entry in entries)
        expected = sorted(
            [
                (f"{_bucket_name}/{prefix}/dir1", True),
                (f"{_bucket_name}/{prefix}/dir1/subdir", True),
                (f"{_bucket_name}/{prefix}/dir2", True),
                (f"{_bucket_name}/{prefix}/dir1/file1.txt", False),
                (f"{_bucket_name}/{prefix}/dir1/subdir/file2.txt", False),
                (f"{_bucket_name}/{prefix}/dir2/file3.log", False),
                (f"{_bucket_name}/{prefix}/root.txt", False),
            ]
        )
        assert pairs == expected

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

    async def test_symlink_and_readlink_are_unsupported(self, filesystem):
        """Test S3 symlink creation and reading are unsupported."""
        with pytest.raises(S3UnsupportedError):
            await filesystem.symlink(
                f"{_bucket_name}/symlink_src.txt",
                f"{_bucket_name}/symlink_dst.txt",
            )

        with pytest.raises(S3UnsupportedError):
            await filesystem.readlink(f"{_bucket_name}/symlink_dst.txt")

    async def test_is_symlink(self, filesystem):
        """Test S3 never reports objects as symlinks."""
        await self._create_bucket(filesystem)

        client = await filesystem._get_client()
        await client.put_object(
            Bucket=_bucket_name,
            Key="legacy_link_file.txt",
            Body=b"",
            Metadata={"symlink_to": f"s3://{_bucket_name}/regular_file.txt"},
        )

        path = f"{_bucket_name}/legacy_link_file.txt"
        stat_result = await filesystem.stat(path, followlinks=True)
        assert await filesystem.is_symlink(path) is False
        assert stat_result.islnk is False
        assert await filesystem.is_symlink(f"{_bucket_name}/nonexistent.txt") is False

    async def test_readlink_not_a_link(self, filesystem):
        """Test readlink raises unsupported for regular files."""
        await self._create_bucket(filesystem)

        await self._put_object(filesystem, "regular.txt", b"content")

        with pytest.raises(S3UnsupportedError):
            await filesystem.readlink(f"{_bucket_name}/regular.txt")

    async def test_symlink_errors(self, filesystem):
        """Test symlink always raises unsupported."""
        with pytest.raises(S3UnsupportedError):
            await filesystem.symlink("/src", f"{_bucket_name}/dst")

        with pytest.raises(S3UnsupportedError):
            await filesystem.symlink(f"{_bucket_name}/src.txt", "/dst")

        with pytest.raises(S3UnsupportedError):
            await filesystem.symlink(f"{_bucket_name}/src.txt", f"{_bucket_name}/dst/")

    async def test_readlink_errors(self, filesystem):
        """Test readlink always raises unsupported."""
        with pytest.raises(S3UnsupportedError):
            await filesystem.readlink("/path")

        with pytest.raises(S3UnsupportedError):
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

    async def test_multipart_writer_orders_parts(self, filesystem):
        """Test MultiPartWriter orders parts before completion.

        :param filesystem: S3FileSystem fixture.
        """
        await self._create_bucket(filesystem)
        client = await filesystem._get_client()

        part_size = 5 * 1024 * 1024
        part1 = io.BytesIO(b"a" * part_size)
        part2 = io.BytesIO(b"b" * part_size)

        dest_key = "multipart_order.txt"
        dest_path = f"{_bucket_name}/{dest_key}"

        async with MultiPartWriter(client, dest_path) as writer:
            await writer.upload_part(2, part2)
            await writer.upload_part(1, part1)

        content = await self._get_object_content(filesystem, dest_key)
        assert content == b"a" * part_size + b"b" * part_size

    async def test_multipart_writer_upload_part_by_paths_with_range(self, filesystem):
        """Test upload_part_by_paths concatenates ranged content.

        :param filesystem: S3FileSystem fixture.
        """
        await self._create_bucket(filesystem)
        client = await filesystem._get_client()

        part_size = 5 * 1024 * 1024
        await self._put_object(filesystem, "range_a.bin", b"a" * (part_size + 1))
        await self._put_object(filesystem, "range_b.bin", b"b" * 1024 * 1024)

        dest_key = "multipart_range.txt"
        dest_path = f"{_bucket_name}/{dest_key}"
        range_end = part_size - 1
        paths = [
            (f"{_bucket_name}/range_a.bin", f"bytes=0-{range_end}"),
            (f"{_bucket_name}/range_b.bin", None),
        ]

        async with MultiPartWriter(client, dest_path) as writer:
            await writer.upload_part_by_paths(1, paths)

        content = await self._get_object_content(filesystem, dest_key)
        assert content == b"a" * part_size + b"b" * 1024 * 1024

    async def test_concat_block_size_zero(self, filesystem):
        """Test concat with block_size=0 concatenates by copy.

        :param filesystem: S3FileSystem fixture.
        """
        await self._create_bucket(filesystem)
        part_size = 5 * 1024 * 1024
        await self._put_object(filesystem, "copy_a.bin", b"a" * part_size)
        await self._put_object(filesystem, "copy_b.bin", b"b" * part_size)

        dest_key = "concat_copy.bin"
        await filesystem.concat(
            [f"{_bucket_name}/copy_a.bin", f"{_bucket_name}/copy_b.bin"],
            f"{_bucket_name}/{dest_key}",
            block_size=0,
        )

        content = await self._get_object_content(filesystem, dest_key)
        assert content == b"a" * part_size + b"b" * part_size


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

    def test_s3_helper_prefix_and_entry(self):
        """Test prefix normalization and entry name helpers.

        :return: None
        :rtype: None
        """
        assert _become_prefix("prefix") == "prefix/"
        assert _become_prefix("prefix/") == "prefix/"
        assert _become_prefix("") == ""

        assert _s3_entry_name("dir/file.txt") == "file.txt"
        assert _s3_entry_name("dir/file.txt/") == "file.txt"
        assert _s3_entry_name("") == ""

    def test_parse_s3_path_ignore_brace(self):
        """Test parsing bucket/key while ignoring braces.

        :return: None
        :rtype: None
        """
        bucket, key = _parse_s3_path_ignore_brace("{buck/et}/path/to/file")
        assert bucket == "{buck/et}"
        assert key == "path/to/file"

        bucket, key = _parse_s3_path_ignore_brace("bucket")
        assert bucket == "bucket"
        assert key == ""

    def test_s3_split_magic_helpers(self):
        """Test splitting magic parts of S3 paths.

        :return: None
        :rtype: None
        """
        top_dir, magic = _s3_split_magic_ignore_brace("dir/{a,b}/file?.txt")
        assert top_dir == "dir/{a,b}"
        assert magic == "file?.txt"

        prefix, magic_part = _s3_split_magic("abc*def")
        assert prefix == "abc"
        assert magic_part == "*def"

    def test_group_s3path_by_prefix(self):
        """Test grouping paths by expanded prefix.

        :return: None
        :rtype: None
        """
        grouped = _group_s3path_by_prefix("bucket/{a,b}/file.txt")
        assert sorted(grouped) == [
            "bucket/a/file.txt",
            "bucket/b/file.txt",
        ]

        assert _group_s3path_by_prefix("bucket") == ["bucket"]

    def test_get_env_var_with_profile(self, monkeypatch):
        """Test fetching environment variables with profiles.

        :param monkeypatch: Pytest monkeypatch fixture.
        :return: None
        :rtype: None
        """
        monkeypatch.setenv("DEMO__AWS_ACCESS_KEY_ID", "profile-key")
        assert get_env_var("AWS_ACCESS_KEY_ID", profile_name="demo") == "profile-key"

    def test_get_endpoint_url_from_env(self, monkeypatch):
        """Test endpoint URL selection from environment.

        :param monkeypatch: Pytest monkeypatch fixture.
        :return: None
        :rtype: None
        """
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://endpoint")
        assert get_endpoint_url() == "http://endpoint"

        monkeypatch.setenv("PROFILE__AWS_ENDPOINT_URL", "http://profile-endpoint")
        assert get_endpoint_url(profile_name="profile") == "http://profile-endpoint"

    def test_get_access_token_prefers_env_and_config(self, monkeypatch):
        """Test access token resolution from env and config.

        :param monkeypatch: Pytest monkeypatch fixture.
        :return: None
        :rtype: None
        """
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

        access_key, secret_key, session_token = get_access_token()
        assert access_key == "env-key"
        assert secret_key == "env-secret"
        assert session_token is None

        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        config = {
            "aws_access_key_id": "config-key",
            "aws_secret_access_key": "config-secret",
            "aws_session_token": "token",
        }
        access_key, secret_key, session_token = get_access_token(config=config)
        assert access_key == "config-key"
        assert secret_key == "config-secret"
        assert session_token == "token"

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


class TestMD5Header:
    """Test that Content-MD5 header is added to DeleteObjects operations.

    This is a workaround for https://github.com/aws/aws-cli/issues/9214
    """

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

    async def test_delete_objects_has_md5_header(self, filesystem):
        """Test that DeleteObjects operation includes Content-MD5 header."""
        await self._create_bucket(filesystem)

        # Create some test objects in a directory
        # DeleteObjects is only called when removing multiple files (directory)
        await self._put_object(filesystem, "test_dir/file1.txt", b"content1")
        await self._put_object(filesystem, "test_dir/file2.txt", b"content2")
        await self._put_object(filesystem, "test_dir/file3.txt", b"content3")

        # Track if MD5 header is present
        md5_headers_found = []

        def capture_md5_header(params, **kwargs):
            """Capture Content-MD5 header from request params."""
            if "headers" in params and "Content-MD5" in params["headers"]:
                md5_headers_found.append(params["headers"]["Content-MD5"])

        # Register our test handler
        client = await filesystem._get_client()
        client.meta.events.register("before-call.s3.DeleteObjects", capture_md5_header)

        # Trigger DeleteObjects by removing a directory with multiple files
        await filesystem.remove(f"{_bucket_name}/test_dir/")

        # Verify MD5 header was added
        assert len(md5_headers_found) > 0, (
            "Content-MD5 header should be added to DeleteObjects"
        )

        # Verify MD5 value is a valid base64 string
        md5_value = md5_headers_found[0]
        assert isinstance(md5_value, str)
        assert len(md5_value) > 0
        # Base64 encoded MD5 should be 24 characters long (16 bytes -> 24 base64 chars)
        assert len(md5_value) == 24, f"MD5 should be 24 chars, got {len(md5_value)}"

    async def test_delete_objects_md5_value_correctness(self, filesystem):
        """Test that the MD5 value is correctly calculated."""
        import base64
        import hashlib

        await self._create_bucket(filesystem)

        # Create test objects in a directory to trigger DeleteObjects
        await self._put_object(filesystem, "delete_test/obj1.txt", b"data1")
        await self._put_object(filesystem, "delete_test/obj2.txt", b"data2")

        # Capture the actual request body and MD5 header
        captured_data = {}

        def capture_request_data(params, **kwargs):
            """Capture both body and MD5 header."""
            if "body" in params:
                captured_data["body"] = params["body"]
            if "headers" in params and "Content-MD5" in params["headers"]:
                captured_data["md5_header"] = params["headers"]["Content-MD5"]

        client = await filesystem._get_client()
        client.meta.events.register(
            "before-call.s3.DeleteObjects", capture_request_data
        )

        # Trigger DeleteObjects by removing the directory
        await filesystem.remove(f"{_bucket_name}/delete_test/")

        # Verify we captured the data
        assert "body" in captured_data, "Should have captured request body"
        assert "md5_header" in captured_data, "Should have captured MD5 header"

        # Calculate expected MD5
        body_bytes = captured_data["body"]
        if isinstance(body_bytes, str):
            body_bytes = body_bytes.encode("utf-8")

        expected_md5 = base64.b64encode(hashlib.md5(body_bytes).digest()).decode(
            "utf-8"
        )
        actual_md5 = captured_data["md5_header"]

        assert actual_md5 == expected_md5, (
            f"MD5 mismatch: expected {expected_md5}, got {actual_md5}"
        )
