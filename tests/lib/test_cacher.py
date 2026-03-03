import pytest

from aiomegfile.lib.cacher import AioFileCacher


class TestAioCacher:
    """Tests for AioFileCacher."""

    async def test_read_downloads_without_upload(self, tmp_path):
        """Read mode should download and skip upload."""
        payload = b"payload"
        download_calls = []
        upload_calls = []

        async def download(path, fileobj):
            download_calls.append(path)
            await fileobj.write(payload)

        async def upload(fileobj, path):
            upload_calls.append(path)

        cache_dir = tmp_path / "cache"
        async with AioFileCacher(
            "remote://file",
            "rb",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(cache_dir),
        ) as cacher:
            result = await cacher.read()

        assert result == payload
        assert download_calls == ["remote://file"]
        assert upload_calls == []
        assert cache_dir.exists()

    async def test_write_uploads(self, tmp_path):
        """Write mode should upload on close."""
        uploaded = {}

        async def download(path, fileobj):
            raise AssertionError("download should not be called")

        async def upload(fileobj, path):
            uploaded["path"] = path
            uploaded["data"] = await fileobj.read()

        cache_dir = tmp_path / "cache"
        async with AioFileCacher(
            "remote://file",
            "wb",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(cache_dir),
        ) as cacher:
            await cacher.write(b"abc")

        assert uploaded["path"] == "remote://file"
        assert uploaded["data"] == b"abc"

    async def test_append_plus_allows_seek(self, tmp_path):
        """Append-plus mode should allow seeking for reads."""
        uploaded = {}

        async def download(path, fileobj):
            await fileobj.write("hello")

        async def upload(fileobj, path):
            uploaded["data"] = await fileobj.read()

        cache_dir = tmp_path / "cache"
        async with AioFileCacher(
            "remote://file",
            "a+",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(cache_dir),
        ) as cacher:
            pos = await cacher.seek(0)
            assert pos == 0
            result = await cacher.read()
            assert result == "hello"
            await cacher.write("world")

        assert uploaded["data"] == "helloworld"

    async def test_read_before_open_raises(self):
        """Raise error when reading before open.

        :raises RuntimeError: When file is not opened.
        """

        async def download(path, fileobj):
            raise AssertionError("download should not be called")

        async def upload(fileobj, path):
            raise AssertionError("upload should not be called")

        cacher = AioFileCacher(
            "remote://file",
            "rb",
            download_fileobj=download,
            upload_fileobj=upload,
        )
        with pytest.raises(RuntimeError):
            await cacher.read()

    async def test_read_in_write_mode_raises(self, tmp_path):
        """Raise error when reading from write-only mode.

        :raises IOError: When file is not open for reading.
        """

        async def download(path, fileobj):
            raise AssertionError("download should not be called")

        async def upload(fileobj, path):
            return None

        async with AioFileCacher(
            "remote://file",
            "wb",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(tmp_path / "cache"),
        ) as cacher:
            with pytest.raises(IOError):
                await cacher.read()

    async def test_write_in_read_mode_raises(self, tmp_path):
        """Raise error when writing to read-only mode.

        :raises IOError: When file is not open for writing.
        """

        async def download(path, fileobj):
            return None

        async def upload(fileobj, path):
            raise AssertionError("upload should not be called")

        async with AioFileCacher(
            "remote://file",
            "rb",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(tmp_path / "cache"),
        ) as cacher:
            with pytest.raises(IOError):
                await cacher.write(b"x")

    async def test_readlines_and_readinto(self, tmp_path):
        """Readlines and readinto should return expected data."""
        content = b"line1\nline2\n"

        async def download(path, fileobj):
            await fileobj.write(content)

        async def upload(fileobj, path):
            raise AssertionError("upload should not be called")

        async with AioFileCacher(
            "remote://file",
            "rb",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(tmp_path / "cache"),
        ) as cacher:
            lines = await cacher.readlines()
            assert lines == [b"line1\n", b"line2\n"]
            await cacher.seek(0)
            buffer = bytearray(len(content))
            size = await cacher.readinto(buffer)
            assert size == len(content)
            assert bytes(buffer) == content

    async def test_writelines_truncate_tell_flush(self, tmp_path):
        """Test writelines, truncate, tell and flush behaviors."""
        uploaded = {}

        async def download(path, fileobj):
            return None

        async def upload(fileobj, path):
            uploaded["data"] = await fileobj.read()

        async with AioFileCacher(
            "remote://file",
            "wb+",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(tmp_path / "cache"),
        ) as cacher:
            await cacher.writelines([b"aa", b"bb", b"cc"])
            pos = await cacher.tell()
            assert pos == 6
            await cacher.flush()
            await cacher.truncate(4)
            await cacher.seek(0)
            data = await cacher.read()
            assert data == b"aabb"

        assert uploaded["data"] == b"aabb"

    async def test_seek_append_end(self, tmp_path):
        """Append-only mode should seek to end regardless of offset."""
        uploaded = {}

        async def download(path, fileobj):
            await fileobj.write(b"hello")

        async def upload(fileobj, path):
            uploaded["data"] = await fileobj.read()

        async with AioFileCacher(
            "remote://file",
            "ab",
            download_fileobj=download,
            upload_fileobj=upload,
            cache_dir=str(tmp_path / "cache"),
        ) as cacher:
            pos = await cacher.seek(0)
            assert pos == 5
            await cacher.write(b"!")

        assert uploaded["data"] == b"hello!"
