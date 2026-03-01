import sys
import types

from aiomegfile.utils.patch import (
    _run_coroutine,
    _SyncAsyncFile,
    _SyncAsyncProxy,
    _wrap_async_result,
    patch_megfile_smart_methods,
)


def _install_fake_megfile(monkeypatch):
    """Install a fake megfile package into ``sys.modules``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of (megfile module, megfile.smart module).
    :rtype: tuple
    """
    megfile = types.ModuleType("megfile")
    megfile.__path__ = []
    meg_smart = types.ModuleType("megfile.smart")

    def smart_exists(path):
        """Placeholder smart_exists implementation.

        :param path: Input path.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}"

    def smart_open(path, mode="r", **kwargs):
        """Placeholder smart_open implementation.

        :param path: Input path.
        :param mode: File mode.
        :param kwargs: Additional keyword arguments.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}:{mode}:{kwargs}"

    def smart_scandir(path):
        """Placeholder smart_scandir implementation.

        :param path: Input path.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}"

    meg_smart.smart_exists = smart_exists
    meg_smart.smart_open = smart_open
    meg_smart.smart_scandir = smart_scandir

    megfile.smart = meg_smart
    megfile.smart_exists = smart_exists
    megfile.smart_open = smart_open
    megfile.smart_scandir = smart_scandir

    monkeypatch.setitem(sys.modules, "megfile", megfile)
    monkeypatch.setitem(sys.modules, "megfile.smart", meg_smart)
    return megfile, meg_smart


def test_patch_megfile_smart_exists(tmp_path, monkeypatch):
    """Replace megfile smart_exists with aiomegfile's implementation.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patched = patch_megfile_smart_methods()
    assert "smart_exists" in patched

    file_path = tmp_path / "exists.txt"
    file_path.write_text("ok", encoding="utf-8")

    import megfile.smart as meg_smart

    assert meg_smart.smart_exists(str(file_path)) is True


def test_patch_megfile_smart_open(tmp_path, monkeypatch):
    """Expose a sync smart_open that works with context managers.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patch_megfile_smart_methods()

    import megfile.smart as meg_smart

    file_path = tmp_path / "data.txt"
    with meg_smart.smart_open(str(file_path), "w") as handle:
        handle.write("hello")

    with meg_smart.smart_open(str(file_path), "r") as handle:
        assert handle.read() == "hello"


def test_patch_megfile_smart_scandir(tmp_path, monkeypatch):
    """Expose a sync scandir iterator.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patch_megfile_smart_methods()

    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "b.txt").write_text("b", encoding="utf-8")

    import megfile.smart as meg_smart

    entries = list(meg_smart.smart_scandir(str(tmp_path)))
    names = sorted(entry.name for entry in entries)
    assert names == ["a.txt", "b.txt"]

    with meg_smart.smart_scandir(str(tmp_path)) as iterator:
        names = sorted(entry.name for entry in iterator)
        assert names == ["a.txt", "b.txt"]


async def test_run_coroutine_inside_loop():
    """Run coroutine via thread when an event loop is active.

    :return: None
    :rtype: None
    """

    async def sample() -> str:
        """Return a sentinel value.

        :return: Sentinel value.
        :rtype: str
        """

        return "ok"

    result = _run_coroutine(sample())
    assert result == "ok"


def test_wrap_async_result_with_async_iterator():
    """Wrap async iterator into a sync iterator.

    :return: None
    :rtype: None
    """

    class AsyncIter:
        """Simple async iterator for testing."""

        def __init__(self) -> None:
            """Initialize the iterator.

            :return: None
            :rtype: None
            """
            self._items = iter([1, 2])

        def __aiter__(self):
            """Return self as an async iterator.

            :return: Async iterator instance.
            :rtype: AsyncIter
            """
            return self

        async def __anext__(self) -> int:
            """Return the next item or raise StopAsyncIteration.

            :return: Next integer item.
            :rtype: int
            """
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    wrapped = _wrap_async_result(AsyncIter())
    assert list(wrapped) == [1, 2]


def test_wrap_async_result_with_async_context_manager():
    """Wrap async context manager and proxy async methods.

    :return: None
    :rtype: None
    """

    class AsyncValue:
        """Async value provider."""

        async def read(self) -> str:
            """Return a payload string.

            :return: Payload string.
            :rtype: str
            """

            return "payload"

    class AsyncManager:
        """Async context manager returning AsyncValue."""

        async def __aenter__(self) -> AsyncValue:
            """Enter the async context.

            :return: AsyncValue instance.
            :rtype: AsyncValue
            """

            return AsyncValue()

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            """Exit the async context.

            :return: None
            :rtype: None
            """

            return None

    wrapped = _wrap_async_result(AsyncManager())
    with wrapped as handle:
        assert handle.read() == "payload"


def test_sync_async_proxy_iterates_async_iterator():
    """Sync proxy should iterate async iterators.

    :return: None
    :rtype: None
    """

    class AsyncIter:
        """Async iterator for proxy testing."""

        def __init__(self) -> None:
            """Initialize iterator.

            :return: None
            :rtype: None
            """
            self._items = iter(["a", "b"])

        def __aiter__(self):
            """Return self as an async iterator.

            :return: Async iterator instance.
            :rtype: AsyncIter
            """
            return self

        async def __anext__(self) -> str:
            """Return next value or raise StopAsyncIteration.

            :return: Next string item.
            :rtype: str
            """
            try:
                return next(self._items)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    proxy = _SyncAsyncProxy(AsyncIter())
    assert list(proxy) == ["a", "b"]


def test_sync_async_file_close_idempotent():
    """Sync async file should close once without error.

    :return: None
    :rtype: None
    """

    class AsyncFile:
        """Async file-like object."""

        def __init__(self) -> None:
            """Initialize file state.

            :return: None
            :rtype: None
            """
            self.closed = False

        async def read(self) -> str:
            """Return file content.

            :return: File content.
            :rtype: str
            """

            return "data"

        async def close(self) -> None:
            """Close the file.

            :return: None
            :rtype: None
            """
            self.closed = True

    class AsyncFileManager:
        """Async context manager for AsyncFile."""

        def __init__(self) -> None:
            """Initialize the manager.

            :return: None
            :rtype: None
            """
            self.file = AsyncFile()

        async def __aenter__(self) -> AsyncFile:
            """Enter the async context.

            :return: AsyncFile instance.
            :rtype: AsyncFile
            """

            return self.file

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            """Exit the async context.

            :return: None
            :rtype: None
            """
            self.file.closed = True

    manager = AsyncFileManager()
    sync_file = _SyncAsyncFile(manager)
    assert sync_file.read() == "data"

    sync_file.close()
    sync_file.close()
    assert manager.file.closed is True
