import asyncio
import importlib
import inspect
import threading
import typing as T

__all__ = [
    "patch_megfile_smart_methods",
]


class _AsyncToSync:
    """Convert an async callable into a synchronous callable.

    :param func: Async callable to wrap.
    """

    def __init__(self, func):
        """Initialize the wrapper.

        :param func: Async callable to wrap.
        """
        self.func = func

    def __call__(self, *args, **kwargs):
        """Call the wrapped function synchronously.

        :param args: Positional arguments for the wrapped function.
        :param kwargs: Keyword arguments for the wrapped function.
        :return: The computed result.
        """
        result = self.func(*args, **kwargs)
        if inspect.isawaitable(result):
            result = _run_coroutine(result)
        return _wrap_async_result(result)


class _LoopRunner:
    """Run coroutines on a dedicated event loop in a background thread."""

    def __init__(self):
        """Initialize the runner and start the event loop thread."""
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        """Run the event loop forever in the background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro):
        """Execute the coroutine on the background loop.

        :param coro: Awaitable object to execute.
        :return: Result from the coroutine.
        """
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def close(self):
        """Stop and close the background event loop."""
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()


class _SyncAsyncContextManager:
    """Sync adapter for an async context manager."""

    def __init__(self, async_cm, run_awaitable=None):
        """Initialize with an async context manager.

        :param async_cm: Async context manager instance.
        :param run_awaitable: Callable that runs an awaitable.
        """
        self._async_cm = async_cm
        self._owns_runner = run_awaitable is None
        if self._owns_runner:
            self._runner = _LoopRunner()
            self._run = self._runner.run
        else:
            self._runner = None
            self._run = run_awaitable

    def __enter__(self):
        """Enter the async context manager synchronously."""
        result = self._run(self._async_cm.__aenter__())
        return _wrap_async_result(result, run_awaitable=self._run)

    def __exit__(self, exc_type, exc, traceback):
        """Exit the async context manager synchronously."""
        try:
            return self._run(self._async_cm.__aexit__(exc_type, exc, traceback))
        finally:
            if self._owns_runner and self._runner is not None:
                self._runner.close()


class _SyncAsyncIterator:
    """Sync adapter for an async iterator."""

    def __init__(self, async_iter, run_awaitable=None):
        """Initialize with an async iterator.

        :param async_iter: Async iterator instance.
        :param run_awaitable: Callable that runs an awaitable.
        """
        self._async_iter = async_iter
        self._owns_runner = run_awaitable is None
        if self._owns_runner:
            self._runner = _LoopRunner()
            self._run = self._runner.run
        else:
            self._runner = None
            self._run = run_awaitable

    def __iter__(self):
        """Return the iterator itself."""
        return self

    def __next__(self):
        """Return the next item from the async iterator."""
        try:
            return self._run(self._async_iter.__anext__())
        except StopAsyncIteration as exc:
            if self._owns_runner and self._runner is not None:
                self._runner.close()
            raise StopIteration from exc


class _SyncAsyncProxy:
    """Proxy object that synchronizes async method calls."""

    def __init__(self, obj, run_awaitable=None):
        """Initialize the proxy.

        :param obj: Target object to proxy.
        :param run_awaitable: Callable that runs an awaitable.
        """
        self._obj = obj
        self._run = run_awaitable or _run_coroutine

    def __getattr__(self, name):
        """Proxy attributes and sync async call results."""
        attr = getattr(self._obj, name)
        if callable(attr):

            def _wrapped(*args, **kwargs):
                """Invoke the proxied callable and sync async results."""
                result = attr(*args, **kwargs)
                if inspect.isawaitable(result):
                    return self._run(result)
                return _wrap_async_result(result, run_awaitable=self._run)

            return _wrapped
        return attr

    def __iter__(self):
        """Return an iterator for the proxied object."""
        if hasattr(self._obj, "__iter__"):
            return iter(self._obj)
        if _is_async_iterator(self._obj):
            return _SyncAsyncIterator(self._obj, run_awaitable=self._run)
        raise TypeError(f"{type(self._obj)!r} is not iterable")


class _SyncAsyncFile:
    """Sync file adapter for an async file context manager."""

    def __init__(self, async_cm):
        """Initialize and enter the async context manager.

        :param async_cm: Async context manager from smart_open.
        """
        self._async_cm = async_cm
        self._runner = _LoopRunner()
        self._closed = False
        file_obj = self._runner.run(self._async_cm.__aenter__())
        self._file = _wrap_async_result(file_obj, run_awaitable=self._runner.run)

    def __getattr__(self, name):
        """Proxy file attributes and methods."""
        return getattr(self._file, name)

    def close(self):
        """Close the file and exit the async context manager."""
        if self._closed:
            return
        self._closed = True
        if hasattr(self._file, "close"):
            try:
                self._file.close()
            finally:
                self._runner.run(self._async_cm.__aexit__(None, None, None))
                self._runner.close()
        else:
            self._runner.run(self._async_cm.__aexit__(None, None, None))
            self._runner.close()

    def __enter__(self):
        """Enter the context manager and return the file itself."""
        return self

    def __exit__(self, exc_type, exc, traceback):
        """Exit the context manager and close the file."""
        if self._closed:
            return False
        self._closed = True
        try:
            return self._runner.run(self._async_cm.__aexit__(exc_type, exc, traceback))
        finally:
            self._runner.close()


class _SyncAsyncScandir:
    """Sync adapter that exposes async scandir as a sync iterator."""

    def __init__(self, async_cm):
        """Initialize with an async context manager.

        :param async_cm: Async context manager from smart_scandir.
        """
        self._async_cm = async_cm
        self._iterator = None
        self._entered = False
        self._closed = False
        self._runner = _LoopRunner()

    def _ensure_entered(self):
        """Enter the async context manager when needed."""
        if not self._entered:
            self._entered = True
            iterator = self._runner.run(self._async_cm.__aenter__())
            self._iterator = _wrap_async_result(
                iterator, run_awaitable=self._runner.run
            )
        return self._iterator

    def __iter__(self):
        """Return the iterator itself."""
        return self

    def __next__(self):
        """Return the next directory entry."""
        iterator = self._ensure_entered()
        try:
            return next(iterator)
        except StopIteration:
            self.close()
            raise

    def close(self):
        """Close the scandir iterator and exit the async context manager."""
        if self._closed:
            return
        self._closed = True
        self._runner.run(self._async_cm.__aexit__(None, None, None))
        self._runner.close()

    def __enter__(self):
        """Enter the context manager and return the iterator."""
        return self._ensure_entered()

    def __exit__(self, exc_type, exc, traceback):
        """Exit the context manager and close the iterator."""
        if self._closed:
            return False
        self._closed = True
        try:
            return self._runner.run(self._async_cm.__aexit__(exc_type, exc, traceback))
        finally:
            self._runner.close()


def _run_coroutine(coro):
    """Run an awaitable and return its result.

    :param coro: Awaitable object to run.
    :return: Result from the awaitable.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return _run_coroutine_in_thread(coro)


def _run_coroutine_in_thread(coro):
    """Run a coroutine in a dedicated thread when a loop is already running.

    :param coro: Awaitable object to run.
    :return: Result from the awaitable.
    """
    result: T.Dict[str, T.Any] = {}
    error: T.Dict[str, BaseException] = {}

    def _runner():
        """Run the coroutine in a thread-local event loop."""
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:
            error["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in error:
        raise error["error"]
    return result.get("value")


def _is_async_context_manager(obj) -> bool:
    """Return True if object implements async context manager protocol."""
    return hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__")


def _is_async_iterator(obj) -> bool:
    """Return True if object implements async iterator protocol."""
    return hasattr(obj, "__aiter__") and hasattr(obj, "__anext__")


def _has_async_methods(obj) -> bool:
    """Return True if the object exposes coroutine methods."""
    for name in (
        "read",
        "readline",
        "readlines",
        "write",
        "writelines",
        "seek",
        "tell",
        "flush",
        "close",
    ):
        attr = getattr(obj, name, None)
        if attr is not None and inspect.iscoroutinefunction(attr):
            return True
    return False


def _wrap_async_result(result, run_awaitable=None):
    """Wrap async constructs into synchronous adapters.

    :param result: Value returned from an async or sync callable.
    :param run_awaitable: Callable that runs an awaitable.
    :return: Synchronous adapter or the original value.
    """
    if isinstance(
        result,
        (_SyncAsyncContextManager, _SyncAsyncIterator, _SyncAsyncProxy, _SyncAsyncFile),
    ):
        return result
    if _has_async_methods(result):
        return _SyncAsyncProxy(result, run_awaitable=run_awaitable)
    if _is_async_iterator(result):
        return _SyncAsyncIterator(result, run_awaitable=run_awaitable)
    if _is_async_context_manager(result):
        return _SyncAsyncContextManager(result, run_awaitable=run_awaitable)
    return result


def patch_megfile_smart_methods():
    """Patch megfile smart APIs to use aiomegfile implementations.

    Only functions that exist in ``aiomegfile.smart`` are replaced.
    Replaced functions are adapted into synchronous callables.

    :return: Mapping of patched function names to their sync wrappers.
    :rtype: dict
    """
    megfile = importlib.import_module("megfile")
    meg_smart = importlib.import_module("megfile.smart")
    aio_smart = importlib.import_module("aiomegfile.smart")

    patched: T.Dict[str, T.Callable[..., T.Any]] = {}
    for name, func in aio_smart.__dict__.items():
        if name.startswith("_"):
            continue
        if not callable(func):
            continue
        if not hasattr(meg_smart, name):
            continue

        if name == "smart_open":

            def _smart_open_sync(*args, _func=func, **kwargs):
                async_cm = _func(*args, **kwargs)
                return _SyncAsyncFile(async_cm)

            wrapper = _smart_open_sync
        elif name == "smart_scandir":

            def _smart_scandir_sync(*args, _func=func, **kwargs):
                async_cm = _func(*args, **kwargs)
                return _SyncAsyncScandir(async_cm)

            wrapper = _smart_scandir_sync
        else:
            wrapper = _AsyncToSync(func)

        wrapper.__name__ = getattr(func, "__name__", name)
        wrapper.__doc__ = getattr(func, "__doc__", None)

        setattr(meg_smart, name, wrapper)
        if hasattr(megfile, name):
            setattr(megfile, name, wrapper)
        patched[name] = wrapper

    return patched
