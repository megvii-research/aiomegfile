"""Synchronous adapters used by the megfile compatibility patch."""

from __future__ import annotations

import asyncio
import inspect
import os
import pathlib
import stat as stat_module
import threading
import typing as T
from collections.abc import Sequence

from aiomegfile.interfaces import Access
from aiomegfile.utils.path import fspath, split_uri

if T.TYPE_CHECKING:
    from aiomegfile.smart_path import SmartPath as AioSmartPath


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


class _SyncPathParents(Sequence):
    """Synchronous adapter for ``SmartPath.parents``."""

    def __init__(self, parents):
        """Initialize the adapter.

        :param parents: Async SmartPath parents sequence.
        """
        self._parents = parents

    def __len__(self) -> int:
        """Return the number of parent elements."""
        return len(self._parents)

    def __getitem__(self, idx):
        """Return one or more wrapped parent paths.

        :param idx: Integer index or slice.
        :return: Wrapped parent path or tuple of wrapped parents.
        """
        if isinstance(idx, slice):
            return tuple(_wrap_async_result(item) for item in self._parents[idx])
        return _wrap_async_result(self._parents[idx])


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
            result = self._run(self._async_iter.__anext__())
            return _wrap_async_result(result, run_awaitable=self._run)
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


class _SyncSmartPath(os.PathLike):
    """Synchronous compatibility wrapper for ``aiomegfile.smart_path.SmartPath``."""

    protocol = ""
    _accepted_protocols: T.Tuple[str, ...] = ()
    _default_protocol: T.Optional[str] = None
    _path_attr_includes_protocol = True

    def __init__(self, path, *other_paths):
        """Initialize the compatibility path.

        :param path: Input path value.
        :param other_paths: Additional path segments.
        """
        self._aio_path = self._coerce_aio_path(path)
        for other_path in other_paths:
            result = _run_coroutine(
                self._aio_path.joinpath(_unwrap_sync_value(other_path))
            )
            self._aio_path = T.cast("AioSmartPath", result)
        self.protocol = self._aio_path.protocol
        self.filesystem = self._aio_path.filesystem

    @classmethod
    def _normalize_input_path(cls, path) -> T.Any:
        """Normalize input path for protocol-specific subclasses.

        :param path: Raw input path.
        :return: Normalized path object.
        """
        if isinstance(path, _SyncSmartPath):
            path = str(path)
        protocol_path = path
        if isinstance(protocol_path, int):
            raise TypeError("Integer paths are unsupported by aiomegfile SmartPath")
        protocol_path = fspath(protocol_path)
        if cls._accepted_protocols:
            protocol, _, _ = split_uri(protocol_path)
            if "://" in protocol_path:
                if protocol not in cls._accepted_protocols:
                    raise ValueError(
                        "protocol not match, expected one of: %r, got: %r"
                        % (cls._accepted_protocols, protocol_path)
                    )
                return protocol_path
            if cls._default_protocol == "file":
                return protocol_path
            prefix = T.cast(str, cls._default_protocol)
            return f"{prefix}://{protocol_path.lstrip('/')}"
        return protocol_path

    @classmethod
    def _coerce_aio_path(cls, path) -> "AioSmartPath":
        """Convert arbitrary path input into an aiomegfile SmartPath.

        :param path: Raw path value.
        :return: Async SmartPath instance.
        :rtype: AioSmartPath
        """
        from aiomegfile.smart_path import SmartPath as AioSmartPath

        if isinstance(path, AioSmartPath):
            return path
        return AioSmartPath(cls._normalize_input_path(path))

    @classmethod
    def _from_aio_path(cls, path: "AioSmartPath") -> "_SyncSmartPath":
        """Create a sync wrapper from an existing async SmartPath.

        :param path: Async SmartPath instance.
        :return: Wrapped sync path instance.
        :rtype: _SyncSmartPath
        """
        instance = cls.__new__(cls)
        instance._aio_path = path
        instance.protocol = path.protocol
        instance.filesystem = path.filesystem
        return instance

    @property
    def path(self) -> str:
        """Return the compatibility ``path`` attribute."""
        if self._path_attr_includes_protocol:
            return str(self)
        return self.path_without_protocol

    @property
    def path_with_protocol(self) -> str:
        """Return the URI string for the path."""
        return str(self)

    @property
    def path_without_protocol(self) -> str:
        """Return the path string without protocol prefix."""
        uri = str(self)
        if "://" not in uri:
            return uri
        protocol, remainder = uri.split("://", 1)
        _ = protocol
        return remainder

    @property
    def root(self) -> str:
        """Return the path root."""
        return T.cast(str, _wrap_async_result(self._aio_path.root))

    @property
    def anchor(self) -> str:
        """Return the path anchor."""
        return T.cast(str, _wrap_async_result(self._aio_path.anchor))

    @property
    def parts(self) -> T.Tuple[str, ...]:
        """Return path components."""
        return T.cast(T.Tuple[str, ...], self._aio_path.parts)

    @property
    def parents(self) -> _SyncPathParents:
        """Return a sequence of logical parent paths."""
        return _SyncPathParents(self._aio_path.parents)

    @property
    def parent(self):
        """Return the logical parent path."""
        return _wrap_async_result(self._aio_path.parent)

    @property
    def name(self) -> str:
        """Return the final path component."""
        return T.cast(str, self._aio_path.name)

    @property
    def suffix(self) -> str:
        """Return the final suffix."""
        return T.cast(str, self._aio_path.suffix)

    @property
    def suffixes(self) -> T.List[str]:
        """Return all suffixes."""
        return list(self._aio_path.suffixes)

    @property
    def stem(self) -> str:
        """Return the final path component without suffix."""
        return T.cast(str, self._aio_path.stem)

    @property
    def drive(self) -> str:
        """Return the drive part for local filesystem paths."""
        if self.protocol != "file":
            return ""
        return pathlib.PurePath(self._aio_path._path).drive

    def __str__(self) -> str:
        """Return the URI string form of the path."""
        return str(self._aio_path)

    def __repr__(self) -> str:
        """Return debug representation."""
        return "%s(%r)" % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        """Return byte representation."""
        return bytes(self._aio_path)

    def __fspath__(self) -> str:
        """Return the os.fspath-compatible string."""
        return fspath(self._aio_path)

    def __hash__(self) -> int:
        """Return the hash value."""
        return hash(self._aio_path)

    def __eq__(self, other_path) -> bool:
        """Return whether two paths are equal."""
        return self._aio_path == _unwrap_sync_value(other_path)

    def __lt__(self, other_path) -> bool:
        """Return whether this path is ordered before another path."""
        return self._aio_path < _unwrap_sync_value(other_path)

    def __le__(self, other_path) -> bool:
        """Return whether this path is ordered before or equal to another path."""
        return self._aio_path <= _unwrap_sync_value(other_path)

    def __gt__(self, other_path) -> bool:
        """Return whether this path is ordered after another path."""
        return self._aio_path > _unwrap_sync_value(other_path)

    def __ge__(self, other_path) -> bool:
        """Return whether this path is ordered after or equal to another path."""
        return self._aio_path >= _unwrap_sync_value(other_path)

    def __truediv__(self, other_path):
        """Join the path with another path segment."""
        result = self._aio_path / _unwrap_sync_value(other_path)
        return _wrap_async_result(result)

    @classmethod
    def from_path(cls, path):
        """Return a new instance from a path without protocol normalization.

        :param path: Raw path string.
        :return: New path instance.
        """
        return cls(path)

    @classmethod
    def from_uri(cls, path):
        """Return a new instance from a URI string.

        :param path: URI string.
        :return: New path instance.
        """
        return cls(path)

    @classmethod
    def register(cls, path_class, override_ok: bool = False):
        """Raise because aiomegfile does not expose megfile-style path registration.

        :param path_class: Requested path class.
        :param override_ok: Whether overriding would be allowed.
        :raises NotImplementedError: Always raised.
        """
        _ = (path_class, override_ok)
        raise NotImplementedError(
            "SmartPath.register() is unsupported by aiomegfile compatibility patch"
        )

    def makedirs(self, exist_ok: bool = False) -> None:
        """Create a directory tree.

        :param exist_ok: Whether to ignore existing directories.
        """
        self.mkdir(parents=True, exist_ok=exist_ok)

    def is_mount(self) -> bool:
        """Return whether the path is a mount point."""
        if self.protocol != "file":
            return False
        return os.path.ismount(self._aio_path._path)

    def is_reserved(self) -> bool:
        """Return whether the path is reserved on the local platform."""
        if self.protocol != "file":
            return False
        reserved = getattr(pathlib.PurePath(self._aio_path._path), "is_reserved", None)
        if callable(reserved):
            return bool(reserved())
        return False

    def is_socket(self) -> bool:
        """Return whether the path points to a socket."""
        return self._local_mode_check(stat_module.S_ISSOCK)

    def is_fifo(self) -> bool:
        """Return whether the path points to a FIFO."""
        return self._local_mode_check(stat_module.S_ISFIFO)

    def is_block_device(self) -> bool:
        """Return whether the path points to a block device."""
        return self._local_mode_check(stat_module.S_ISBLK)

    def is_char_device(self) -> bool:
        """Return whether the path points to a character device."""
        return self._local_mode_check(stat_module.S_ISCHR)

    def access(self, mode: T.Any = Access.READ) -> bool:
        """Test whether the path has the requested access mode.

        :param mode: Access enum value.
        :return: True if access is granted.
        """
        normalized_mode = _normalize_access_mode(mode)
        result = self._aio_path.access(mode=normalized_mode)
        return T.cast(bool, _run_coroutine(result))

    def _local_mode_check(self, checker: T.Callable[[int], bool]) -> bool:
        """Evaluate a stat mode predicate on a local path.

        :param checker: Callable receiving ``st_mode``.
        :return: True if the predicate matches.
        """
        if self.protocol != "file":
            return False
        try:
            return checker(os.lstat(self._aio_path._path).st_mode)
        except OSError:
            return False

    def __getattr__(self, name):
        """Delegate missing attributes to the underlying async SmartPath."""
        attr = getattr(self._aio_path, name)
        if callable(attr):

            def _wrapped(*args, **kwargs):
                """Invoke the delegated path method synchronously."""
                result = attr(
                    *tuple(_unwrap_sync_value(arg) for arg in args),
                    **{key: _unwrap_sync_value(value) for key, value in kwargs.items()},
                )
                if inspect.isawaitable(result):
                    result = _run_coroutine(result)
                return _wrap_async_result(result)

            _wrapped.__name__ = name
            _wrapped.__doc__ = getattr(attr, "__doc__", None)
            return _wrapped
        return _wrap_async_result(attr)


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


def _is_aio_smart_path(value: T.Any) -> bool:
    """Return whether the value is an aiomegfile SmartPath instance.

    :param value: Arbitrary value.
    :return: True when the value is an async SmartPath.
    :rtype: bool
    """
    from aiomegfile.smart_path import SmartPath as AioSmartPath

    return isinstance(value, AioSmartPath)


def _unwrap_sync_value(value: T.Any) -> T.Any:
    """Unwrap sync compatibility path values for async delegation.

    :param value: Arbitrary value.
    :return: Unwrapped value.
    """
    if isinstance(value, _SyncSmartPath):
        return value._aio_path
    if isinstance(value, list):
        return [_unwrap_sync_value(item) for item in value]
    if isinstance(value, tuple) and not hasattr(value, "_fields"):
        return tuple(_unwrap_sync_value(item) for item in value)
    if isinstance(value, set):
        return {_unwrap_sync_value(item) for item in value}
    if isinstance(value, dict):
        return {
            key: _unwrap_sync_value(item_value) for key, item_value in value.items()
        }
    return value


def _normalize_access_mode(mode: T.Any) -> Access:
    """Normalize access enum values coming from megfile or aiomegfile.

    :param mode: Raw access value.
    :return: aiomegfile Access enum value.
    :rtype: Access
    :raises TypeError: If the mode is unsupported.
    """
    if isinstance(mode, Access):
        return mode
    mode_name = getattr(mode, "name", None)
    if mode_name == "READ":
        return Access.READ
    if mode_name == "WRITE":
        return Access.WRITE
    if mode == 1:
        return Access.READ
    if mode == 2:
        return Access.WRITE
    raise TypeError(f"Unsupported mode: {mode}")


def _wrap_sequence_values(value: T.Any) -> T.Any:
    """Wrap container members recursively when needed.

    :param value: Arbitrary container.
    :return: Wrapped container.
    """
    if isinstance(value, list):
        return [_wrap_async_result(item) for item in value]
    if isinstance(value, tuple) and not hasattr(value, "_fields"):
        return tuple(_wrap_async_result(item) for item in value)
    if isinstance(value, set):
        return {_wrap_async_result(item) for item in value}
    if isinstance(value, dict):
        return {key: _wrap_async_result(item) for key, item in value.items()}
    return value


def _wrap_async_result(result, run_awaitable=None):
    """Wrap async constructs into synchronous adapters.

    :param result: Value returned from an async or sync callable.
    :param run_awaitable: Callable that runs an awaitable.
    :return: Synchronous adapter or the original value.
    """
    if isinstance(
        result,
        (
            _SyncAsyncContextManager,
            _SyncAsyncIterator,
            _SyncAsyncProxy,
            _SyncAsyncFile,
            _SyncPathParents,
            _SyncSmartPath,
        ),
    ):
        return result
    if _is_aio_smart_path(result):
        return _SyncSmartPath._from_aio_path(result)
    if hasattr(result, "__class__") and result.__class__.__name__ == "URIPathParents":
        return _SyncPathParents(result)
    wrapped_sequence = _wrap_sequence_values(result)
    if wrapped_sequence is not result:
        return wrapped_sequence
    if _has_async_methods(result):
        return _SyncAsyncProxy(result, run_awaitable=run_awaitable)
    if _is_async_iterator(result):
        return _SyncAsyncIterator(result, run_awaitable=run_awaitable)
    if _is_async_context_manager(result):
        return _SyncAsyncContextManager(result, run_awaitable=run_awaitable)
    return result
