import asyncio
import hashlib
import logging
import os
import posixpath
import random
import stat
import tempfile
import typing as T
import urllib.parse
import weakref
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

import asyncssh

from aiomegfile.config import (
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    SFTP_MAX_RETRY_TIMES,
)
from aiomegfile.errors.sftp import sftp_retry, translate_sftp_error
from aiomegfile.interfaces import (
    Access,
    AioScannableManager,
    AioSeekable,
    AioWritable,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.lib.prefetch_reader.sftp_prefetch_reader import AioSftpPrefetchReader
from aiomegfile.utils.path import PathLike, fspath

logger = logging.getLogger(__name__)
asyncssh.set_log_level(logging.ERROR)
asyncssh.set_sftp_log_level(logging.ERROR)

__all__ = [
    "get_sftp_client",
    "SftpFileSystem",
    "is_sftp",
]

SFTP_DEFAULT_PORT = 22
SFTP_DEFAULT_CONNECT_TIMEOUT = 10.0
SFTP_TYPE_DIRECTORY = 2
SFTP_TYPE_SYMLINK = 3
SFTP_DEFAULT_MAX_UNAUTH_CONNECTIONS = 10
SFTP_DEFAULT_KEEPALIVE_INTERVAL = 15

SFTP_PORT_ENV = "SFTP_PORT"
SFTP_USERNAME_ENV = "SFTP_USERNAME"
SFTP_PASSWORD_ENV = "SFTP_PASSWORD"  # nosec B105
SFTP_PRIVATE_KEY_PATH_ENV = "SFTP_PRIVATE_KEY_PATH"
SFTP_PRIVATE_KEY_PASSPHRASE_ENV = "SFTP_PRIVATE_KEY_PASSPHRASE"  # nosec B105
SFTP_CONNECT_TIMEOUT_ENV = "SFTP_CONNECT_TIMEOUT"
SFTP_KEEPALIVE_INTERVAL_ENV = "SFTP_KEEPALIVE_INTERVAL"
SFTP_MAX_UNAUTH_CONNECTIONS_ENV = "SFTP_MAX_UNAUTH_CONNECTIONS"
SFTP_HOST_KEY_POLICY_ENV = "MEGFILE_SFTP_HOST_KEY_POLICY"

_SFTP_CLIENT_CACHE = weakref.WeakKeyDictionary()
_SFTP_CLIENT_LOCKS = weakref.WeakKeyDictionary()
_SFTP_CONNECT_LOCK_SUFFIX = ".lock"
_SFTP_CONNECT_LOCK_DIR = os.path.join(tempfile.gettempdir(), "aiomegfile-sftp-locks")


@dataclass(frozen=True)
class _SftpEndpoint:
    """Connection endpoint details for an SFTP server.

    :param host: SFTP host.
    :param port: SFTP port.
    :param username: Optional username.
    :param password: Optional password.
    """

    host: str
    port: int = SFTP_DEFAULT_PORT
    username: T.Optional[str] = None
    password: T.Optional[str] = None


def _get_sftp_max_unauth_connections() -> int:
    """Return configured SFTP unauthenticated connection slot count.

    :return: Maximum slot count used by connect lock sharding.
    :rtype: int
    """
    raw_value = os.getenv(
        SFTP_MAX_UNAUTH_CONNECTIONS_ENV,
        str(SFTP_DEFAULT_MAX_UNAUTH_CONNECTIONS),
    )
    try:
        max_unauth_connections = int(raw_value)
    except (TypeError, ValueError):
        return SFTP_DEFAULT_MAX_UNAUTH_CONNECTIONS
    return max(max_unauth_connections, 1)


def _build_sftp_connect_lock_path(endpoint: _SftpEndpoint) -> str:
    """Build file-lock path used for SFTP connection establishment.

    A server-side session quota is usually shared by host and port. To increase
    connection creation parallelism while respecting that quota, lock files are
    sharded by a random slot in ``[1, max_unauth_connections]``.

    :param endpoint: Connection endpoint.
    :return: Absolute lock file path.
    :rtype: str
    """
    max_unauth_connections = _get_sftp_max_unauth_connections()
    slot = random.randint(1, max_unauth_connections)

    lock_key = f"{endpoint.host}:{endpoint.port}:{slot}".encode("utf-8")
    lock_name = hashlib.sha256(lock_key).hexdigest() + _SFTP_CONNECT_LOCK_SUFFIX
    return os.path.join(_SFTP_CONNECT_LOCK_DIR, lock_name)


def _acquire_lock_file(lock_path: str) -> int:
    """Acquire an exclusive lock on the given lock file path.

    :param lock_path: Absolute lock file path.
    :return: Open lock file handle.
    :rtype: int
    """
    import fcntl

    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    lock_file = os.open(
        lock_path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o666,
    )
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
    except Exception:
        os.close(lock_file)
        raise
    return lock_file


def _release_lock_file(lock_file: T.Optional[int]) -> None:
    """Release and close an acquired lock file.

    :param lock_file: Open lock file handle.
    """
    import fcntl

    if lock_file is None:
        return
    with suppress(Exception):
        fcntl.flock(lock_file, fcntl.LOCK_UN)
    with suppress(Exception):
        os.close(lock_file)


@asynccontextmanager
async def _sftp_connect_file_lock(endpoint: _SftpEndpoint):
    """Acquire a per-endpoint file lock while creating an SFTP connection.

    :param endpoint: Connection endpoint.
    :yield: None.
    """
    lock_path = _build_sftp_connect_lock_path(endpoint)
    lock_file = await asyncio.to_thread(_acquire_lock_file, lock_path)
    try:
        yield
    finally:
        await asyncio.to_thread(_release_lock_file, lock_file)


def _freeze_cache_value(value: T.Any) -> T.Hashable:
    """Convert nested cache key values to hashable equivalents.

    :param value: Arbitrary value.
    :return: Hashable value.
    :rtype: T.Hashable
    """
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _freeze_cache_value(item)) for key, item in value.items())
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_freeze_cache_value(item) for item in value)
    return T.cast(T.Hashable, value)


def _build_sftp_cache_key(endpoint: _SftpEndpoint) -> T.Tuple[T.Hashable, ...]:
    """Build cache key for SFTP connection endpoint.

    :param endpoint: Connection endpoint.
    :return: Hashable cache key.
    :rtype: tuple[T.Hashable, ...]
    """
    return (
        endpoint.host,
        endpoint.port,
        endpoint.username,
        endpoint.password,
    )


def _is_client_pair_alive(connection: T.Any, sftp_client: T.Any) -> bool:
    """Return whether cached SFTP pair can still be reused.

    :param connection: SSH connection object.
    :param sftp_client: SFTP client object.
    :return: True when connection appears alive.
    :rtype: bool
    """
    _ = sftp_client
    if connection is None:
        return False
    try:
        return not bool(connection.is_closed())
    except Exception:
        return False


async def _close_client_pair(connection: T.Any, sftp_client: T.Any) -> None:
    """Close cached SFTP pair safely.

    :param connection: SSH connection object.
    :param sftp_client: SFTP client object.
    """
    if sftp_client is not None:
        with suppress(Exception):
            sftp_client.exit()
    if connection is not None:
        with suppress(Exception):
            connection.close()
        with suppress(Exception):
            await connection.wait_closed()


def _get_connect_timeout():
    """Return connect timeout value from environment or default.

    :return: Connect timeout in seconds.
    :rtype: float
    """
    raw_value = os.getenv(
        SFTP_CONNECT_TIMEOUT_ENV,
        str(SFTP_DEFAULT_CONNECT_TIMEOUT),
    )
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return SFTP_DEFAULT_CONNECT_TIMEOUT


def _get_keepalive_interval():
    """Return keepalive interval value from environment or default.

    :return: Keepalive interval in seconds.
    :rtype: float
    """
    raw_value = os.getenv(
        SFTP_KEEPALIVE_INTERVAL_ENV,
        str(SFTP_DEFAULT_KEEPALIVE_INTERVAL),
    )
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return SFTP_DEFAULT_KEEPALIVE_INTERVAL


async def _get_sftp_client(
    endpoint: _SftpEndpoint,
    *,
    max_retries: int = SFTP_MAX_RETRY_TIMES,
) -> T.Tuple[T.Any, T.Any]:
    """Create a new SFTP client pair for the given endpoint.

    :param endpoint: Connection endpoint.
    :param max_retries: Maximum retry attempts for connection setup.
    :return: Tuple of ``(ssh_connection, sftp_client)``.
    :rtype: tuple[Any, Any]
    """
    connect_kwargs: T.Dict[str, T.Any] = {
        "host": endpoint.host,
        "port": endpoint.port,
        "connect_timeout": _get_connect_timeout(),
        "keepalive_interval": _get_keepalive_interval(),
    }
    if endpoint.username is not None:
        connect_kwargs["username"] = endpoint.username
    if endpoint.password is not None:
        connect_kwargs["password"] = endpoint.password

    client_key = os.getenv(SFTP_PRIVATE_KEY_PATH_ENV)
    if client_key:
        connect_kwargs["client_keys"] = client_key

    passphrase = os.getenv(SFTP_PRIVATE_KEY_PASSPHRASE_ENV)
    if passphrase:
        connect_kwargs["passphrase"] = passphrase

    policy = os.getenv(SFTP_HOST_KEY_POLICY_ENV, "reject").lower()
    if policy == "auto":
        connect_kwargs["known_hosts"] = None
    elif policy == "warning":
        connect_kwargs["known_hosts"] = None
        known_hosts_path = os.path.expanduser("~/.ssh/known_hosts")
        if os.path.exists(known_hosts_path):
            # Preload known hosts to trigger warnings for unknown keys
            known_hosts = asyncssh.read_known_hosts(known_hosts_path)
            try:
                r = known_hosts.match(endpoint.host, "", endpoint.port)
                if not r or not r[0]:
                    raise ValueError("Host key not found in known_hosts")
            except Exception:
                logger.warning(
                    "Connecting to unknown SFTP host %s:%d. "
                    "Host key verification is set to 'warning', "
                    "but the host is not in known_hosts.",
                    endpoint.host,
                    endpoint.port,
                )

    @sftp_retry(max_retries=max_retries)
    async def _connect_once() -> T.Tuple[T.Any, T.Any]:
        async with _sftp_connect_file_lock(endpoint):
            connection = await asyncssh.connect(**connect_kwargs)
        try:
            sftp_client = await connection.start_sftp_client()
        except Exception:
            await _close_client_pair(connection, None)
            raise

        def _ensure_closed(conn):
            try:
                if not conn.is_closed():
                    conn.close()
            except Exception as error:
                logger.debug(
                    "Failed to close SSH connection during finalization: %s",
                    error,
                )

        weakref.finalize(sftp_client, _ensure_closed, connection)
        return connection, sftp_client

    return await _connect_once()


async def get_sftp_client(
    host: str,
    port: int = SFTP_DEFAULT_PORT,
    *,
    username: T.Optional[str] = None,
    password: T.Optional[str] = None,
) -> T.Tuple[T.Any, T.Any]:
    """Get a cached SFTP client pair bound to the current event loop.

    :param host: SFTP host.
    :param port: SFTP port.
    :param username: Optional username.
    :param password: Optional password.
    :return: Tuple of ``(ssh_connection, sftp_client)``.
    :rtype: tuple[Any, Any]
    """
    endpoint = _SftpEndpoint(
        host=host,
        port=int(port),
        username=username,
        password=password,
    )

    loop = asyncio.get_running_loop()
    cache = _SFTP_CLIENT_CACHE.setdefault(loop, {})
    lock = _SFTP_CLIENT_LOCKS.setdefault(loop, asyncio.Lock())
    cache_key = _build_sftp_cache_key(endpoint)

    pair = cache.get(cache_key)
    if pair is not None and _is_client_pair_alive(pair[0], pair[1]):
        return pair

    async with lock:
        pair = cache.get(cache_key)
        if pair is not None and _is_client_pair_alive(pair[0], pair[1]):
            return pair
        if pair is not None:
            await _close_client_pair(pair[0], pair[1])
        pair = await _get_sftp_client(endpoint)
        cache[cache_key] = pair
        return pair


def _ensure_text(value: T.Union[bytes, str]) -> str:
    """Return text value from bytes/str input.

    :param value: Input bytes or string.
    :return: Decoded text.
    :rtype: str
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="surrogateescape")
    return value


def _is_absolute_uri_path(path: str) -> bool:
    """Return whether URI path is absolute in megfile SFTP semantics.

    :param path: Path without protocol.
    :return: True when path starts with ``/``.
    :rtype: bool
    """
    return path.startswith("/")


def _to_absolute_uri_path(remote_path: str) -> str:
    """Convert a normalized remote path to absolute URI path format.

    :param remote_path: Absolute remote path like ``/a/b``.
    :return: Absolute path without protocol like ``/a/b``.
    :rtype: str
    """
    normalized = posixpath.normpath(remote_path)
    if normalized == ".":
        normalized = "/"
    return "/" + normalized.lstrip("/")


def _join_uri_path(base_path: str, name: str) -> str:
    """Join a URI path and child name while preserving URI path style.

    :param base_path: Base path without protocol.
    :param name: Child entry name.
    :return: Joined URI path without protocol.
    :rtype: str
    """
    if not base_path:
        return name

    normalized_base = (
        _to_absolute_uri_path(base_path) if base_path.startswith("/") else base_path
    )
    if normalized_base == "/":
        return f"/{name}"
    return f"{normalized_base.rstrip('/')}/{name}"


def _make_stat_result(attrs: T.Any) -> StatResult:
    """Convert SFTP attrs into ``StatResult``.

    :param attrs: asyncssh ``SFTPAttrs`` object.
    :return: Converted ``StatResult``.
    :rtype: StatResult
    """
    type_value = getattr(attrs, "type", None)
    permissions = getattr(attrs, "permissions", None)

    is_dir = False
    is_lnk = False

    if type_value == SFTP_TYPE_DIRECTORY:
        is_dir = True
    elif type_value == SFTP_TYPE_SYMLINK:
        is_lnk = True

    if permissions is not None:
        if stat.S_ISDIR(permissions):
            is_dir = True
        if stat.S_ISLNK(permissions):
            is_lnk = True

    size = int(getattr(attrs, "size", 0) or 0)
    mtime = float(getattr(attrs, "mtime", 0.0) or 0.0)
    ctime = float(getattr(attrs, "ctime", mtime) or mtime)

    return StatResult(
        st_size=size,
        st_ctime=ctime,
        st_mtime=mtime,
        isdir=is_dir,
        islnk=is_lnk,
        extra=attrs,
    )


def is_sftp(path: PathLike) -> bool:
    """Return whether the given path is an SFTP URI.

    :param path: Path to be tested.
    :return: True if path is an SFTP URI.
    :rtype: bool
    """
    parsed = urllib.parse.urlsplit(fspath(path))
    return parsed.scheme == "sftp" and bool(parsed.netloc)


class AioSftpWritableFile(AioWritable[T.AnyStr], AioSeekable[T.AnyStr]):
    """Async writable SFTP file wrapper.

    :param filesystem: Owning SFTP filesystem.
    :param path: Path without protocol.
    :param mode: File mode.
    :param encoding: Text encoding in text mode.
    :param errors: Text error handling mode.
    """

    def __init__(
        self,
        filesystem: "SftpFileSystem",
        path: str,
        mode: str,
        encoding: T.Optional[str],
        errors: T.Optional[str],
    ) -> None:
        """Initialize writable SFTP file adapter.

        :param filesystem: Owning filesystem instance.
        :param path: Path without protocol.
        :param mode: File mode.
        :param encoding: Text encoding in text mode.
        :param errors: Text error handling mode.
        """
        self._filesystem = filesystem
        self._path = path
        self._mode = mode
        self._encoding = encoding or "utf-8"
        self._errors = errors or "strict"

        self._connection = None
        self._client = None
        self._file = None

    @property
    def name(self) -> str:
        """Return full URI of the opened file.

        :return: Full URI.
        :rtype: str
        """
        return self._filesystem.build_uri(self._path)

    @property
    def mode(self) -> str:
        """Return open mode.

        :return: File mode.
        :rtype: str
        """
        return self._mode

    async def __aenter__(self):
        """Enter async context and open remote file.

        :return: Opened writable file.
        :rtype: AioSftpWritableFile
        """
        self._connection, self._client = await self._filesystem._open_client()

        try:
            remote_path = await self._filesystem._resolve_remote_path(
                self._client,
                self._path,
            )
            parent_path = posixpath.dirname(remote_path)
            if parent_path and parent_path != "/":
                await self._client.makedirs(parent_path, exist_ok=True)

            self._file = await self._filesystem._open_remote_file(
                self._client,
                remote_path,
                self._mode,
                encoding=(None if "b" in self._mode else self._encoding),
                errors=self._errors,
            )
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error

        return self

    def _ensure_open(self) -> None:
        """Validate the remote file is still open.

        :raises IOError: If file has been closed.
        """
        if self.closed:
            raise IOError(f"file already closed: {self.name!r}")
        if self._file is None:
            raise IOError(f"file not open: {self.name!r}")

    async def write(self, data: T.AnyStr) -> int:
        """Write data to remote file.

        :param data: Data to write.
        :return: Number of written bytes/chars.
        :rtype: int
        """
        self._ensure_open()

        if "b" in self._mode and not isinstance(data, (bytes, bytearray)):
            raise TypeError("a bytes-like object is required, not 'str'")
        if "b" not in self._mode and not isinstance(data, str):
            raise TypeError("write() argument must be str in text mode")

        try:
            written = await self._file.write(data)
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error

        if written is None:
            return len(data)
        return int(written)

    async def flush(self) -> None:
        """Flush pending writes.

        asyncssh SFTP file does not expose explicit flush; this is a no-op.
        """
        self._ensure_open()
        return None

    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Seek stream position.

        :param offset: Offset value.
        :param whence: Seek origin.
        :return: New absolute position.
        :rtype: int
        """
        self._ensure_open()
        try:
            position = await self._file.seek(offset, whence)
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error
        return int(position)

    async def tell(self) -> int:
        """Return current stream position.

        :return: Stream position.
        :rtype: int
        """
        self._ensure_open()
        try:
            position = await self._file.tell()
        except Exception as error:
            translated = translate_sftp_error(error, self.name)
            raise translated from error
        return int(position)

    async def close(self) -> None:
        """Close file and its underlying SFTP session resources."""
        if self._file is not None:
            with suppress(Exception):
                await self._file.close()
            self._file = None

        self._connection = None
        self._client = None


class SftpFileSystem(BaseFileSystem):
    """Filesystem adapter for ``sftp://`` URIs using asyncssh.

    URI formats:

    - Absolute remote path: ``sftp://[username[:password]@]hostname[:port]//file_path``.
    - Home-relative path: ``sftp://[username[:password]@]hostname[:port]/path/to/file``.
      Path part does not start with ``//`` after parsing.
    """

    protocol = "sftp"

    def __init__(
        self,
        host: str,
        port: T.Optional[int] = None,
        *,
        username: T.Optional[str] = None,
        password: T.Optional[str] = None,
        show_port_in_uri: T.Optional[bool] = None,
        show_username_in_uri: T.Optional[bool] = None,
        show_password_in_uri: T.Optional[bool] = None,
    ) -> None:
        """Initialize SFTP filesystem endpoint.

        :param host: SFTP host.
        :param port: SFTP port.
        :param username: Optional username.
        :param password: Optional password.
        :param show_port_in_uri: Whether to render port in ``build_uri``.
        :param show_username_in_uri: Whether to render username in ``build_uri``.
        :param show_password_in_uri: Whether to render password in ``build_uri``.
        """
        self._show_port_in_uri = show_port_in_uri
        if show_port_in_uri is None:
            self._show_port_in_uri = port is not None
        self._show_username_in_uri = show_username_in_uri
        if show_username_in_uri is None:
            self._show_username_in_uri = username is not None
        self._show_password_in_uri = show_password_in_uri
        if show_password_in_uri is None:
            # Never show password in URI by default for security reasons
            self._show_password_in_uri = False

        self._endpoint = _SftpEndpoint(
            host=host,
            port=port or SFTP_DEFAULT_PORT,
            username=username,
            password=password,
        )

    async def _open_client(self) -> T.Tuple[T.Any, T.Any]:
        """Get a cached SSH and SFTP client pair.

        :return: Tuple of ``(ssh_connection, sftp_client)``.
        :rtype: tuple[Any, Any]
        """
        return await get_sftp_client(
            host=self._endpoint.host,
            port=self._endpoint.port,
            username=self._endpoint.username,
            password=self._endpoint.password,
        )

    async def _open_remote_file(
        self,
        sftp_client: T.Any,
        remote_path: str,
        mode: str,
        *,
        encoding: T.Optional[str],
        errors: str,
    ) -> T.Any:
        """Open a remote file and return asyncssh file object.

        :param sftp_client: SFTP client object.
        :param remote_path: Absolute remote path.
        :param mode: File mode.
        :param encoding: Optional text encoding.
        :param errors: Text error handling.
        :return: Opened remote file object.
        :rtype: Any
        """
        return await sftp_client.open(
            remote_path,
            mode,
            encoding=encoding,
            errors=errors,
        )

    async def _resolve_remote_path(self, sftp_client: T.Any, path: str) -> str:
        """Resolve URI path into absolute remote path.

        ``/`` prefix is treated as absolute remote path. Path without leading
        slash is treated as relative to remote home directory.

        :param sftp_client: SFTP client object.
        :param path: URI path without protocol.
        :return: Absolute remote path.
        :rtype: str
        """
        if not path:
            path = "."

        if _is_absolute_uri_path(path):
            normalized = posixpath.normpath("/" + path.lstrip("/"))
            return "/" if normalized == "." else normalized

        home_dir = _ensure_text(await sftp_client.realpath("."))
        relative_path = path.lstrip("/")
        if relative_path in ("", "."):
            return home_dir
        return posixpath.normpath(posixpath.join(home_dir, relative_path))

    async def _stat_attrs(
        self,
        sftp_client: T.Any,
        remote_path: str,
        *,
        followlinks: bool,
    ) -> T.Any:
        """Return SFTP attrs for a remote path.

        :param sftp_client: SFTP client object.
        :param remote_path: Absolute remote path.
        :param followlinks: Whether to follow symbolic links.
        :return: SFTP attrs object.
        :rtype: Any
        """
        if followlinks:
            return await sftp_client.stat(remote_path, follow_symlinks=True)
        return await sftp_client.lstat(remote_path)

    async def _remove_remote_path(
        self,
        sftp_client: T.Any,
        remote_path: str,
        *,
        missing_ok: bool,
    ) -> None:
        """Remove remote file or directory recursively.

        :param sftp_client: SFTP client object.
        :param remote_path: Absolute remote path.
        :param missing_ok: Ignore missing target when True.
        """
        try:
            attrs = await sftp_client.lstat(remote_path)
        except Exception as error:
            translated = translate_sftp_error(
                error,
                self.build_uri(_to_absolute_uri_path(remote_path)),
            )
            if missing_ok and isinstance(translated, FileNotFoundError):
                return
            raise translated from error

        stat_result = _make_stat_result(attrs)
        if stat_result.is_dir() and not stat_result.is_symlink():
            await sftp_client.rmtree(remote_path)
        else:
            await sftp_client.remove(remote_path)

    def _build_progress_handler(
        self,
        callback: T.Optional[T.Callable[[int], None]],
    ) -> T.Optional[T.Callable[[bytes, bytes, int, int], None]]:
        """Build asyncssh-style progress handler from byte-delta callback.

        :param callback: Optional callback receiving copied byte delta.
        :return: Progress handler compatible with asyncssh APIs.
        :rtype: T.Optional[T.Callable[[bytes, bytes, int, int], None]]
        """
        if callback is None:
            return None

        previous_values: T.Dict[T.Tuple[str, str], int] = {}

        def _progress_handler(
            src_path: bytes,
            dst_path: bytes,
            copied: int,
            total: int,
        ) -> None:
            _ = total
            key = (_ensure_text(src_path), _ensure_text(dst_path))
            previous = previous_values.get(key, 0)
            delta = copied - previous
            if delta > 0:
                callback(delta)  # pyre-ignore[29]
            previous_values[key] = copied

        return _progress_handler

    @asynccontextmanager
    async def _session(self):
        """Yield an opened SFTP client and ensure cleanup."""
        _, sftp_client = await self._open_client()
        yield sftp_client

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True when path points to a directory.

        :param path: Path without protocol.
        :param followlinks: Whether to follow symbolic links.
        :return: True for directories, otherwise False.
        :rtype: bool
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                attrs = await self._stat_attrs(
                    sftp_client,
                    remote_path,
                    followlinks=followlinks,
                )
                return _make_stat_result(attrs).is_dir()
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, FileNotFoundError):
                return False
            raise translated from error

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True when path points to a regular file.

        :param path: Path without protocol.
        :param followlinks: Whether to follow symbolic links.
        :return: True for files, otherwise False.
        :rtype: bool
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                attrs = await self._stat_attrs(
                    sftp_client,
                    remote_path,
                    followlinks=followlinks,
                )
                return _make_stat_result(attrs).is_file()
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, FileNotFoundError):
                return False
            raise translated from error

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether path exists.

        :param path: Path without protocol.
        :param followlinks: Whether to follow symbolic links.
        :return: True if path exists.
        :rtype: bool
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                await self._stat_attrs(
                    sftp_client,
                    remote_path,
                    followlinks=followlinks,
                )
                return True
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, FileNotFoundError):
                return False
            raise translated from error

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Return stat information for path.

        :param path: Path without protocol.
        :param followlinks: Whether to follow symbolic links.
        :return: StatResult object.
        :rtype: StatResult
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                attrs = await self._stat_attrs(
                    sftp_client,
                    remote_path,
                    followlinks=followlinks,
                )
                return _make_stat_result(attrs)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove file or directory recursively.

        :param path: Path without protocol.
        :param missing_ok: Ignore missing target when True.
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                await self._remove_remote_path(
                    sftp_client,
                    remote_path,
                    missing_ok=missing_ok,
                )
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if missing_ok and isinstance(translated, FileNotFoundError):
                return
            raise translated from error

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create directory.

        :param path: Path without protocol.
        :param mode: Directory permission bits.
        :param parents: Create missing parent directories.
        :param exist_ok: Ignore existing target directory.
        """
        uri = self.build_uri(path)
        attrs = asyncssh.sftp.SFTPAttrs(permissions=mode)

        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                if parents:
                    await sftp_client.makedirs(
                        remote_path,
                        attrs=attrs,
                        exist_ok=exist_ok,
                    )
                else:
                    await sftp_client.mkdir(remote_path, attrs=attrs)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if exist_ok and isinstance(translated, FileExistsError):
                return
            raise translated from error

    async def absolute(self, path: str) -> str:
        """Return absolute URI path without protocol.

        :param path: Path without protocol.
        :return: Absolute path in ``/`` form.
        :rtype: str
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                absolute_remote = _ensure_text(await sftp_client.realpath(remote_path))
                return _to_absolute_uri_path(absolute_remote)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def is_absolute(self, path: str) -> bool:
        """Return whether URI path is absolute in SFTP semantics.

        :param path: Path without protocol.
        :return: True when path starts with ``/``.
        :rtype: bool
        """
        return _is_absolute_uri_path(path)

    async def samefile(self, path: str, other_path: str) -> bool:
        """Return whether two paths reference the same remote file.

        :param path: First path without protocol.
        :param other_path: Second path without protocol.
        :return: True if they resolve to the same real path.
        :rtype: bool
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                left = await self._resolve_remote_path(sftp_client, path)
                right = await self._resolve_remote_path(sftp_client, other_path)
                left_real = _ensure_text(await sftp_client.realpath(left))
                right_real = _ensure_text(await sftp_client.realpath(right))
                return posixpath.normpath(left_real) == posixpath.normpath(right_real)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, FileNotFoundError):
                return False
            raise translated from error

    async def readlink(self, path: str) -> str:
        """Return symbolic link target for path.

        :param path: Symbolic link path without protocol.
        :return: Target path without protocol.
        :rtype: str
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                target = _ensure_text(await sftp_client.readlink(remote_path))
                if target.startswith("/"):
                    return _to_absolute_uri_path(target)
                return target
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def is_symlink(self, path: str) -> bool:
        """Return whether path points to a symbolic link.

        :param path: Path without protocol.
        :return: True when path is a symbolic link.
        :rtype: bool
        """
        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                attrs = await sftp_client.lstat(remote_path)
                return _make_stat_result(attrs).is_symlink()
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, FileNotFoundError):
                return False
            raise translated from error

    async def symlink(self, src_path: str, dst_path: str) -> None:
        """Create symbolic link.

        :param src_path: Source path without protocol.
        :param dst_path: Destination symlink path without protocol.
        """
        uri = self.build_uri(dst_path)
        try:
            async with self._session() as sftp_client:
                src_remote = await self._resolve_remote_path(sftp_client, src_path)
                dst_remote = await self._resolve_remote_path(sftp_client, dst_path)
                parent_path = posixpath.dirname(dst_remote)
                if parent_path and parent_path != "/":
                    await sftp_client.makedirs(parent_path, exist_ok=True)
                await sftp_client.symlink(src_remote, dst_remote)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> str:
        """Copy a single file on the same SFTP endpoint.

        :param src_path: Source path without protocol.
        :param dst_path: Destination path without protocol.
        :param callback: Optional copy progress callback.
        :return: Destination path without protocol.
        :rtype: str
        """
        if src_path == dst_path:
            raise OSError(
                f"src and dst are the same file: {self.build_uri(src_path)!r}"
            )

        uri = self.build_uri(src_path)
        try:
            async with self._session() as sftp_client:
                src_remote = await self._resolve_remote_path(sftp_client, src_path)
                dst_remote = await self._resolve_remote_path(sftp_client, dst_path)

                src_attrs = await sftp_client.lstat(src_remote)
                src_stat = _make_stat_result(src_attrs)
                if src_stat.is_dir() and not src_stat.is_symlink():
                    raise IsADirectoryError(
                        f"Is a directory: {self.build_uri(src_path)!r}"
                    )

                parent_path = posixpath.dirname(dst_remote)
                if parent_path and parent_path != "/":
                    await sftp_client.makedirs(parent_path, exist_ok=True)

                if await sftp_client.exists(dst_remote):
                    await self._remove_remote_path(
                        sftp_client,
                        dst_remote,
                        missing_ok=True,
                    )

                await sftp_client.copy(
                    src_remote,
                    dst_remote,
                    preserve=False,
                    recurse=False,
                    follow_symlinks=False,
                    sparse=True,
                    progress_handler=self._build_progress_handler(callback),
                    remote_only=True,
                )

                return dst_path
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def upload(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Upload a single local file to this SFTP endpoint.

        :param src_path: Local source path.
        :param dst_path: Remote destination path without protocol.
        :param callback: Optional upload progress callback.
        """
        uri = self.build_uri(dst_path)
        if os.path.isdir(src_path):
            raise IsADirectoryError(f"Is a directory: {src_path!r}")

        try:
            async with self._session() as sftp_client:
                dst_remote = await self._resolve_remote_path(sftp_client, dst_path)
                parent_path = posixpath.dirname(dst_remote)
                if parent_path and parent_path != "/":
                    await sftp_client.makedirs(parent_path, exist_ok=True)

                await sftp_client.put(
                    src_path,
                    dst_remote,
                    preserve=False,
                    recurse=False,
                    follow_symlinks=False,
                    sparse=True,
                    progress_handler=self._build_progress_handler(callback),
                )
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def download(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Download a single remote file to local filesystem.

        :param src_path: Remote source path without protocol.
        :param dst_path: Local destination path.
        :param callback: Optional download progress callback.
        """
        uri = self.build_uri(src_path)
        try:
            async with self._session() as sftp_client:
                src_remote = await self._resolve_remote_path(sftp_client, src_path)
                src_attrs = await sftp_client.lstat(src_remote)
                src_stat = _make_stat_result(src_attrs)
                if src_stat.is_dir() and not src_stat.is_symlink():
                    raise IsADirectoryError(f"Is a directory: {uri!r}")

                local_parent = os.path.dirname(dst_path)
                if local_parent:
                    os.makedirs(local_parent, exist_ok=True)

                await sftp_client.get(
                    src_remote,
                    dst_path,
                    preserve=False,
                    recurse=False,
                    follow_symlinks=False,
                    sparse=True,
                    progress_handler=self._build_progress_handler(callback),
                )
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """Move file or directory to a destination path.

        :param src_path: Source path without protocol.
        :param dst_path: Destination path without protocol.
        :param overwrite: Overwrite destination when it exists.
        :return: Destination path without protocol.
        :rtype: str
        """
        uri = self.build_uri(src_path)
        try:
            async with self._session() as sftp_client:
                src_remote = await self._resolve_remote_path(sftp_client, src_path)
                dst_remote = await self._resolve_remote_path(sftp_client, dst_path)

                if not overwrite and await sftp_client.exists(dst_remote):
                    raise FileExistsError(f"File exists: {self.build_uri(dst_path)!r}")

                if overwrite and await sftp_client.exists(dst_remote):
                    await self._remove_remote_path(
                        sftp_client,
                        dst_remote,
                        missing_ok=True,
                    )

                parent_path = posixpath.dirname(dst_remote)
                if parent_path and parent_path != "/":
                    await sftp_client.makedirs(parent_path, exist_ok=True)

                await sftp_client.rename(src_remote, dst_remote)
                return dst_path
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            raise translated from error

    async def access(self, path: str, mode: Access = Access.READ) -> bool:
        """Check read/write access heuristically for a path.

        :param path: Path without protocol.
        :param mode: Access mode enum.
        :return: Whether access is likely available.
        :rtype: bool
        """
        if mode not in (Access.READ, Access.WRITE):
            raise TypeError(f"Unsupported mode: {mode}")

        uri = self.build_uri(path)
        try:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                if mode == Access.READ:
                    return await sftp_client.exists(remote_path)

                if await sftp_client.exists(remote_path):
                    attrs = await sftp_client.lstat(remote_path)
                    stat_result = _make_stat_result(attrs)
                    return not stat_result.is_dir()

                parent_path = posixpath.dirname(remote_path)
                if not parent_path:
                    parent_path = "/"
                return await sftp_client.exists(parent_path)
        except Exception as error:
            translated = translate_sftp_error(error, uri)
            if isinstance(translated, (FileNotFoundError, PermissionError)):
                return False
            raise translated from error

    def open(
        self,
        path: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        **kwargs: T.Any,
    ) -> T.AsyncContextManager:
        """Open path as async reader or writer.

        Read mode uses ``AioSftpPrefetchReader``.

        :param path: Path without protocol.
        :param mode: File open mode.
        :param buffering: Unused compatibility argument.
        :param encoding: Text encoding in text mode.
        :param errors: Text error handling.
        :param newline: Currently unused compatibility argument.
        :param kwargs: Extra prefetch parameters.
        :return: Async context manager for opened file.
        :rtype: T.AsyncContextManager
        """
        _ = buffering, newline

        normalized_mode = mode.replace("t", "")
        if "+" in normalized_mode:
            raise ValueError(f"unsupported mode: {mode!r}")
        if normalized_mode not in {"r", "rb", "w", "wb", "a", "ab", "x", "xb"}:
            raise ValueError(f"unacceptable mode: {mode!r}")

        if normalized_mode in {"r", "rb"}:
            block_size = kwargs.get("block_size")
            max_buffer_size = kwargs.get("max_buffer_size")
            block_forward = kwargs.get("block_forward")
            max_retries = kwargs.get("max_retries")

            return AioSftpPrefetchReader(
                path,
                filesystem=self,
                mode=normalized_mode,
                encoding=encoding,
                errors=errors,
                newline=newline,
                block_size=(
                    int(block_size) if block_size is not None else READER_BLOCK_SIZE
                ),
                max_buffer_size=(
                    int(max_buffer_size)
                    if max_buffer_size is not None
                    else READER_MAX_BUFFER_SIZE
                ),
                block_forward=(
                    int(block_forward) if block_forward is not None else None
                ),
                max_retries=(
                    int(max_retries)
                    if max_retries is not None
                    else SFTP_MAX_RETRY_TIMES
                ),
            )

        return AioSftpWritableFile(
            self,
            path,
            normalized_mode,
            encoding=encoding,
            errors=errors,
        )

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return async iterator over direct children of a directory.

        :param path: Directory path without protocol.
        :return: Async context manager yielding ``FileEntry`` values.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        uri = self.build_uri(path)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            async with self._session() as sftp_client:
                remote_path = await self._resolve_remote_path(sftp_client, path)
                attrs = await sftp_client.stat(remote_path, follow_symlinks=True)
                if not _make_stat_result(attrs).is_dir():
                    raise NotADirectoryError(f"Not a directory: {uri!r}")

                async for entry in sftp_client.scandir(remote_path):
                    name = _ensure_text(entry.filename)
                    if name in (".", ".."):
                        continue
                    entry_path = _join_uri_path(path, name)
                    yield FileEntry(
                        name=name,
                        path=entry_path,
                        stat=_make_stat_result(entry.attrs),
                    )

        iterator = aiterator()

        async def aexit(exc_type, exc_value, traceback) -> None:
            with suppress(Exception):
                await iterator.aclose()

        return AioScannableManager(iterator, aexit)

    def scanfile(
        self,
        path: str,
        sort: bool = False,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return async iterator over files recursively.

        :param path: Root path without protocol.
        :param sort: Compatibility flag for protocol-aligned scanfile APIs.
        :return: Async context manager yielding file ``FileEntry`` values.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        _ = sort
        uri = self.build_uri(path)

        async def _iter_files(
            sftp_client: T.Any,
            current_path: str,
        ) -> T.AsyncIterator[FileEntry]:
            remote_path = await self._resolve_remote_path(sftp_client, current_path)
            attrs = await sftp_client.lstat(remote_path)
            stat_result = _make_stat_result(attrs)

            if not stat_result.is_dir() or stat_result.is_symlink():
                name = posixpath.basename(current_path.rstrip("/"))
                if not name:
                    name = "/"
                yield FileEntry(name=name, path=current_path, stat=stat_result)
                return

            children: T.List[T.Tuple[str, str, StatResult]] = []
            async for entry in sftp_client.scandir(remote_path):
                name = _ensure_text(entry.filename)
                if name in (".", ".."):
                    continue
                child_path = _join_uri_path(current_path, name)
                children.append((name, child_path, _make_stat_result(entry.attrs)))

            children.sort(key=lambda item: item[0])
            for name, child_path, child_stat in children:
                if child_stat.is_dir() and not child_stat.is_symlink():
                    async for nested_entry in _iter_files(sftp_client, child_path):
                        yield nested_entry
                else:
                    yield FileEntry(name=name, path=child_path, stat=child_stat)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            async with self._session() as sftp_client:
                try:
                    async for file_entry in _iter_files(sftp_client, path):
                        yield file_entry
                except Exception as error:
                    translated = translate_sftp_error(error, uri)
                    raise translated from error

        iterator = aiterator()

        async def aexit(exc_type, exc_value, traceback) -> None:
            with suppress(Exception):
                await iterator.aclose()

        return AioScannableManager(iterator, aexit)

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """Return whether another filesystem points to same SFTP endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True when two filesystems share the same endpoint settings.
        :rtype: bool
        """
        if not isinstance(other_filesystem, SftpFileSystem):
            return False
        return _build_sftp_cache_key(self._endpoint) == _build_sftp_cache_key(
            other_filesystem._endpoint
        )

    def parse_uri(self, uri: str) -> str:
        """Parse URI into path part without protocol.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        parsed = urllib.parse.urlsplit(uri)
        if parsed.scheme == "":
            return uri
        path = parsed.path or "/"
        if path.startswith("//"):
            return _to_absolute_uri_path(path)
        if path.startswith("/"):
            return path.lstrip("/")
        return path

    def build_uri(self, path: str) -> str:
        """Build URI from path part.

        :param path: Path without protocol.
        :return: Full SFTP URI.
        :rtype: str
        """
        if not path:
            uri_path = "/"
        else:
            normalized_path = (
                _to_absolute_uri_path(path) if path.startswith("/") else path
            )
            if normalized_path.startswith("/"):
                uri_path = (
                    "//"
                    if normalized_path == "/"
                    else f"//{normalized_path.lstrip('/')}"
                )
            else:
                uri_path = f"/{normalized_path}"

        host = self._endpoint.host

        netloc = host
        if self._show_port_in_uri and self._endpoint.port:
            netloc = f"{netloc}:{self._endpoint.port}"

        if self._show_username_in_uri and self._endpoint.username:
            username = urllib.parse.quote(self._endpoint.username, safe="")
            if self._show_password_in_uri and self._endpoint.password is not None:
                password = urllib.parse.quote(self._endpoint.password, safe="")
                userinfo = f"{username}:{password}"
            else:
                userinfo = username
            netloc = f"{userinfo}@{netloc}"

        return urllib.parse.urlunsplit((self.protocol, netloc, uri_path, "", ""))

    @classmethod
    def from_uri(cls, uri: str) -> "SftpFileSystem":
        """Create filesystem instance from URI.

        :param uri: URI string.
        :return: SftpFileSystem instance.
        :rtype: SftpFileSystem
        """
        parsed = urllib.parse.urlsplit(uri)
        if parsed.scheme != cls.protocol:
            raise ValueError(f"unsupported scheme for sftp filesystem: {uri!r}")
        if not parsed.hostname:
            raise ValueError(f"missing host in sftp uri: {uri!r}")

        username = (
            urllib.parse.unquote(parsed.username)
            if parsed.username is not None
            else os.getenv(SFTP_USERNAME_ENV)
        )
        password = (
            urllib.parse.unquote(parsed.password)
            if parsed.password is not None
            else os.getenv(SFTP_PASSWORD_ENV)
        )

        env_port = os.getenv(SFTP_PORT_ENV)
        if parsed.port is not None:
            port = parsed.port
        elif env_port is not None:
            port = int(env_port)
        else:
            port = SFTP_DEFAULT_PORT

        return cls(
            host=parsed.hostname,  # pyre-ignore[6]
            port=port,
            username=username,
            password=password,
            show_port_in_uri=parsed.port is not None,
            show_username_in_uri=parsed.username is not None,
            show_password_in_uri=parsed.password is not None,
        )
