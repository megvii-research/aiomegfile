"""Filesystem adapter for WebDAV resources backed by ``aiodav``."""

from __future__ import annotations

import asyncio
import inspect
import io
import os
import posixpath
import shlex
import subprocess
import threading
import typing as T
import urllib.parse
import weakref
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache

import aiofiles

from aiomegfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from aiomegfile.errors.webdav import (
    WEBDAV_INSTALL_HINT,
    translate_webdav_error,
    webdav_retry,
)
from aiomegfile.interfaces import (
    Access,
    AioScannableManager,
    AioSeekable,
    AioWritable,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.lib.prefetch_reader.webdav_prefetch_reader import (
    AioWebdavPrefetchReader,
)
from aiomegfile.utils.parse import parse_boolean
from aiomegfile.utils.path import PathLike, fspath

if T.TYPE_CHECKING:
    from aiodav.client import Client as AiodavClient
else:
    AiodavClient = T.Any

__all__ = [
    "WEBDAV_DEFAULT_TIMEOUT",
    "WEBDAV_INSECURE_ENV",
    "WEBDAV_INSTALL_HINT",
    "WEBDAV_PASSWORD_ENV",
    "WEBDAV_TIMEOUT_ENV",
    "WEBDAV_TOKEN_COMMAND_ENV",
    "WEBDAV_TOKEN_ENV",
    "WEBDAV_USERNAME_ENV",
    "clear_webdav_client_cache",
    "get_webdav_client",
    "import_aiodav_client_class",
    "load_webdav_insecure",
    "load_webdav_timeout",
    "load_webdav_token",
    "load_webdav_token_from_command",
    "WebdavFileSystem",
    "WebdavsFileSystem",
    "is_webdav",
]

WEBDAV_DEFAULT_TIMEOUT = 30.0
WEBDAV_USERNAME_ENV = "WEBDAV_USERNAME"
WEBDAV_PASSWORD_ENV = "WEBDAV_PASSWORD"  # nosec B105
WEBDAV_TOKEN_ENV = "WEBDAV_TOKEN"
WEBDAV_TOKEN_COMMAND_ENV = "WEBDAV_TOKEN_COMMAND"
WEBDAV_TIMEOUT_ENV = "WEBDAV_TIMEOUT"
WEBDAV_INSECURE_ENV = "WEBDAV_INSECURE"

_WEBDAV_CLIENT_CACHE: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[tuple[T.Hashable, ...], T.Any],
] = weakref.WeakKeyDictionary()
_WEBDAV_CLIENT_FALLBACK_CACHE: dict[tuple[T.Hashable, ...], T.Any] = {}
_WEBDAV_CLIENT_CACHE_LOCK = threading.Lock()


def _import_aiodav_module(module_name: str):
    """Import a WebDAV optional module with install hints.

    :param module_name: Target module name under ``aiodav`` package.
    :return: Imported module object.
    :rtype: module
    :raises ImportError: If optional dependency is unavailable.
    """
    try:
        return __import__(module_name, fromlist=["*"])
    except ImportError as error:
        raise ImportError(
            inspect.cleandoc(
                """
                Failed to import aiodav, the following steps show you how to install it:

                    pip3 install 'aiomegfile[webdav]' --user
                """
            )
        ) from error


@lru_cache(maxsize=1)
def import_aiodav_client_class():
    """Import ``aiodav.client.Client`` with install hint.

    :return: Imported ``aiodav.client.Client`` class.
    :rtype: type
    :raises ImportError: If ``aiodav`` package is not installed.
    """
    module = _import_aiodav_module("aiodav.client")
    client_class = getattr(module, "Client", None)
    if not isinstance(client_class, type):
        raise ImportError("Unable to import aiodav client class")
    return client_class


def _normalize_optional_text(value: T.Optional[str]) -> T.Optional[str]:
    """Normalize optional string by trimming surrounding spaces.

    :param value: Optional input string.
    :return: Stripped string or ``None`` for empty values.
    :rtype: T.Optional[str]
    """
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def load_webdav_timeout(timeout: T.Optional[float] = None) -> float:
    """Return WebDAV timeout from argument/environment with fallback.

    :param timeout: Explicit timeout value.
    :return: Timeout in seconds.
    :rtype: float
    """
    raw_value: T.Union[str, float] = (
        timeout if timeout is not None else os.getenv(WEBDAV_TIMEOUT_ENV, "")
    )
    if raw_value == "":
        return WEBDAV_DEFAULT_TIMEOUT
    try:
        parsed_timeout = float(raw_value)
    except (TypeError, ValueError):
        return WEBDAV_DEFAULT_TIMEOUT
    return parsed_timeout if parsed_timeout > 0 else WEBDAV_DEFAULT_TIMEOUT


def load_webdav_insecure(insecure: T.Optional[bool] = None) -> bool:
    """Return WebDAV insecure mode from argument/environment.

    :param insecure: Explicit insecure flag.
    :return: True when SSL verification is disabled.
    :rtype: bool
    """
    if insecure is not None:
        return bool(insecure)
    return parse_boolean(os.getenv(WEBDAV_INSECURE_ENV), default=False)


def load_webdav_token_from_command(token_command: str) -> str:
    """Run command and return stripped WebDAV token.

    :param token_command: Command text to fetch token.
    :return: Retrieved token.
    :rtype: str
    :raises RuntimeError: If command execution fails or returns empty token.
    """
    normalized_command = _normalize_optional_text(token_command)
    if normalized_command is None:
        raise RuntimeError("WEBDAV token command is empty")

    commands = shlex.split(normalized_command)
    if not commands:
        raise RuntimeError("WEBDAV token command is empty")

    try:
        output = subprocess.check_output(
            commands,
            stderr=subprocess.STDOUT,
        )  # nosec B603,B607
    except FileNotFoundError as error:
        raise RuntimeError(
            f"Failed to execute WEBDAV token command: {normalized_command!r}"
        ) from error
    except subprocess.CalledProcessError as error:
        error_output = (
            error.output.decode("utf-8", errors="replace").strip()
            if error.output
            else ""
        )
        details = f", output: {error_output}" if error_output else ""
        raise RuntimeError(
            "WEBDAV token command failed with exit code "
            f"{error.returncode}: {normalized_command!r}{details}"
        ) from error

    token = output.decode("utf-8", errors="replace").strip()
    if not token:
        raise RuntimeError(
            f"WEBDAV token command returned empty token: {normalized_command!r}"
        )
    return token


def load_webdav_token(
    *,
    token: T.Optional[str] = None,
    token_command: T.Optional[str] = None,
) -> T.Optional[str]:
    """Resolve WebDAV token with ``token_command`` priority.

    :param token: Explicit token value.
    :param token_command: Explicit token command value.
    :return: Resolved token text or ``None``.
    :rtype: T.Optional[str]
    """
    resolved_token_command = _normalize_optional_text(token_command)
    if resolved_token_command is None:
        resolved_token_command = _normalize_optional_text(
            os.getenv(WEBDAV_TOKEN_COMMAND_ENV)
        )
    if resolved_token_command is not None:
        return load_webdav_token_from_command(resolved_token_command)

    resolved_token = _normalize_optional_text(token)
    if resolved_token is None:
        resolved_token = _normalize_optional_text(os.getenv(WEBDAV_TOKEN_ENV))
    return resolved_token


def _is_webdav_client_available(client: T.Any) -> bool:
    """Return whether cached WebDAV client can be reused.

    :param client: Cached client instance.
    :return: True when underlying session is still open.
    :rtype: bool
    """
    if client is None:
        return False
    session = getattr(client, "session", None)
    if session is None:
        return True
    if hasattr(session, "closed"):
        with suppress(Exception):
            return not bool(session.closed)
    return True


def _build_webdav_client_cache_key(
    hostname: str,
    *,
    username: T.Optional[str],
    password: T.Optional[str],
    token: T.Optional[str],
    token_command: T.Optional[str],
    timeout: float,
    insecure: bool,
) -> tuple[T.Hashable, ...]:
    """Build hashable cache key for WebDAV client reuse.

    :param hostname: WebDAV host url with scheme.
    :param username: Optional username.
    :param password: Optional password.
    :param token: Optional resolved token.
    :param token_command: Optional token command text.
    :param timeout: Request timeout.
    :param insecure: SSL verification disabled flag.
    :return: Cache key tuple.
    :rtype: tuple[T.Hashable, ...]
    """
    return (
        hostname,
        username,
        password,
        token,
        token_command,
        timeout,
        insecure,
    )


def clear_webdav_client_cache() -> None:
    """Clear WebDAV client caches for all event loops."""
    with _WEBDAV_CLIENT_CACHE_LOCK:
        _WEBDAV_CLIENT_CACHE.clear()
        _WEBDAV_CLIENT_FALLBACK_CACHE.clear()


def get_webdav_client(
    hostname: str,
    *,
    username: T.Optional[str] = None,
    password: T.Optional[str] = None,
    token: T.Optional[str] = None,
    token_command: T.Optional[str] = None,
    timeout: T.Optional[float] = None,
    insecure: T.Optional[bool] = None,
):
    """Get cached WebDAV client bound to current event loop.

    ``token_command`` has higher priority than ``token``, following megfile
    semantics. If token is resolved, username/password will be ignored.

    :param hostname: WebDAV host url with scheme.
    :param username: Optional username.
    :param password: Optional password.
    :param token: Optional bearer token.
    :param token_command: Optional command to fetch token.
    :param timeout: Optional request timeout in seconds.
    :param insecure: Optional SSL verification flag.
    :return: Cached initialized ``aiodav.client.Client`` instance.
    :rtype: Any
    :raises ModuleNotFoundError: If optional dependency is unavailable.
    """
    resolved_username = (
        username if username is not None else os.getenv(WEBDAV_USERNAME_ENV)
    )
    resolved_password = (
        password if password is not None else os.getenv(WEBDAV_PASSWORD_ENV)
    )
    resolved_token_command = (
        token_command
        if token_command is not None
        else os.getenv(WEBDAV_TOKEN_COMMAND_ENV)
    )
    resolved_token_command = _normalize_optional_text(resolved_token_command)
    resolved_token = load_webdav_token(
        token=token,
        token_command=resolved_token_command,
    )

    if resolved_token is not None:
        resolved_username = None
        resolved_password = None

    resolved_timeout = load_webdav_timeout(timeout)
    resolved_insecure = load_webdav_insecure(insecure)
    cache_key = _build_webdav_client_cache_key(
        hostname,
        username=resolved_username,
        password=resolved_password,
        token=resolved_token,
        token_command=resolved_token_command,
        timeout=resolved_timeout,
        insecure=resolved_insecure,
    )

    try:
        loop: T.Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    with _WEBDAV_CLIENT_CACHE_LOCK:
        if loop is None:
            cache = _WEBDAV_CLIENT_FALLBACK_CACHE
        else:
            cache = _WEBDAV_CLIENT_CACHE.setdefault(loop, {})

        cached_client = cache.get(cache_key)
        if _is_webdav_client_available(cached_client):
            return cached_client

        try:
            client_class = import_aiodav_client_class()
        except ImportError as error:
            raise ModuleNotFoundError(WEBDAV_INSTALL_HINT) from error

        client = client_class(
            hostname=hostname,
            login=resolved_username,
            password=resolved_password,
            token=resolved_token,
            timeout=resolved_timeout,
            insecure=resolved_insecure,
        )
        cache[cache_key] = client
        return client


@dataclass(frozen=True)
class _WebdavEndpoint:
    """Connection endpoint details for a WebDAV server.

    :param scheme: Underlying HTTP scheme (``http``/``https``).
    :param host: WebDAV host.
    :param port: Optional service port.
    :param username: Optional username.
    :param password: Optional password.
    :param token: Optional bearer token.
    :param token_command: Optional command to fetch bearer token.
    :param timeout: Request timeout in seconds.
    :param insecure: Whether SSL verification is disabled.
    """

    scheme: str
    host: str
    port: T.Optional[int] = None
    username: T.Optional[str] = None
    password: T.Optional[str] = None
    token: T.Optional[str] = None
    token_command: T.Optional[str] = None
    timeout: float = WEBDAV_DEFAULT_TIMEOUT
    insecure: bool = False

    @property
    def hostname(self) -> str:
        """Return full aiodav hostname value.

        :return: ``http(s)://host[:port]`` string.
        :rtype: str
        """
        if self.port is None:
            return f"{self.scheme}://{self.host}"
        return f"{self.scheme}://{self.host}:{self.port}"


def is_webdav(path: PathLike) -> bool:
    """Return whether the given path is a WebDAV URI.

    :param path: Path to be tested.
    :return: True when path uses ``webdav://`` or ``webdavs://``.
    :rtype: bool
    """
    parsed = urllib.parse.urlsplit(fspath(path))
    return parsed.scheme in {"webdav", "webdavs"} and bool(parsed.netloc)


def _parse_webdav_timestamp(raw_timestamp: T.Optional[str]) -> float:
    """Parse WebDAV timestamp text into unix timestamp.

    :param raw_timestamp: Timestamp string from WebDAV metadata.
    :return: Parsed unix timestamp, or ``0.0`` when unavailable.
    :rtype: float
    """
    if not raw_timestamp:
        return 0.0

    with suppress(Exception):
        parsed = parsedate_to_datetime(raw_timestamp)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    with suppress(Exception):
        iso_text = raw_timestamp.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(iso_text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    return 0.0


def _entry_name_from_info(info: dict[str, T.Any]) -> str:
    """Extract entry name from aiodav list info item.

    :param info: Metadata dictionary from ``client.list(..., get_info=True)``.
    :return: Entry basename.
    :rtype: str
    """
    name = info.get("name")
    if isinstance(name, str) and name:
        return name.rstrip("/")

    path = str(info.get("path") or "")
    stripped = path.rstrip("/")
    return posixpath.basename(stripped)


def _make_stat_result(info: dict[str, T.Any], *, isdir: bool) -> StatResult:
    """Convert WebDAV metadata info to ``StatResult``.

    :param info: Metadata dictionary from aiodav.
    :param isdir: Whether the resource is a directory.
    :return: Converted stat object.
    :rtype: StatResult
    """
    size = int(info.get("size") or 0)
    modified_time = _parse_webdav_timestamp(
        T.cast(T.Optional[str], info.get("modified"))
    )
    return StatResult(
        st_size=size,
        st_ctime=modified_time,
        st_mtime=modified_time,
        isdir=isdir,
        islnk=False,
        extra=info,
    )


async def _call_webdav(
    uri: str,
    max_retries: int,
    operation: T.Callable[[], T.Awaitable[T.Any]],
) -> T.Any:
    """Execute WebDAV operation with retry and translated exceptions.

    :param uri: Target URI for error reporting.
    :param max_retries: Maximum retry attempts.
    :param operation: Zero-argument async operation.
    :return: Operation result.
    """

    @webdav_retry(max_retries=max_retries)
    async def _execute():
        return await operation()

    try:
        return await _execute()
    except Exception as error:
        translated = translate_webdav_error(error, uri)
        raise translated from error


class AioWebdavWritableFile(AioWritable[T.AnyStr], AioSeekable[T.AnyStr]):
    """Async writable WebDAV file wrapper.

    :param filesystem: Owning WebDAV filesystem.
    :param path: Path without protocol.
    :param mode: File mode.
    :param encoding: Text encoding in text mode.
    :param errors: Text error handling mode.
    """

    def __init__(
        self,
        filesystem: "WebdavFileSystem",
        path: str,
        mode: str,
        encoding: T.Optional[str],
        errors: T.Optional[str],
    ) -> None:
        """Initialize writable WebDAV file adapter.

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

        self._client: T.Optional[AiodavClient] = None
        self._owns_client = False
        self._buffer: T.Union[io.BytesIO, io.StringIO]
        if "b" in mode:
            self._buffer = io.BytesIO()
        else:
            self._buffer = io.StringIO()

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
        """Enter async context and initialize write buffer.

        :return: Opened writable file.
        :rtype: AioWebdavWritableFile
        """
        self._client = self._filesystem._create_client()
        self._owns_client = False
        remote_path = self._filesystem._normalize_remote_path(self._path)
        uri = self.name

        await _call_webdav(
            uri,
            self._filesystem.max_retries,
            lambda: self._filesystem._ensure_parent_directory(
                self._client,
                remote_path,
            ),
        )

        exists = T.cast(
            bool,
            await _call_webdav(
                uri,
                self._filesystem.max_retries,
                lambda: self._client.exists(remote_path),  # pyre-ignore[16]
            ),
        )

        if exists:
            is_dir = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self._filesystem.max_retries,
                    lambda: self._client.is_directory(remote_path),  # pyre-ignore[16]
                ),
            )
            if is_dir:
                raise IsADirectoryError(f"Is a directory: {uri!r}")

        if "x" in self._mode and exists:
            raise FileExistsError(f"File exists: {uri!r}")

        if "a" in self._mode and exists:
            downloaded = io.BytesIO()
            await _call_webdav(
                uri,
                self._filesystem.max_retries,
                lambda: self._client.download_to(  # pyre-ignore[16]
                    remote_path,
                    downloaded,
                ),
            )
            data = downloaded.getvalue()
            if "b" in self._mode:
                self._buffer = io.BytesIO(data)
            else:
                self._buffer = io.StringIO(
                    data.decode(self._encoding, errors=self._errors)
                )
            self._buffer.seek(0, os.SEEK_END)

        return self

    def _ensure_open(self) -> None:
        """Validate file is still open.

        :raises IOError: If file has been closed.
        """
        if self.closed:
            raise IOError(f"file already closed: {self.name!r}")
        if self._client is None:
            raise IOError(f"file not open: {self.name!r}")

    async def write(self, data: T.AnyStr) -> int:
        """Write data into local upload buffer.

        :param data: Data to write.
        :return: Number of written bytes/chars.
        :rtype: int
        """
        self._ensure_open()

        if "b" in self._mode:
            if isinstance(data, (bytes, bytearray)):
                raw_data = bytes(data)
            else:
                raise TypeError("a bytes-like object is required, not 'str'")
            return T.cast(io.BytesIO, self._buffer).write(raw_data)

        if not isinstance(data, str):
            raise TypeError("write() argument must be str in text mode")
        return T.cast(io.StringIO, self._buffer).write(data)

    async def flush(self) -> None:
        """Flush pending writes.

        :return: None.
        :rtype: None
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
        return int(self._buffer.seek(offset, whence))

    async def tell(self) -> int:
        """Return current stream position.

        :return: Stream position.
        :rtype: int
        """
        self._ensure_open()
        return int(self._buffer.tell())

    async def close(self) -> None:
        """Upload buffered content and close writable handle."""
        if self._client is None:
            return

        remote_path = self._filesystem._normalize_remote_path(self._path)
        if "b" in self._mode:
            payload = T.cast(io.BytesIO, self._buffer).getvalue()
        else:
            text_content = T.cast(io.StringIO, self._buffer).getvalue()
            payload = text_content.encode(self._encoding, errors=self._errors)

        data_buffer = io.BytesIO(payload)
        await _call_webdav(
            self.name,
            self._filesystem.max_retries,
            lambda: self._client.upload_to(
                path=remote_path,
                buffer=data_buffer,
                buffer_size=len(payload),
                overwrite=True,
            ),  # pyre-ignore[16]
        )

        self._client = None


class WebdavFileSystem(BaseFileSystem):
    """Filesystem adapter for ``webdav://`` URIs using ``aiodav``."""

    protocol = "webdav"

    def __init__(
        self,
        host: str,
        port: T.Optional[int] = None,
        *,
        username: T.Optional[str] = None,
        password: T.Optional[str] = None,
        token: T.Optional[str] = None,
        token_command: T.Optional[str] = None,
        timeout: T.Optional[float] = None,
        insecure: T.Optional[bool] = None,
        show_port_in_uri: T.Optional[bool] = None,
        show_username_in_uri: T.Optional[bool] = None,
        show_password_in_uri: T.Optional[bool] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    ) -> None:
        """Initialize WebDAV filesystem endpoint.

        :param host: WebDAV host.
        :param port: Optional service port.
        :param username: Optional username.
        :param password: Optional password.
        :param token: Optional bearer token.
        :param token_command: Optional command to fetch bearer token.
        :param timeout: Request timeout in seconds.
        :param insecure: Whether SSL verification is disabled.
        :param show_port_in_uri: Whether to render port in ``build_uri``.
        :param show_username_in_uri: Whether to render username in ``build_uri``.
        :param show_password_in_uri: Whether to render password in ``build_uri``.
        :param max_retries: Maximum retry attempts for WebDAV operations.
        """
        scheme = "https" if self.protocol == "webdavs" else "http"

        self._show_port_in_uri = show_port_in_uri
        if self._show_port_in_uri is None:
            self._show_port_in_uri = port is not None
        self._show_username_in_uri = show_username_in_uri
        if self._show_username_in_uri is None:
            self._show_username_in_uri = username is not None
        self._show_password_in_uri = show_password_in_uri
        if self._show_password_in_uri is None:
            self._show_password_in_uri = False

        self._endpoint = _WebdavEndpoint(
            scheme=scheme,
            host=host,
            port=port,
            username=username,
            password=password,
            token=token,
            token_command=token_command,
            timeout=load_webdav_timeout(timeout),
            insecure=load_webdav_insecure(insecure),
        )
        self.max_retries = int(max_retries)

    @staticmethod
    def _normalize_remote_path(path: str) -> str:
        """Normalize path into absolute remote WebDAV path.

        :param path: Input path without protocol.
        :return: Absolute normalized path.
        :rtype: str
        """
        unquoted = urllib.parse.unquote(path or "/")
        if not unquoted.startswith("/"):
            unquoted = "/" + unquoted
        normalized = posixpath.normpath(unquoted)
        if normalized in ("", "."):
            return "/"
        return normalized

    def _build_cache_key(self) -> tuple[T.Any, ...]:
        """Return cache key components representing current endpoint.

        :return: Hashable endpoint key.
        :rtype: tuple
        """
        return (
            self.protocol,
            self._endpoint.host,
            self._endpoint.port,
            self._endpoint.username,
            self._endpoint.password,
            self._endpoint.token,
            self._endpoint.token_command,
        )

    def _create_client(self) -> AiodavClient:
        """Get cached aiodav client for current endpoint.

        :return: Configured aiodav client.
        :rtype: AiodavClient
        """
        return T.cast(
            AiodavClient,
            get_webdav_client(
                hostname=self._endpoint.hostname,
                username=self._endpoint.username,
                password=self._endpoint.password,
                token=self._endpoint.token,
                token_command=self._endpoint.token_command,
                timeout=self._endpoint.timeout,
                insecure=self._endpoint.insecure,
            ),
        )

    @asynccontextmanager
    async def _session(self):
        """Yield cached WebDAV client for current operation."""
        yield self._create_client()

    async def _ensure_parent_directory(self, client: AiodavClient, path: str) -> None:
        """Create parent directories for a target path when missing.

        :param client: Active aiodav client.
        :param path: Target remote path.
        """
        remote_path = self._normalize_remote_path(path)
        parent = posixpath.dirname(remote_path)
        if parent in ("", "/"):
            return

        current = ""
        for part in parent.strip("/").split("/"):
            current = f"{current}/{part}" if current else f"/{part}"
            uri = self.build_uri(current)
            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(current),
                ),
            )
            if exists:
                is_dir = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.is_directory(current),
                    ),
                )
                if not is_dir:
                    raise FileExistsError(f"File exists: {uri!r}")
                continue

            try:
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.create_directory(current),
                )
            except Exception:
                exists_after_error = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.exists(current),
                    ),
                )
                if exists_after_error:
                    continue
                raise

    def _build_progress_handler(
        self,
        callback: T.Optional[T.Callable[[int], None]],
    ) -> T.Optional[T.Callable[..., None]]:
        """Build aiodav progress callback from byte-delta callback.

        :param callback: Optional callback receiving copied byte delta.
        :return: Progress handler compatible with aiodav APIs.
        :rtype: T.Optional[T.Callable[..., None]]
        """
        if callback is None:
            return None

        state = {"current": 0}

        def _progress(current: int, total: int, *args) -> None:
            _ = total, args
            delta = int(current) - int(state["current"])
            if delta > 0:
                callback(delta)
            state["current"] = int(current)

        return _progress

    @staticmethod
    def _join_uri_path(base_path: str, name: str) -> str:
        """Join URI path and child name while preserving root style.

        :param base_path: Parent path without protocol.
        :param name: Child entry name.
        :return: Joined path without protocol.
        :rtype: str
        """
        normalized_base = WebdavFileSystem._normalize_remote_path(base_path)
        if normalized_base == "/":
            return f"/{name}"
        return f"{normalized_base.rstrip('/')}/{name}"

    async def is_dir(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path is directory, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                ),
            )
            if not exists:
                return False
            return T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.is_directory(remote_path),
                ),
            )

    async def is_file(self, path: str, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path is file, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                ),
            )
            if not exists:
                return False
            is_dir = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.is_directory(remote_path),
                ),
            )
            return not is_dir

    async def exists(self, path: str, followlinks: bool = False) -> bool:
        """Return whether the path points to an existing resource.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path exists, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            return T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                ),
            )

    async def stat(self, path: str, followlinks: bool = False) -> StatResult:
        """Get metadata status for the path.

        :param path: Path without protocol.
        :param followlinks: Ignored for WebDAV protocol.
        :return: ``StatResult`` for the path.
        :rtype: StatResult
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            info = T.cast(
                dict[str, T.Any],
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.info(remote_path),
                ),
            )
            is_dir = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.is_directory(remote_path),
                ),
            )
            return _make_stat_result(info, isdir=is_dir)

    async def remove(self, path: str, missing_ok: bool = False) -> None:
        """Remove (delete) file or directory recursively.

        :param path: Path without protocol.
        :param missing_ok: Ignore missing target when True.
        """
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                ),
            )
            if not exists:
                if missing_ok:
                    return
                raise FileNotFoundError(f"No such file: {uri!r}")

            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.delete(remote_path),
            )

    async def mkdir(
        self,
        path: str,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create a directory.

        :param path: Directory path without protocol.
        :param mode: Permission bits for compatibility only.
        :param parents: Whether to create parent directories.
        :param exist_ok: Whether to ignore existing directory.
        :raises FileExistsError: If directory exists and ``exist_ok`` is False.
        """
        _ = mode
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        async with self._session() as client:
            if remote_path == "/":
                return

            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                ),
            )
            if exists:
                is_dir = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.is_directory(remote_path),
                    ),
                )
                if is_dir and exist_ok:
                    return
                raise FileExistsError(f"File exists: {uri!r}")

            if parents:
                await self._ensure_parent_directory(client, remote_path)
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.create_directory(remote_path),
            )

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

        Read mode uses ``AioWebdavPrefetchReader``.

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

            return AioWebdavPrefetchReader(
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
                block_forward=int(block_forward) if block_forward is not None else None,
                max_retries=(
                    int(max_retries) if max_retries is not None else self.max_retries
                ),
            )

        return AioWebdavWritableFile(
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
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            async with self._session() as client:
                exists = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.exists(remote_path),
                    ),
                )
                if not exists:
                    raise FileNotFoundError(f"No such file or directory: {uri!r}")

                is_dir = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.is_directory(remote_path),
                    ),
                )
                if not is_dir:
                    raise NotADirectoryError(f"Not a directory: {uri!r}")

                infos = T.cast(
                    list[dict[str, T.Any]],
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.list(remote_path, get_info=True),
                    ),
                )

                for info in sorted(infos, key=_entry_name_from_info):
                    name = _entry_name_from_info(info)
                    if name in ("", ".", ".."):
                        continue
                    entry_path = self._join_uri_path(remote_path, name)
                    is_child_dir = bool(info.get("isdir"))
                    yield FileEntry(
                        name=name,
                        path=entry_path,
                        stat=_make_stat_result(info, isdir=is_child_dir),
                    )

        iterator = aiterator()

        async def aexit(exc_type, exc_value, traceback) -> None:
            with suppress(Exception):
                await iterator.aclose()

        return AioScannableManager(iterator, aexit)

    def scanfile(
        self,
        path: str,
    ) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return async iterator over files recursively.

        :param path: Root path without protocol.
        :return: Async context manager yielding file ``FileEntry`` values.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)

        async def _iter_files(
            client: AiodavClient,
            current_path: str,
        ) -> T.AsyncIterator[FileEntry]:
            current_uri = self.build_uri(current_path)
            current_info = T.cast(
                dict[str, T.Any],
                await _call_webdav(
                    current_uri,
                    self.max_retries,
                    lambda: client.info(current_path),
                ),
            )
            current_is_dir = T.cast(
                bool,
                await _call_webdav(
                    current_uri,
                    self.max_retries,
                    lambda: client.is_directory(current_path),
                ),
            )

            if not current_is_dir:
                name = posixpath.basename(current_path.rstrip("/")) or ""
                yield FileEntry(
                    name=name,
                    path=current_path,
                    stat=_make_stat_result(current_info, isdir=False),
                )
                return

            infos = T.cast(
                list[dict[str, T.Any]],
                await _call_webdav(
                    current_uri,
                    self.max_retries,
                    lambda: client.list(current_path, get_info=True),
                ),
            )
            for info in sorted(infos, key=_entry_name_from_info):
                name = _entry_name_from_info(info)
                if name in ("", ".", ".."):
                    continue
                child_path = self._join_uri_path(current_path, name)
                child_is_dir = bool(info.get("isdir"))
                if child_is_dir:
                    async for nested_entry in _iter_files(client, child_path):
                        yield nested_entry
                else:
                    yield FileEntry(
                        name=name,
                        path=child_path,
                        stat=_make_stat_result(info, isdir=False),
                    )

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            async with self._session() as client:
                exists = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.exists(remote_path),
                    ),
                )
                if not exists:
                    raise FileNotFoundError(f"No such file or directory: {uri!r}")

                async for file_entry in _iter_files(client, remote_path):
                    yield file_entry

        iterator = aiterator()

        async def aexit(exc_type, exc_value, traceback) -> None:
            with suppress(Exception):
                await iterator.aclose()

        return AioScannableManager(iterator, aexit)

    async def upload(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Upload a local file to WebDAV.

        :param src_path: Local source file path.
        :param dst_path: Remote destination path without protocol.
        :param callback: Optional progress callback receiving byte deltas.
        """
        if os.path.isdir(src_path):
            raise IsADirectoryError(f"Is a directory: {src_path!r}")
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"No such file: {src_path!r}")

        remote_path = self._normalize_remote_path(dst_path)
        uri = self.build_uri(remote_path)
        size = os.path.getsize(src_path)
        progress = self._build_progress_handler(callback)

        async with self._session() as client:
            await self._ensure_parent_directory(client, remote_path)
            async with aiofiles.open(src_path, "rb") as file_obj:
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.upload_to(
                        path=remote_path,
                        buffer=file_obj,
                        buffer_size=size,
                        overwrite=True,
                        progress=progress,
                    ),
                )

    async def download(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Download a remote file from WebDAV to local path.

        :param src_path: Remote source path without protocol.
        :param dst_path: Local destination file path.
        :param callback: Optional progress callback receiving byte deltas.
        """
        remote_path = self._normalize_remote_path(src_path)
        uri = self.build_uri(remote_path)
        progress = self._build_progress_handler(callback)

        async with self._session() as client:
            is_dir = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.is_directory(remote_path),
                ),
            )
            if is_dir:
                raise IsADirectoryError(f"Is a directory: {uri!r}")

            parent_dir = os.path.dirname(dst_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            async with aiofiles.open(dst_path, "wb") as file_obj:
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.download_to(
                        path=remote_path,
                        buffer=file_obj,
                        progress=progress,
                    ),
                )

    async def copy(
        self,
        src_path: str,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> str:
        """Copy a single file on WebDAV.

        :param src_path: Source path without protocol.
        :param dst_path: Destination path without protocol.
        :param callback: Optional callback receiving copied byte deltas.
        :return: Destination path after copy.
        :rtype: str
        """
        src_remote = self._normalize_remote_path(src_path)
        dst_remote = self._normalize_remote_path(dst_path)
        src_uri = self.build_uri(src_remote)
        dst_uri = self.build_uri(dst_remote)

        if src_remote == dst_remote:
            raise OSError(f"'{src_uri}' and '{dst_uri}' are the same file")

        async with self._session() as client:
            src_is_dir = T.cast(
                bool,
                await _call_webdav(
                    src_uri,
                    self.max_retries,
                    lambda: client.is_directory(src_remote),
                ),
            )
            if src_is_dir:
                raise IsADirectoryError(f"Is a directory: {src_uri!r}")

            await self._ensure_parent_directory(client, dst_remote)
            src_info = T.cast(
                dict[str, T.Any],
                await _call_webdav(
                    src_uri,
                    self.max_retries,
                    lambda: client.info(src_remote),
                ),
            )
            await _call_webdav(
                dst_uri,
                self.max_retries,
                lambda: client.copy(src_remote, dst_remote, depth=1),
            )
            if callback is not None:
                callback(int(src_info.get("size") or 0))

        return dst_path

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> str:
        """Move file or directory on WebDAV.

        :param src_path: Source path without protocol.
        :param dst_path: Destination path without protocol.
        :param overwrite: Whether to overwrite destination when exists.
        :return: Destination path after move.
        :rtype: str
        :raises FileExistsError: If destination exists and ``overwrite`` is False.
        """
        src_remote = self._normalize_remote_path(src_path)
        dst_remote = self._normalize_remote_path(dst_path)
        src_uri = self.build_uri(src_remote)
        dst_uri = self.build_uri(dst_remote)

        if src_remote == dst_remote:
            return dst_path

        async with self._session() as client:
            src_exists = T.cast(
                bool,
                await _call_webdav(
                    src_uri,
                    self.max_retries,
                    lambda: client.exists(src_remote),
                ),
            )
            if not src_exists:
                raise FileNotFoundError(f"No such file: {src_uri!r}")

            dst_exists = T.cast(
                bool,
                await _call_webdav(
                    dst_uri,
                    self.max_retries,
                    lambda: client.exists(dst_remote),
                ),
            )
            if dst_exists and not overwrite:
                raise FileExistsError(f"File exists: {dst_uri!r}")
            if dst_exists and overwrite:
                await _call_webdav(
                    dst_uri,
                    self.max_retries,
                    lambda: client.delete(dst_remote),
                )

            await self._ensure_parent_directory(client, dst_remote)
            await _call_webdav(
                src_uri,
                self.max_retries,
                lambda: client.move(src_remote, dst_remote, overwrite=overwrite),
            )

        return dst_path

    async def is_symlink(self, path: str) -> bool:
        """Return whether path points to a symbolic link.

        WebDAV does not support symlinks.

        :param path: Path without protocol.
        :return: Always ``False``.
        :rtype: bool
        """
        _ = path
        return False

    async def absolute(self, path: str) -> str:
        """Return absolute normalized path without resolving symlinks.

        :param path: Path without protocol.
        :return: Absolute normalized path.
        :rtype: str
        """
        return self._normalize_remote_path(path)

    async def samefile(self, path: str, other_path: str) -> bool:
        """Return whether two paths refer to the same remote resource.

        :param path: Path without protocol.
        :param other_path: Other path without protocol.
        :return: True when both normalized paths are equal and exist.
        :rtype: bool
        """
        normalized_path = self._normalize_remote_path(path)
        normalized_other = self._normalize_remote_path(other_path)
        if normalized_path != normalized_other:
            return False
        return await self.exists(normalized_path)

    async def access(self, path: str, mode: Access = Access.READ) -> bool:
        """Test if path has access permission described by mode.

        :param path: Path without protocol.
        :param mode: Access mode.
        :return: True if path is accessible by requested mode.
        :rtype: bool
        """
        if not isinstance(mode, Access):
            raise TypeError("Unsupported mode: %r" % (mode,))

        if mode == Access.READ:
            return await self.exists(path)

        if mode == Access.WRITE:
            remote_path = self._normalize_remote_path(path)
            if await self.exists(remote_path):
                return not await self.is_dir(remote_path)
            parent = posixpath.dirname(remote_path)
            if parent in ("", "."):
                parent = "/"
            return await self.exists(parent)

        return False

    async def is_absolute(self, path: str) -> bool:
        """Return whether a path is absolute.

        :param path: Path without protocol.
        :return: True when path starts with ``/``.
        :rtype: bool
        """
        return path.startswith("/")

    def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
        """Return whether this filesystem points to same WebDAV endpoint.

        :param other_filesystem: Filesystem to compare.
        :return: True when both filesystems share endpoint settings.
        :rtype: bool
        """
        if not isinstance(other_filesystem, WebdavFileSystem):
            return False
        return self._build_cache_key() == other_filesystem._build_cache_key()

    def parse_uri(self, uri: str) -> str:
        """Parse URI into path part without protocol.

        :param uri: URI string.
        :return: Path without protocol.
        :rtype: str
        """
        parsed = urllib.parse.urlsplit(uri)
        if parsed.scheme == "":
            return self._normalize_remote_path(uri)
        return self._normalize_remote_path(parsed.path or "/")

    def build_uri(self, path: str) -> str:
        """Build URI from path part.

        :param path: Path without protocol.
        :return: Full WebDAV URI.
        :rtype: str
        """
        normalized_path = self._normalize_remote_path(path)
        quoted_path = urllib.parse.quote(normalized_path, safe="/")

        netloc = self._endpoint.host
        if self._show_port_in_uri and self._endpoint.port is not None:
            netloc = f"{netloc}:{self._endpoint.port}"

        if self._show_username_in_uri and self._endpoint.username:
            username = urllib.parse.quote(self._endpoint.username, safe="")
            if self._show_password_in_uri and self._endpoint.password is not None:
                password = urllib.parse.quote(self._endpoint.password, safe="")
                userinfo = f"{username}:{password}"
            else:
                userinfo = username
            netloc = f"{userinfo}@{netloc}"

        return urllib.parse.urlunsplit((self.protocol, netloc, quoted_path, "", ""))

    @classmethod
    def from_uri(cls, uri: str) -> "WebdavFileSystem":
        """Create filesystem instance from URI.

        :param uri: URI string.
        :return: WebdavFileSystem instance.
        :rtype: WebdavFileSystem
        """
        parsed = urllib.parse.urlsplit(uri)
        if parsed.scheme != cls.protocol:
            raise ValueError(f"unsupported scheme for webdav filesystem: {uri!r}")
        if not parsed.hostname:
            raise ValueError(f"missing host in webdav uri: {uri!r}")

        username = (
            urllib.parse.unquote(parsed.username)
            if parsed.username is not None
            else os.getenv(WEBDAV_USERNAME_ENV)
        )
        password = (
            urllib.parse.unquote(parsed.password)
            if parsed.password is not None
            else os.getenv(WEBDAV_PASSWORD_ENV)
        )
        token = os.getenv(WEBDAV_TOKEN_ENV)
        token_command = os.getenv(WEBDAV_TOKEN_COMMAND_ENV)

        return cls(
            host=parsed.hostname,  # pyre-ignore[6]
            port=parsed.port,
            username=username,
            password=password,
            token=token,
            token_command=token_command,
            timeout=load_webdav_timeout(),
            insecure=load_webdav_insecure(),
            show_port_in_uri=parsed.port is not None,
            show_username_in_uri=parsed.username is not None,
            show_password_in_uri=parsed.password is not None,
        )


class WebdavsFileSystem(WebdavFileSystem):
    """Filesystem adapter for ``webdavs://`` URIs."""

    protocol = "webdavs"
