"""Filesystem adapter for WebDAV resources backed by ``aiodav``."""

from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import shlex
import subprocess
import threading
import time
import typing as T
import urllib.parse
import weakref
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from aiomegfile.config import (
    DEFAULT_COPY_BUFFER_SIZE,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    WEBDAV_MAX_RETRY_TIMES,
)
from aiomegfile.errors.webdav import (
    WebdavFileExistsError,
    WebdavFileNotFoundError,
    WebdavIsADirectoryError,
    WebdavNotADirectoryError,
    WebdavPermissionError,
    WebdavSameFileError,
    _ensure_aiodav,
    translate_webdav_error,
    webdav_retry,
)
from aiomegfile.interfaces import (
    AioScannableManager,
    BaseFileSystem,
    FileEntry,
    StatResult,
)
from aiomegfile.lib.cacher import AioFileCacher
from aiomegfile.lib.prefetch_reader.webdav_prefetch_reader import (
    AioWebdavPrefetchReader,
)
from aiomegfile.utils.parse import parse_boolean
from aiomegfile.utils.path import PathLike, fspath

if T.TYPE_CHECKING:
    from aiodav.client import Client as AiodavClient

logger = logging.getLogger(__name__)

__all__ = [
    "WEBDAV_DEFAULT_TIMEOUT",
    "WEBDAV_INSECURE_ENV",
    "WEBDAV_PASSWORD_ENV",
    "WEBDAV_TIMEOUT_ENV",
    "WEBDAV_TOKEN_COMMAND_ENV",
    "WEBDAV_TOKEN_ENV",
    "WEBDAV_USERNAME_ENV",
    "get_webdav_client",
    "WebdavFileSystem",
    "WebdavsFileSystem",
    "is_webdav",
]

WEBDAV_DEFAULT_TIMEOUT = 30
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
_WEBDAV_CLIENT_CACHE_LOCK = threading.Lock()


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


def _load_webdav_timeout(timeout: T.Optional[float] = None) -> int:
    """Return WebDAV timeout from argument/environment with fallback.

    :param timeout: Explicit timeout value.
    :return: Timeout in seconds.
    :rtype: int
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
    return int(parsed_timeout) if parsed_timeout > 0 else WEBDAV_DEFAULT_TIMEOUT


def _load_webdav_insecure(insecure: T.Optional[bool] = None) -> bool:
    """Return WebDAV insecure mode from argument/environment.

    :param insecure: Explicit insecure flag.
    :return: True when SSL verification is disabled.
    :rtype: bool
    """
    if insecure is not None:
        return bool(insecure)
    return parse_boolean(os.getenv(WEBDAV_INSECURE_ENV), default=False)


def _load_webdav_token_from_command(token_command: str) -> str:
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


def _load_webdav_token(
    *,
    token: T.Optional[str] = None,
    token_command: T.Optional[str] = None,
) -> T.Optional[str]:
    """Resolve WebDAV token from explicit token, environment, then command.

    :param token: Explicit token value.
    :param token_command: Explicit token command value.
    :return: Resolved token text or ``None``.
    :rtype: T.Optional[str]
    """
    resolved_token = _normalize_optional_text(token)
    if resolved_token is not None:
        return resolved_token

    resolved_token = _normalize_optional_text(os.getenv(WEBDAV_TOKEN_ENV))
    if resolved_token is not None:
        return resolved_token

    resolved_token_command = _normalize_optional_text(token_command)
    if resolved_token_command is None:
        resolved_token_command = _normalize_optional_text(
            os.getenv(WEBDAV_TOKEN_COMMAND_ENV)
        )
    if resolved_token_command is None:
        return None

    return _load_webdav_token_from_command(resolved_token_command)


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


def _finalize_webdav_client_session(
    loop: asyncio.AbstractEventLoop,
    session: T.Any,
) -> None:
    """Schedule session close on the event loop that created the client.

    :param loop: Event loop associated with the client.
    :param session: Session object to close.
    :return: ``None``.
    :rtype: None
    """
    if session is None:
        return
    if loop.is_closed():
        logger.debug(
            "Skip closing WebDAV session during finalization "
            "because the event loop is closed"
        )
        return

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    try:
        if running_loop is loop:
            future = loop.create_task(session.close())
        else:
            future = asyncio.run_coroutine_threadsafe(session.close(), loop)
    except Exception as error:
        logger.debug(
            "Failed to schedule WebDAV session close during finalization: %s",
            error,
        )
        return

    def _log_close_result(close_future: T.Any) -> None:
        try:
            close_future.result()
        except Exception as error:
            logger.debug(
                "Failed to close WebDAV session during finalization: %s",
                error,
            )

    future.add_done_callback(_log_close_result)


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


async def get_webdav_client(
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

    Explicit token and ``WEBDAV_TOKEN`` are resolved before ``token_command``.
    If token is resolved, username/password will be ignored.

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
    _ensure_aiodav()

    from aiodav.client import Client

    resolved_username = _normalize_optional_text(
        username if username is not None else os.getenv(WEBDAV_USERNAME_ENV)
    )
    resolved_password = _normalize_optional_text(
        password if password is not None else os.getenv(WEBDAV_PASSWORD_ENV)
    )
    resolved_token_command = _normalize_optional_text(token_command)
    if resolved_token_command is None:
        resolved_token_command = _normalize_optional_text(
            os.getenv(WEBDAV_TOKEN_COMMAND_ENV)
        )

    resolved_token = _load_webdav_token(
        token=token,
        token_command=resolved_token_command,
    )
    # aiohttp not allow username/password when token is provided,
    # as may ignore basic auth when bearer token is present
    client_username = None if resolved_token is not None else resolved_username
    client_password = None if resolved_token is not None else resolved_password

    resolved_timeout = _load_webdav_timeout(timeout)
    resolved_insecure = _load_webdav_insecure(insecure)
    cache_key = _build_webdav_client_cache_key(
        hostname,
        username=client_username,
        password=client_password,
        token=resolved_token,
        token_command=resolved_token_command,
        timeout=resolved_timeout,
        insecure=resolved_insecure,
    )

    loop: T.Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()

    with _WEBDAV_CLIENT_CACHE_LOCK:
        cache = _WEBDAV_CLIENT_CACHE.setdefault(loop, {})
        cached_client = cache.get(cache_key)
        if _is_webdav_client_available(cached_client):
            return cached_client

        client = Client(
            hostname=hostname,
            login=client_username,
            password=client_password,
            token=resolved_token,
            timeout=resolved_timeout,
            insecure=resolved_insecure,
        )
        client._token_command = resolved_token_command  # pyre-ignore[16]
        client._token_command_last_call = 0  # pyre-ignore[16]
        cache[cache_key] = client

        weakref.finalize(
            client,
            _finalize_webdav_client_session,
            loop,
            client.session,
        )

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
    timeout: T.Optional[float] = None
    insecure: T.Optional[bool] = None

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
    client: T.Optional["AiodavClient"],
) -> T.Any:
    """Execute WebDAV operation with retry and translated exceptions.

    :param uri: Target URI for error reporting.
    :param max_retries: Maximum retry attempts.
    :param operation: Zero-argument async operation.
    :return: Operation result.
    """

    async def retry_callback(error: Exception, *args, **kwargs) -> None:
        """Optional callback executed before each retry.

        :param error: Caught exception triggering the retry.
        :param args: Positional arguments passed to the operation.
        :param kwargs: Keyword arguments passed to the operation.
        :return: None.
        :rtype: T.Awaitable[None]
        """
        _ensure_aiodav()
        from aiodav.exceptions import ResponseErrorCode

        if isinstance(error, ResponseErrorCode):
            status = int(getattr(error, "code", 0))
            if status == 401:
                token_command = getattr(client, "_token_command", None)
                last_call = getattr(client, "_token_command_last_call", 0)
                if token_command is not None and time.time() - last_call > 5:
                    client._token_command_last_call = time.time()  # pyre-ignore[16]
                    client._token = _load_webdav_token_from_command(  # pyre-ignore[16]
                        token_command
                    )
                    logger.debug(
                        "update webdav token by command: %s",
                        token_command,
                    )
                    return
                raise WebdavPermissionError(f"Permission denied: {uri!r}")

    @webdav_retry(max_retries=max_retries, retry_callback=retry_callback)
    async def _execute():
        return await operation()

    try:
        return await _execute()
    except Exception as error:
        translated = translate_webdav_error(error, uri)
        raise translated from error


class WebdavFileSystem(BaseFileSystem):
    """
    Filesystem adapter for ``webdav://`` URIs using ``aiodav``.

    uri format:
        - webdav://[username[:password]@]hostname[:port]/file_path
        - webdavs://[username[:password]@]hostname[:port]/file_path
    """

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
        max_retries: int = WEBDAV_MAX_RETRY_TIMES,
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
            timeout=timeout,
            insecure=insecure,
        )
        self.max_retries = int(max_retries)
        self._client = None

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
            self._endpoint.timeout,
            self._endpoint.insecure,
        )

    async def _create_client(self) -> "AiodavClient":
        """Get cached aiodav client for current endpoint.

        :return: Configured aiodav client.
        :rtype: AiodavClient
        """
        if not self._client:
            self._client = await get_webdav_client(
                hostname=self._endpoint.hostname,
                username=self._endpoint.username,
                password=self._endpoint.password,
                token=self._endpoint.token,
                token_command=self._endpoint.token_command,
                timeout=self._endpoint.timeout,
                insecure=self._endpoint.insecure,
            )
        return self._client

    async def _ensure_parent_directory(self, client: "AiodavClient", path: str) -> None:
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
                    client,
                ),
            )
            if exists:
                is_dir = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.is_directory(current),
                        client,
                    ),
                )
                if not is_dir:
                    raise WebdavFileExistsError(f"File exists: {uri!r}")
                continue

            try:
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.create_directory(current),
                    client,
                )
            except Exception:
                exists_after_error = T.cast(
                    bool,
                    await _call_webdav(
                        uri,
                        self.max_retries,
                        lambda: client.exists(current),
                        client,
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
                callback(delta)  # pyre-ignore[29]
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

    async def is_dir(self, path: str, followlinks: bool = True) -> bool:
        """Return True if the path points to a directory.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path is directory, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        client = await self._create_client()
        try:
            return await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.is_directory(remote_path),
                client,
            )
        except WebdavFileNotFoundError:
            return False

    async def is_file(self, path: str, followlinks: bool = True) -> bool:
        """Return True if the path points to a regular file.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path is file, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        client = await self._create_client()
        try:
            return not await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.is_directory(remote_path),
                client,
            )
        except WebdavFileNotFoundError:
            return False

    async def exists(self, path: str, followlinks: bool = True) -> bool:
        """Return whether the path points to an existing resource.

        :param path: The path to check.
        :param followlinks: Ignored for WebDAV protocol.
        :return: True if path exists, otherwise False.
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        client = await self._create_client()
        return T.cast(
            bool,
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.exists(remote_path),
                client,
            ),
        )

    async def stat(self, path: str, followlinks: bool = True) -> StatResult:
        """Get metadata status for the path.

        :param path: Path without protocol.
        :param followlinks: Ignored for WebDAV protocol.
        :return: ``StatResult`` for the path.
        :rtype: StatResult
        """
        _ = followlinks
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)
        client = await self._create_client()
        info = T.cast(
            dict[str, T.Any],
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.info(remote_path),
                client,
            ),
        )
        is_dir = T.cast(
            bool,
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.is_directory(remote_path),
                client,
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
        client = await self._create_client()

        try:
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.delete(remote_path),
                client,
            )
        except WebdavFileNotFoundError:
            if missing_ok:
                return
            raise

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
        client = await self._create_client()
        if remote_path == "/":
            return

        exists = True
        try:
            is_dir = await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.is_directory(remote_path),
                client,
            )
        except WebdavFileNotFoundError:
            exists = is_dir = False

        if exists:
            if is_dir and exist_ok:
                return
            raise WebdavFileExistsError(f"File exists: {uri!r}")

        if parents:
            await self._ensure_parent_directory(client, remote_path)
        await _call_webdav(
            uri,
            self.max_retries,
            lambda: client.create_directory(remote_path),
            client,
        )

    def _open_cacher(self, path: str, mode: str) -> AioFileCacher:
        """Return a cache-backed writable WebDAV file handle.

        :param path: Remote path without protocol.
        :param mode: Writable file mode.
        :return: Cache-backed async file handle.
        :rtype: AioFileCacher
        """
        return AioFileCacher(
            path,
            mode,
            download_fileobj=self._download_fileobj,
            upload_fileobj=self._upload_fileobj,
        )

    def _open_exclusive_cacher(
        self,
        path: str,
        mode: str,
    ) -> T.AsyncContextManager[AioFileCacher]:
        """Return a best-effort exclusive-create writer backed by local cache.

        :param path: Remote path without protocol.
        :param mode: Exclusive file mode, such as ``x`` or ``xb``.
        :return: Async context manager for exclusive creation.
        :rtype: T.AsyncContextManager[AioFileCacher]
        """
        uri = self.build_uri(path)
        create_mode = mode.replace("x", "w")

        @asynccontextmanager
        async def manager() -> T.AsyncIterator[AioFileCacher]:
            if await self.exists(path):
                raise WebdavFileExistsError(f"File exists: {uri!r}")
            async with self._open_cacher(path, create_mode) as file_obj:
                yield file_obj

        return manager()

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

        Read mode uses ``AioWebdavPrefetchReader``. Append mode uses
        ``AioFileCacher`` to preserve local append semantics.

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
                    block_size if block_size is not None else READER_BLOCK_SIZE
                ),
                max_buffer_size=(
                    max_buffer_size
                    if max_buffer_size is not None
                    else READER_MAX_BUFFER_SIZE
                ),
                block_forward=(block_forward if block_forward is not None else None),
                max_retries=(
                    max_retries if max_retries is not None else self.max_retries
                ),
            )
        if "x" in normalized_mode:
            return self._open_exclusive_cacher(path, normalized_mode)
        return self._open_cacher(path, normalized_mode)

    def scandir(self, path: str) -> T.AsyncContextManager[T.AsyncIterator[FileEntry]]:
        """Return async iterator over direct children of a directory.

        :param path: Directory path without protocol.
        :return: Async context manager yielding ``FileEntry`` values.
        :rtype: T.AsyncContextManager[T.AsyncIterator[FileEntry]]
        """
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)

        async def aiterator() -> T.AsyncIterator[FileEntry]:
            client = await self._create_client()

            is_dir = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.is_directory(remote_path),
                    client,
                ),
            )
            if not is_dir:
                raise WebdavNotADirectoryError(f"Not a directory: {uri!r}")

            infos = T.cast(
                list[dict[str, T.Any]],
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.list(remote_path, get_info=True),
                    client,
                ),
            )

            for info in sorted(infos, key=_entry_name_from_info):
                name = _entry_name_from_info(info)
                if name in ("", ".", ".."):
                    continue
                entry_path = self._join_uri_path(remote_path, name)
                is_child_dir = bool(info["isdir"])
                yield FileEntry(
                    name=name,
                    path=entry_path,
                    stat=_make_stat_result(info, isdir=is_child_dir),
                )

        return AioScannableManager(aiterator())

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
        remote_path = self._normalize_remote_path(path)
        uri = self.build_uri(remote_path)

        async def _iter_files(
            client: "AiodavClient",
            current_path: str,
        ) -> T.AsyncIterator[FileEntry]:
            current_uri = self.build_uri(current_path)
            current_is_dir = T.cast(
                bool,
                await _call_webdav(
                    current_uri,
                    self.max_retries,
                    lambda: client.is_directory(current_path),
                    client,
                ),
            )

            if not current_is_dir:
                name = posixpath.basename(current_path.rstrip("/")) or ""
                current_info = T.cast(
                    dict[str, T.Any],
                    await _call_webdav(
                        current_uri,
                        self.max_retries,
                        lambda: client.info(current_path),
                        client,
                    ),
                )
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
                    client,
                ),
            )
            for info in sorted(infos, key=_entry_name_from_info):
                name = _entry_name_from_info(info)
                if name in ("", ".", ".."):
                    continue
                child_path = self._join_uri_path(current_path, name)
                child_is_dir = bool(info["isdir"])
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
            client = await self._create_client()
            exists = T.cast(
                bool,
                await _call_webdav(
                    uri,
                    self.max_retries,
                    lambda: client.exists(remote_path),
                    client,
                ),
            )
            if not exists:
                raise WebdavFileNotFoundError(f"No such file or directory: {uri!r}")

            async for file_entry in _iter_files(client, remote_path):
                yield file_entry

        return AioScannableManager(aiterator())

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
            raise WebdavIsADirectoryError(f"Is a directory: {src_path!r}")
        if not os.path.exists(src_path):
            raise WebdavFileNotFoundError(f"No such file: {src_path!r}")

        remote_path = self._normalize_remote_path(dst_path)
        uri = self.build_uri(remote_path)
        progress = self._build_progress_handler(callback)

        client = await self._create_client()
        await self._ensure_parent_directory(client, remote_path)
        await _call_webdav(
            uri,
            self.max_retries,
            lambda: client.upload_file(
                remote_path=remote_path,
                local_path=src_path,
                progress=progress,
            ),
            client,
        )

    async def _download_fileobj(
        self,
        src_path: str,
        fileobj: T.Any,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Download a WebDAV file into a file-like object.

        :param src_path: Remote source path without protocol.
        :param fileobj: Destination file-like object.
        :param callback: Optional progress callback receiving chunk sizes.
        :return: ``None``.
        :rtype: None
        """
        file_mode = getattr(fileobj, "mode", "")
        mode = "rb" if "b" in file_mode else "r"

        async with AioWebdavPrefetchReader(
            src_path,
            filesystem=self,
            mode=mode,
        ) as webdav_file:
            while True:
                chunk = await webdav_file.read(DEFAULT_COPY_BUFFER_SIZE)
                if not chunk:
                    break
                await fileobj.write(chunk)
                if callback is not None:
                    callback(len(chunk))

    async def _upload_fileobj(
        self,
        fileobj: T.Any,
        dst_path: str,
        callback: T.Optional[T.Callable[[int], None]] = None,
    ) -> None:
        """Upload a file-like object to WebDAV.

        :param fileobj: Source file-like object.
        :param dst_path: Remote destination path without protocol.
        :param callback: Optional progress callback receiving chunk sizes.
        :return: ``None``.
        :rtype: None
        """
        file_mode = getattr(fileobj, "mode", "")
        mode = "wb" if "b" in file_mode else "w"
        if "b" not in mode:

            class _AsyncBytesWrapper:
                def __init__(self, fileobj: T.Any):
                    self._fileobj = fileobj

                async def read(self, size: int = -1) -> bytes:
                    data = await self._fileobj.read(size)
                    if isinstance(data, str):
                        return data.encode()
                    return data

                def __getattr__(self, name):
                    return getattr(self._fileobj, name)

            fileobj = _AsyncBytesWrapper(fileobj)

        client = await self._create_client()
        await _call_webdav(
            dst_path,
            self.max_retries,
            lambda: client.upload_to(
                dst_path,
                buffer=fileobj,
                progress=self._build_progress_handler(callback),
            ),
            client,
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

        client = await self._create_client()
        is_dir = T.cast(
            bool,
            await _call_webdav(
                uri,
                self.max_retries,
                lambda: client.is_directory(remote_path),
                client,
            ),
        )
        if is_dir:
            raise WebdavIsADirectoryError(f"Is a directory: {uri!r}")

        parent_dir = os.path.dirname(dst_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        await _call_webdav(
            uri,
            self.max_retries,
            lambda: client.download_file(
                remote_path,
                dst_path,
                progress=progress,
            ),
            client,
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
            raise WebdavSameFileError(f"'{src_uri}' and '{dst_uri}' are the same file")

        client = await self._create_client()
        src_is_dir = T.cast(
            bool,
            await _call_webdav(
                src_uri,
                self.max_retries,
                lambda: client.is_directory(src_remote),
                client,
            ),
        )
        if src_is_dir:
            raise WebdavIsADirectoryError(f"Is a directory: {src_uri!r}")

        await self._ensure_parent_directory(client, dst_remote)
        await _call_webdav(
            dst_uri,
            self.max_retries,
            lambda: client.copy(src_remote, dst_remote, depth=1),
            client,
        )
        if callback is not None:
            src_info = T.cast(
                dict[str, T.Any],
                await _call_webdav(
                    src_uri,
                    self.max_retries,
                    lambda: client.info(src_remote),
                    client,
                ),
            )
            callback(int(src_info.get("size") or 0))

        return dst_path

    async def move(self, src_path: str, dst_path: str) -> str:
        """Move file or directory on WebDAV.

        :param src_path: Source path without protocol.
        :param dst_path: Destination path without protocol.
        :return: Destination path after move.
        :rtype: str
        """
        src_remote = self._normalize_remote_path(src_path)
        dst_remote = self._normalize_remote_path(dst_path)
        src_uri = self.build_uri(src_remote)

        if src_remote == dst_remote:
            return dst_path

        client = await self._create_client()
        src_exists = T.cast(
            bool,
            await _call_webdav(
                src_uri,
                self.max_retries,
                lambda: client.exists(src_remote),
                client,
            ),
        )
        if not src_exists:
            raise WebdavFileNotFoundError(f"No such file: {src_uri!r}")

        await self._ensure_parent_directory(client, dst_remote)
        await _call_webdav(
            src_uri,
            self.max_retries,
            lambda: client.move(src_remote, dst_remote, overwrite=True),
            client,
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
        if normalized_path == normalized_other:
            return True
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
            else None
        )
        password = (
            urllib.parse.unquote(parsed.password)
            if parsed.password is not None
            else None
        )

        return cls(
            host=parsed.hostname,
            port=parsed.port,
            username=username,
            password=password,
            timeout=_load_webdav_timeout(),
            insecure=_load_webdav_insecure(),
            show_port_in_uri=parsed.port is not None,
            show_username_in_uri=parsed.username is not None,
            show_password_in_uri=parsed.password is not None,
        )


class WebdavsFileSystem(WebdavFileSystem):
    """Filesystem adapter for ``webdavs://`` URIs."""

    protocol = "webdavs"
