"""Tests for WebDAV dependency checks and cached client helpers."""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType

import pytest

from aiomegfile.errors import webdav as webdav_errors_module
from aiomegfile.filesystem import webdav as webdav_module


class _FakeSession:
    """Simple fake aiohttp session state holder.

    :param closed: Whether the session is closed.
    """

    def __init__(self, closed: bool = False) -> None:
        """Initialize fake session state.

        :param closed: Whether the session is closed.
        """
        self.closed = closed
        self.close_calls = 0

    async def close(self) -> None:
        """Mark fake session as closed.

        :return: ``None``.
        :rtype: None
        """
        self.close_calls += 1
        self.closed = True


class _FakeWebdavClient:
    """Fake aiodav client used by cache behavior tests.

    :param hostname: WebDAV endpoint.
    :param login: Optional username.
    :param password: Optional password.
    :param token: Optional bearer token.
    :param timeout: Optional timeout.
    :param insecure: Optional insecure mode.
    """

    created_kwargs: list[dict[str, object]] = []

    def __init__(
        self,
        hostname: str,
        login: object = None,
        password: object = None,
        token: object = None,
        timeout: object = None,
        insecure: object = None,
    ) -> None:
        """Capture constructor parameters for assertions.

        :param hostname: WebDAV endpoint.
        :param login: Optional username.
        :param password: Optional password.
        :param token: Optional bearer token.
        :param timeout: Optional timeout.
        :param insecure: Optional insecure mode.
        """
        kwargs = {
            "hostname": hostname,
            "login": login,
            "password": password,
            "token": token,
            "timeout": timeout,
            "insecure": insecure,
        }
        type(self).created_kwargs.append(kwargs)
        self.kwargs = kwargs
        self.session = _FakeSession(closed=False)


def _install_fake_aiodav_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install fake ``aiodav.client`` module for import-based tests.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: ``None``.
    :rtype: None
    """
    aiodav_module = ModuleType("aiodav")
    aiodav_module.__path__ = []  # type: ignore[attr-defined]
    client_module = ModuleType("aiodav.client")
    client_module.Client = _FakeWebdavClient  # type: ignore[attr-defined]
    aiodav_module.client = client_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "aiodav", aiodav_module)
    monkeypatch.setitem(sys.modules, "aiodav.client", client_module)


def _clear_webdav_client_cache():
    from aiomegfile.filesystem.webdav import (
        _WEBDAV_CLIENT_CACHE,
        _WEBDAV_CLIENT_CACHE_LOCK,
    )

    with _WEBDAV_CLIENT_CACHE_LOCK:
        _WEBDAV_CLIENT_CACHE.clear()


@pytest.fixture(autouse=True)
def _reset_webdav_cache():
    """Reset cached helper state between tests."""
    _clear_webdav_client_cache()
    yield
    _clear_webdav_client_cache()
    _FakeWebdavClient.created_kwargs.clear()


def test__load_webdav_token_from_command(monkeypatch):
    """Test token command output is decoded and stripped."""
    commands: list[list[str]] = []

    def _fake_check_output(args, stderr=None):
        _ = stderr
        commands.append(args)
        return b"token-from-command\n"

    monkeypatch.setattr(webdav_module.subprocess, "check_output", _fake_check_output)

    token = webdav_module._load_webdav_token_from_command("echo token")

    assert token == "token-from-command"
    assert commands == [["echo", "token"]]


def test__load_webdav_token_prefers_argument_then_env_then_command(monkeypatch):
    """Test token resolution order for WebDAV authentication."""
    monkeypatch.setenv(webdav_module.WEBDAV_TOKEN_ENV, "token-from-env")
    monkeypatch.setenv(webdav_module.WEBDAV_TOKEN_COMMAND_ENV, "echo command-token")

    called = {"command": None, "count": 0}

    def _fake_load_token_from_command(token_command: str) -> str:
        called["command"] = token_command
        called["count"] += 1
        return "token-from-command"

    monkeypatch.setattr(
        webdav_module,
        "_load_webdav_token_from_command",
        _fake_load_token_from_command,
    )

    assert webdav_module._load_webdav_token(token="token-from-arg") == "token-from-arg"
    assert called["count"] == 0

    assert webdav_module._load_webdav_token() == "token-from-env"
    assert called["count"] == 0

    monkeypatch.delenv(webdav_module.WEBDAV_TOKEN_ENV)
    assert webdav_module._load_webdav_token() == "token-from-command"
    assert called["command"] == "echo command-token"
    assert called["count"] == 1


def test__load_webdav_timeout_and_insecure_from_env(monkeypatch):
    """Test timeout and insecure helpers parse environment values."""
    monkeypatch.setenv(webdav_module.WEBDAV_TIMEOUT_ENV, "15")
    monkeypatch.setenv(webdav_module.WEBDAV_INSECURE_ENV, "true")

    assert webdav_module._load_webdav_timeout() == 15.0
    assert webdav_module._load_webdav_insecure() is True

    monkeypatch.setenv(webdav_module.WEBDAV_TIMEOUT_ENV, "invalid")
    assert webdav_module._load_webdav_timeout() == webdav_module.WEBDAV_DEFAULT_TIMEOUT


def test_ensure_aiodav_missing_dependency(monkeypatch):
    """Test dependency helper raises install hint when ``aiodav`` is missing."""

    def _fake_import_module(module_name: str):
        if module_name == "aiodav":
            raise ImportError("No module named aiodav")
        return object()

    monkeypatch.setattr(
        webdav_errors_module.importlib,
        "import_module",
        _fake_import_module,
    )

    with pytest.raises(ImportError, match="aiomegfile\\[webdav\\]"):
        webdav_errors_module._ensure_aiodav()


async def test_get_webdav_client_cache_hit(monkeypatch):
    """Test same parameters return cached client instance."""
    _install_fake_aiodav_client(monkeypatch)
    monkeypatch.setattr(webdav_module, "_ensure_aiodav", lambda: None)

    client1 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )
    client2 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )

    assert client1 is client2
    assert len(_FakeWebdavClient.created_kwargs) == 1


async def test_get_webdav_client_recreate_when_cached_client_closed(monkeypatch):
    """Test closed cached client is replaced by a new one."""
    _install_fake_aiodav_client(monkeypatch)
    monkeypatch.setattr(webdav_module, "_ensure_aiodav", lambda: None)

    client1 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )
    client1.session.closed = True

    client2 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )

    assert client1 is not client2
    assert len(_FakeWebdavClient.created_kwargs) == 2


async def test_finalize_webdav_client_session_uses_bound_loop() -> None:
    """Test finalizer closes session on the original running loop."""
    loop = asyncio.get_running_loop()
    session = _FakeSession(closed=False)

    webdav_module._finalize_webdav_client_session(loop, session)
    await asyncio.sleep(0)

    assert session.closed is True
    assert session.close_calls == 1


def test_finalize_webdav_client_session_skips_closed_loop() -> None:
    """Test finalizer quietly skips cleanup when loop is already closed."""
    loop = asyncio.new_event_loop()
    session = _FakeSession(closed=False)

    loop.close()
    webdav_module._finalize_webdav_client_session(loop, session)

    assert session.closed is False
    assert session.close_calls == 0


async def test_get_webdav_client_prefers_explicit_token_over_command(monkeypatch):
    """Test explicit token bypasses token command loading."""
    _install_fake_aiodav_client(monkeypatch)
    monkeypatch.setattr(webdav_module, "_ensure_aiodav", lambda: None)

    called = {"count": 0}

    def _fake_load_token_from_command(token_command: str) -> str:
        _ = token_command
        called["count"] += 1
        return "token-from-command"

    monkeypatch.setattr(
        webdav_module,
        "_load_webdav_token_from_command",
        _fake_load_token_from_command,
    )

    client1 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token="token-from-arg",
        token_command="echo get-token",
    )
    client2 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token="token-from-arg",
        token_command="echo get-token",
    )

    assert client1 is client2
    assert _FakeWebdavClient.created_kwargs[0]["token"] == "token-from-arg"
    assert _FakeWebdavClient.created_kwargs[0]["login"] is None
    assert _FakeWebdavClient.created_kwargs[0]["password"] is None
    assert called["count"] == 0


async def test_get_webdav_client_falls_back_to_token_command(monkeypatch):
    """Test token command is used when no direct token is available."""
    _install_fake_aiodav_client(monkeypatch)
    monkeypatch.setattr(webdav_module, "_ensure_aiodav", lambda: None)

    tokens = iter(["token-1", "token-2"])
    monkeypatch.setattr(
        webdav_module,
        "_load_webdav_token_from_command",
        lambda token_command: next(tokens),
    )

    client1 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token_command="echo get-token",
    )
    client2 = await webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token_command="echo get-token",
    )

    assert client1 is not client2
    assert _FakeWebdavClient.created_kwargs[0]["token"] == "token-1"
    assert _FakeWebdavClient.created_kwargs[1]["token"] == "token-2"
    assert _FakeWebdavClient.created_kwargs[0]["login"] is None
    assert _FakeWebdavClient.created_kwargs[0]["password"] is None


async def test_get_webdav_client_missing_dependency(monkeypatch):
    """Test missing optional dependency is surfaced from helper setup."""

    def _raise_missing_dependency() -> None:
        raise ImportError("Failed to import aiodav")

    monkeypatch.setattr(webdav_module, "_ensure_aiodav", _raise_missing_dependency)

    with pytest.raises(ImportError, match="Failed to import aiodav"):
        await webdav_module.get_webdav_client("http://example.com")
