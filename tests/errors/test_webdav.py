"""Tests for WebDAV dependency checks and cached client helpers."""

from types import SimpleNamespace

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


class _FakeWebdavClient:
    """Fake aiodav client used by cache behavior tests.

    :param hostname: WebDAV endpoint.
    :param login: Optional username.
    :param password: Optional password.
    :param token: Optional bearer token.
    :param timeout: Optional timeout.
    :param insecure: Optional insecure mode.
    """

    created_kwargs: list[dict] = []

    def __init__(
        self,
        hostname: str,
        login=None,
        password=None,
        token=None,
        timeout=None,
        insecure=None,
    ) -> None:
        """Capture constructor parameters for assertions."""
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


@pytest.fixture(autouse=True)
def _reset_webdav_cache():
    """Reset cached helper state between tests."""
    webdav_module.clear_webdav_client_cache()
    for cached_function in (
        webdav_errors_module._import_aiodav_exceptions,
        webdav_module.import_aiodav_client_class,
    ):
        cache_clear = getattr(cached_function, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()
    yield
    webdav_module.clear_webdav_client_cache()
    for cached_function in (
        webdav_errors_module._import_aiodav_exceptions,
        webdav_module.import_aiodav_client_class,
    ):
        cache_clear = getattr(cached_function, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()
    _FakeWebdavClient.created_kwargs.clear()


def test_load_webdav_token_from_command(monkeypatch):
    """Test token command output is decoded and stripped."""
    commands: list[list[str]] = []

    def _fake_check_output(args, stderr=None):
        _ = stderr
        commands.append(args)
        return b"token-from-command\n"

    monkeypatch.setattr(webdav_module.subprocess, "check_output", _fake_check_output)
    token = webdav_module.load_webdav_token_from_command("echo token")

    assert token == "token-from-command"
    assert commands == [["echo", "token"]]


def test_load_webdav_token_prefers_token_command(monkeypatch):
    """Test token resolution prioritizes ``WEBDAV_TOKEN_COMMAND`` over token."""
    monkeypatch.setenv(webdav_module.WEBDAV_TOKEN_ENV, "token-from-env")
    monkeypatch.setenv(webdav_module.WEBDAV_TOKEN_COMMAND_ENV, "echo command-token")

    called = {"command": None}

    def _fake_load_token_from_command(token_command: str) -> str:
        called["command"] = token_command
        return "token-from-command"

    monkeypatch.setattr(
        webdav_module,
        "load_webdav_token_from_command",
        _fake_load_token_from_command,
    )

    assert webdav_module.load_webdav_token() == "token-from-command"
    assert called["command"] == "echo command-token"


def test_load_webdav_timeout_and_insecure_from_env(monkeypatch):
    """Test timeout/insecure helpers parse environment values."""
    monkeypatch.setenv(webdav_module.WEBDAV_TIMEOUT_ENV, "15")
    monkeypatch.setenv(webdav_module.WEBDAV_INSECURE_ENV, "true")

    assert webdav_module.load_webdav_timeout() == 15.0
    assert webdav_module.load_webdav_insecure() is True

    monkeypatch.setenv(webdav_module.WEBDAV_TIMEOUT_ENV, "invalid")
    assert webdav_module.load_webdav_timeout() == webdav_module.WEBDAV_DEFAULT_TIMEOUT


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

    with pytest.raises(ModuleNotFoundError, match="aiomegfile\\[webdav\\]"):
        webdav_errors_module._ensure_aiodav()


def test_import_aiodav_client_class_checks_dependency(monkeypatch):
    """Test client import helper validates dependency before loading submodule."""
    calls = {"ensure": 0}

    def _fake_ensure_aiodav() -> None:
        calls["ensure"] += 1

    def _fake_import_module(module_name: str) -> SimpleNamespace:
        assert module_name == "aiodav.client"
        return SimpleNamespace(Client=_FakeWebdavClient)

    monkeypatch.setattr(webdav_module, "_ensure_aiodav", _fake_ensure_aiodav)
    monkeypatch.setattr(webdav_module.importlib, "import_module", _fake_import_module)

    assert webdav_module.import_aiodav_client_class() is _FakeWebdavClient
    assert calls["ensure"] == 1


async def test_get_webdav_client_cache_hit(monkeypatch):
    """Test same parameters return cached client instance."""
    _FakeWebdavClient.created_kwargs.clear()
    monkeypatch.setattr(
        webdav_module,
        "import_aiodav_client_class",
        lambda: _FakeWebdavClient,
    )

    client1 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )
    client2 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )

    assert client1 is client2
    assert len(_FakeWebdavClient.created_kwargs) == 1


async def test_get_webdav_client_recreate_when_cached_client_closed(monkeypatch):
    """Test closed cached client is replaced by a new one."""
    _FakeWebdavClient.created_kwargs.clear()
    monkeypatch.setattr(
        webdav_module,
        "import_aiodav_client_class",
        lambda: _FakeWebdavClient,
    )

    client1 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )
    client1.session.closed = True

    client2 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
    )

    assert client1 is not client2
    assert len(_FakeWebdavClient.created_kwargs) == 2


async def test_get_webdav_client_with_token_command(monkeypatch):
    """Test token command is applied and takes precedence over username/password."""
    _FakeWebdavClient.created_kwargs.clear()
    monkeypatch.setattr(
        webdav_module,
        "import_aiodav_client_class",
        lambda: _FakeWebdavClient,
    )

    tokens = iter(["token-1", "token-2"])
    monkeypatch.setattr(
        webdav_module,
        "load_webdav_token_from_command",
        lambda token_command: next(tokens),
    )

    client1 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token="token-from-arg",
        token_command="echo get-token",
    )
    client2 = webdav_module.get_webdav_client(
        "http://example.com",
        username="demo",
        password="secret",
        token="token-from-arg",
        token_command="echo get-token",
    )

    assert client1 is not client2
    assert _FakeWebdavClient.created_kwargs[0]["token"] == "token-1"
    assert _FakeWebdavClient.created_kwargs[1]["token"] == "token-2"
    assert _FakeWebdavClient.created_kwargs[0]["login"] is None
    assert _FakeWebdavClient.created_kwargs[0]["password"] is None


def test_get_webdav_client_missing_dependency(monkeypatch):
    """Test missing optional dependency is converted to install hint."""

    def _raise_import_error():
        raise ImportError("No module named aiodav")

    monkeypatch.setattr(
        webdav_module,
        "import_aiodav_client_class",
        _raise_import_error,
    )

    with pytest.raises(ModuleNotFoundError, match="aiomegfile\\[webdav\\]"):
        webdav_module.get_webdav_client("http://example.com")
