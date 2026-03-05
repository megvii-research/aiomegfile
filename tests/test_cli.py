"""CLI tests."""

from __future__ import annotations

import asyncio
import configparser
import io
import sys

import click
import pytest
from click.testing import CliRunner

import aiomegfile  # noqa: F401
from aiomegfile.__version__ import __version__
from aiomegfile.cli import (
    PathType,
    _get_human_size,
    _run_async,
    _safe_makedirs,
    _tail_follow_content,
    cli,
)
from aiomegfile.interfaces import FileEntry, StatResult
from aiomegfile.utils.async_tools import maybe_await


def test_cli_version() -> None:
    """Test the version command prints the package version.

    :return: None
    :rtype: None
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_ls_cat_head_tail(tmp_path) -> None:
    """Test basic read-only CLI commands on local files.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    sample_path = tmp_path / "sample.txt"
    sample_path.write_text("line1\nline2\nline3")

    runner = CliRunner()

    result = runner.invoke(cli, ["ls", str(tmp_path)])
    assert result.exit_code == 0
    assert "sample.txt" in result.output

    result = runner.invoke(cli, ["cat", str(sample_path)])
    assert result.exit_code == 0
    assert "line1" in result.output

    result = runner.invoke(cli, ["head", "-n", "1", str(sample_path)])
    assert result.exit_code == 0
    assert result.output.strip() == "line1"

    result = runner.invoke(cli, ["tail", "-n", "1", str(sample_path)])
    assert result.exit_code == 0
    assert result.output.strip() == "line3"


def test_cli_cp_mv_rm(tmp_path) -> None:
    """Test copy, move, and remove commands.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    src_path = tmp_path / "src.txt"
    src_path.write_text("data")

    dst_dir = tmp_path / "dst"
    dst_dir.mkdir()

    runner = CliRunner()

    result = runner.invoke(cli, ["cp", str(src_path), str(dst_dir)])
    assert result.exit_code == 0
    copied_path = dst_dir / "src.txt"
    assert copied_path.read_text() == "data"

    moved_path = tmp_path / "moved.txt"
    result = runner.invoke(cli, ["mv", str(copied_path), str(moved_path)])
    assert result.exit_code == 0
    assert moved_path.read_text() == "data"
    assert not copied_path.exists()

    result = runner.invoke(cli, ["rm", str(moved_path)])
    assert result.exit_code == 0
    assert not moved_path.exists()


def test_get_human_size_formats_units() -> None:
    """_get_human_size should format byte sizes.

    :return: None
    :rtype: None
    """
    assert _get_human_size(0) == "0B"
    assert _get_human_size(512) == "512B"
    assert _get_human_size(1024) == "1.0KB"
    assert _get_human_size(1024 * 1024) == "1.0MB"


async def test_run_async_raises_in_active_loop() -> None:
    """_run_async should raise when event loop is running.

    :return: None
    :rtype: None
    """
    coro = asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="active event loop"):
            _run_async(coro)
    finally:
        coro.close()


async def test_maybe_await_handles_values() -> None:
    """maybe_await should return values and await coroutines.

    :return: None
    :rtype: None
    """
    assert await maybe_await("value") == "value"
    assert await maybe_await(asyncio.sleep(0, result="ok")) == "ok"


async def test_tail_follow_content_outputs_and_returns_offset(tmp_path, monkeypatch):
    """_tail_follow_content should emit data and return updated offset.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    data = b"line1\nline2\n"
    path = tmp_path / "follow.txt"
    path.write_bytes(data)

    output = io.BytesIO()
    monkeypatch.setattr(click, "get_binary_stream", lambda _: output)

    offset = await _tail_follow_content(str(path), 0)
    assert offset == path.stat().st_size
    assert output.getvalue() == data


def test_safe_makedirs_creates_nested(tmp_path) -> None:
    """_safe_makedirs should create nested directories.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    target = tmp_path / "a" / "b"
    _safe_makedirs(str(target))
    assert target.is_dir()

    _safe_makedirs("")
    _safe_makedirs(".")
    _safe_makedirs("/")


def test_path_type_shell_complete_protocols() -> None:
    """PathType should suggest protocol prefixes.

    :return: None
    :rtype: None
    """
    items = PathType().shell_complete(None, None, "")
    values = {item.value for item in items}
    assert "file://" in values


def test_path_type_shell_complete_profiles(monkeypatch) -> None:
    """PathType should include s3 profile prefixes.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """

    def fake_profiles() -> list[str]:
        """Return fake profile names.

        :return: Profile names.
        :rtype: list[str]
        """
        return ["default", "demo"]

    monkeypatch.setattr("aiomegfile.cli._get_s3_profiles", fake_profiles)
    items = PathType().shell_complete(None, None, "")
    values = {item.value for item in items}
    assert "s3+demo://" in values
    assert "s3+default://" not in values


def test_cli_config_alias_writes_config(tmp_path) -> None:
    """CLI config alias should write config entries.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    runner = CliRunner()
    config_path = tmp_path / "megfile.conf"

    result = runner.invoke(
        cli, ["config", "alias", "-p", str(config_path), "data", "s3"]
    )
    assert result.exit_code == 0

    parser = configparser.ConfigParser()
    parser.read(config_path)
    assert parser.has_section("alias")
    assert parser.get("alias", "data") == "s3"

    result = runner.invoke(
        cli, ["config", "alias", "-p", str(config_path), "data", "s3", "--no-cover"]
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, NameError)
    assert "alias-name has been used" in str(result.exception)


def test_cli_config_env_writes_config(tmp_path) -> None:
    """CLI config env should write config entries.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    runner = CliRunner()
    config_path = tmp_path / "megfile.conf"

    result = runner.invoke(cli, ["config", "env", "-p", str(config_path), "FOO=bar"])
    assert result.exit_code == 0

    parser = configparser.ConfigParser()
    parser.read(config_path)
    assert parser.has_section("env")
    assert parser.get("env", "FOO") == "bar"

    result = runner.invoke(
        cli, ["config", "env", "-p", str(config_path), "FOO=baz", "--no-cover"]
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, NameError)


def test_cli_config_hdfs_writes_config(tmp_path) -> None:
    """CLI config hdfs should write config entries.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    runner = CliRunner()
    config_path = tmp_path / "hdfscli.cfg"

    result = runner.invoke(
        cli,
        [
            "config",
            "hdfs",
            "--path",
            str(config_path),
            "http://localhost:9870",
            "--profile-name",
            "demo",
            "--user",
            "alice",
            "--root",
            "/data",
            "--token",
            "token123",
            "--timeout",
            "5",
        ],
    )
    assert result.exit_code == 0

    parser = configparser.ConfigParser()
    parser.read(config_path)
    assert parser.has_section("global")
    assert parser.get("global", "default.alias") == "default"
    assert parser.has_section("demo.alias")
    assert parser.get("demo.alias", "url") == "http://localhost:9870"
    assert parser.get("demo.alias", "user") == "alice"
    assert parser.get("demo.alias", "root") == "/data"
    assert parser.get("demo.alias", "token") == "token123"
    assert parser.get("demo.alias", "timeout") == "5"

    result = runner.invoke(
        cli,
        [
            "config",
            "hdfs",
            "--path",
            str(config_path),
            "http://localhost:9870",
            "--profile-name",
            "demo",
            "--no-cover",
        ],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, NameError)


def test_path_type_shell_complete_local_paths(tmp_path) -> None:
    """PathType should complete local filesystem paths.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    file_path = tmp_path / "sample.txt"
    file_path.write_text("x", encoding="utf-8")
    dir_path = tmp_path / "sample_dir"
    dir_path.mkdir()

    items = PathType().shell_complete(None, None, str(tmp_path / "sam"))
    values = {item.value for item in items}
    assert str(file_path) in values
    assert str(dir_path) + "/" in values


def test_path_type_shell_complete_file_scheme(tmp_path) -> None:
    """PathType should complete file:// paths.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    file_path = tmp_path / "sample.txt"
    file_path.write_text("x", encoding="utf-8")

    prefix = f"file://{tmp_path}/sam"
    items = PathType().shell_complete(None, None, prefix)
    values = {item.value for item in items}
    assert f"file://{file_path}" in values


def test_path_type_shell_complete_remote(monkeypatch) -> None:
    """PathType should complete remote paths using glob_stat.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """

    async def fake_glob_stat(_pattern: str, recursive: bool = True):
        """Yield fake entries for completion.

        :param _pattern: Input pattern.
        :param recursive: Whether to recurse.
        :return: Async iterator of FileEntry.
        :rtype: typing.AsyncIterator[FileEntry]
        """
        yield FileEntry(
            name="file.txt",
            path="s3://bucket/file.txt",
            stat=StatResult(),
        )
        yield FileEntry(
            name="dir",
            path="s3://bucket/dir",
            stat=StatResult(isdir=True),
        )

    monkeypatch.setattr("aiomegfile.cli._glob_stat", fake_glob_stat)
    items = PathType().shell_complete(None, None, "s3://bucket/pa")
    values = {item.value for item in items}
    assert "s3://bucket/file.txt" in values
    assert "s3://bucket/dir/" in values


def test_cli_size_mtime_stat_and_to(tmp_path) -> None:
    """CLI size/mtime/stat and to commands should work on local files.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    runner = CliRunner()
    file_path = tmp_path / "data.txt"
    file_path.write_text("hello", encoding="utf-8")

    result = runner.invoke(cli, ["size", str(file_path)])
    assert result.exit_code == 0
    assert result.output.strip() == str(file_path.stat().st_size)

    result = runner.invoke(cli, ["mtime", str(file_path)])
    assert result.exit_code == 0
    assert float(result.output.strip()) > 0

    result = runner.invoke(cli, ["stat", str(file_path)])
    assert result.exit_code == 0
    assert "st_size" in result.output

    dst_path = tmp_path / "stdin.txt"
    result = runner.invoke(cli, ["to", str(dst_path)], input="payload")
    assert result.exit_code == 0
    assert dst_path.read_text(encoding="utf-8") == "payload"


def test_cli_edit_invokes_editor(tmp_path, monkeypatch) -> None:
    """CLI edit should invoke the editor with the cached path.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    target_path = tmp_path / "edit.txt"
    target_path.write_text("data", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_check_call(cmd: list[str]) -> None:
        """Record editor invocation.

        :param cmd: Editor command list.
        :return: None
        :rtype: None
        """
        calls.append(cmd)

    monkeypatch.setattr("aiomegfile.cli.subprocess.check_call", fake_check_call)
    runner = CliRunner()
    result = runner.invoke(cli, ["edit", "-e", "echo", str(target_path)])
    assert result.exit_code == 0
    assert calls
    assert calls[0][0] == "echo"
    assert calls[0][-1] == str(target_path)


def test_cli_config_s3_writes_and_updates(tmp_path) -> None:
    """CLI config s3 should write and update credentials file.

    :param tmp_path: Pytest temporary path fixture.
    :return: None
    :rtype: None
    """
    runner = CliRunner()
    config_path = tmp_path / "credentials"

    result = runner.invoke(
        cli,
        [
            "config",
            "s3",
            "--path",
            str(config_path),
            "--profile-name",
            "demo",
            "key",
            "secret",
            "--endpoint-url",
            "http://localhost",
            "--addressing-style",
            "path",
        ],
    )
    assert result.exit_code == 0
    text = config_path.read_text(encoding="utf-8")
    assert "[demo]" in text
    assert "aws_access_key_id = key" in text
    assert "endpoint_url = http://localhost" in text

    result = runner.invoke(
        cli,
        [
            "config",
            "s3",
            "--path",
            str(config_path),
            "--profile-name",
            "demo",
            "newkey",
            "newsecret",
        ],
    )
    assert result.exit_code == 0
    updated = config_path.read_text(encoding="utf-8")
    assert "aws_access_key_id = newkey" in updated

    result = runner.invoke(
        cli,
        [
            "config",
            "s3",
            "--path",
            str(config_path),
            "--profile-name",
            "demo",
            "key2",
            "secret2",
            "--no-cover",
        ],
    )
    assert result.exit_code != 0


def test_cli_completion_scripts_write_to_home(tmp_path, monkeypatch) -> None:
    """Completion commands should write script snippets.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["aiomegfile"])

    bashrc = tmp_path / ".bashrc"
    bashrc.write_text("", encoding="utf-8")
    zshrc = tmp_path / ".zshrc"
    zshrc.write_text("", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["completion", "bash"])
    assert result.exit_code == 0
    assert "COMPLETE=bash_source" in bashrc.read_text(encoding="utf-8")

    result = runner.invoke(cli, ["completion", "zsh"])
    assert result.exit_code == 0
    assert "COMPLETE=zsh_source" in zshrc.read_text(encoding="utf-8")

    result = runner.invoke(cli, ["completion", "fish"])
    assert result.exit_code == 0
    fish_path = tmp_path / ".config" / "fish" / "completions" / "aiomegfile.fish"
    assert fish_path.exists()
