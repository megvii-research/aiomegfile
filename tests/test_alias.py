"""Tests for alias handling."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

from aiomegfile.interfaces import BaseFileSystem, get_filesystem_by_uri
from aiomegfile.smart_path import SmartPath
from aiomegfile.utils.path import split_uri


@pytest.fixture
def filesystem_registry_snapshot():
    """Snapshot and restore filesystem registry.

    :return: Snapshot of the registry.
    :rtype: dict[str, type[BaseFileSystem]]
    """
    from aiomegfile.interfaces import FILE_SYSTEMS

    snapshot = dict(FILE_SYSTEMS)
    yield snapshot
    FILE_SYSTEMS.clear()
    FILE_SYSTEMS.update(snapshot)


def _register_dummy_filesystem() -> type[BaseFileSystem]:
    """Register a dummy filesystem for alias tests.

    :return: Dummy filesystem class.
    :rtype: type[BaseFileSystem]
    """

    class DummyFileSystem(BaseFileSystem):
        protocol = "dummy"

        def same_endpoint(self, other_filesystem: BaseFileSystem) -> bool:
            return isinstance(other_filesystem, DummyFileSystem)

        def parse_uri(self, uri: str) -> str:
            _, path, _ = split_uri(uri)
            return path

        def build_uri(self, path: str) -> str:
            return super().build_uri(path)

        @classmethod
        def from_uri(cls, uri: str) -> "DummyFileSystem":
            return cls()

    return DummyFileSystem


def _write_alias_config(path: Path, mapping: dict[str, str]) -> None:
    """Write alias mapping to a config file.

    :param path: Config file path.
    :param mapping: Alias mapping.
    :return: None
    :rtype: None
    """
    parser = configparser.ConfigParser()
    parser["alias"] = mapping
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        parser.write(handle)


def test_alias_resolves_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filesystem_registry_snapshot
) -> None:
    """Alias with prefix should be applied for parsing and rendering.

    :param tmp_path: Temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param filesystem_registry_snapshot: Filesystem registry snapshot fixture.
    :return: None
    :rtype: None
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    _register_dummy_filesystem()

    config_path = tmp_path / ".config" / "megfile" / "megfile.conf"
    _write_alias_config(config_path, {"data": "dummy://bucket/prefix/"})

    path = SmartPath("data://dir/file.txt")
    assert path.filesystem.protocol == "dummy"
    assert path._path == "bucket/prefix/dir/file.txt"
    assert str(path) == "data://dir/file.txt"
    assert path.root == "data://"
    assert path.parts[0] == "data://"
    assert str(path.parent) == "data://dir"

    filesystem = get_filesystem_by_uri("data://dir/file.txt")
    assert filesystem.build_uri("bucket/prefix/dir/file.txt") == "data://dir/file.txt"
    assert filesystem.build_uri("bucket/other") == "dummy://bucket/other"

    direct_fs = type(filesystem).from_uri("data://dir/file.txt")
    assert direct_fs.build_uri("bucket/prefix/dir/file.txt") == "data://dir/file.txt"


def test_alias_resolves_protocol_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, filesystem_registry_snapshot
) -> None:
    """Alias without prefix should map protocol only.

    :param tmp_path: Temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param filesystem_registry_snapshot: Filesystem registry snapshot fixture.
    :return: None
    :rtype: None
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    _register_dummy_filesystem()

    config_path = tmp_path / ".config" / "megfile" / "megfile.conf"
    _write_alias_config(config_path, {"short": "dummy"})

    path = SmartPath("short://bucket/key")
    assert path.filesystem.protocol == "dummy"
    assert path._path == "bucket/key"
    assert str(path) == "short://bucket/key"
