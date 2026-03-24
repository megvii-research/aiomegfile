"""Tests for configuration loading."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

PROJECT_ENV_KEYS = {
    "MEGFILE_MAX_RETRY_TIMES",
    "MEGFILE_MAX_WORKERS",
    "MEGFILE_WRITER_BLOCK_SIZE",
    "MEGFILE_WRITER_BLOCK_AUTOSCALE",
    "MEGFILE_WRITER_MAX_BUFFER_SIZE",
    "MEGFILE_READER_BLOCK_SIZE",
    "MEGFILE_READER_MAX_BUFFER_SIZE",
    "MEGFILE_READER_LAZY_PREFETCH",
    "MEGFILE_S3_MAX_RETRY_TIMES",
    "MEGFILE_HDFS_MAX_RETRY_TIMES",
    "MEGFILE_HTTP_MAX_RETRY_TIMES",
    "MEGFILE_SFTP_MAX_RETRY_TIMES",
    "MEGFILE_WEBDAV_MAX_RETRY_TIMES",
}


def _load_config_module(monkeypatch, env: dict[str, str | None]):
    """Load a fresh config module with isolated environment overrides.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param env: Environment overrides to apply before loading.
    :return: Loaded config module object.
    :rtype: types.ModuleType
    """
    for key in PROJECT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    config_path = Path(__file__).resolve().parents[1] / "aiomegfile" / "config.py"
    module_name = f"aiomegfile._config_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, config_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load config module spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_uses_megfile_environment_overrides(monkeypatch):
    """Config module should use ``MEGFILE_*`` overrides when provided.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "MEGFILE_MAX_RETRY_TIMES": "3",
            "MEGFILE_MAX_WORKERS": "12",
            "MEGFILE_WRITER_BLOCK_SIZE": "4Mi",
            "MEGFILE_WRITER_MAX_BUFFER_SIZE": "16Mi",
            "MEGFILE_READER_BLOCK_SIZE": "2Mi",
            "MEGFILE_READER_MAX_BUFFER_SIZE": "8Mi",
            "MEGFILE_READER_LAZY_PREFETCH": "true",
            "MEGFILE_S3_MAX_RETRY_TIMES": "9",
            "MEGFILE_HDFS_MAX_RETRY_TIMES": "7",
            "MEGFILE_HTTP_MAX_RETRY_TIMES": "5",
            "MEGFILE_SFTP_MAX_RETRY_TIMES": "4",
            "MEGFILE_WEBDAV_MAX_RETRY_TIMES": "6",
            "MEGFILE_WRITER_BLOCK_AUTOSCALE": "false",
        },
    )

    assert module.DEFAULT_MAX_RETRY_TIMES == 3
    assert module.GLOBAL_MAX_WORKERS == 12
    assert module.WRITER_BLOCK_SIZE == 4 * 1024 * 1024
    assert module.WRITER_MAX_BUFFER_SIZE == 16 * 1024 * 1024
    assert module.READER_BLOCK_SIZE == 2 * 1024 * 1024
    assert module.READER_MAX_BUFFER_SIZE == 8 * 1024 * 1024
    assert module.READER_LAZY_PREFETCH is True
    assert module.S3_MAX_RETRY_TIMES == 9
    assert module.HDFS_MAX_RETRY_TIMES == 7
    assert module.HTTP_MAX_RETRY_TIMES == 5
    assert module.SFTP_MAX_RETRY_TIMES == 4
    assert module.WEBDAV_MAX_RETRY_TIMES == 6
    assert module.DEFAULT_WRITER_BLOCK_AUTOSCALE is False


def test_config_uses_megfile_retry_overrides_without_global_override(monkeypatch):
    """Protocol retry limits should work without overriding the global default.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "MEGFILE_MAX_RETRY_TIMES": "11",
            "MEGFILE_HTTP_MAX_RETRY_TIMES": "13",
            "MEGFILE_SFTP_MAX_RETRY_TIMES": "15",
            "MEGFILE_WEBDAV_MAX_RETRY_TIMES": "17",
        },
    )

    assert module.DEFAULT_MAX_RETRY_TIMES == 11
    assert module.HTTP_MAX_RETRY_TIMES == 13
    assert module.SFTP_MAX_RETRY_TIMES == 15
    assert module.WEBDAV_MAX_RETRY_TIMES == 17


def test_config_defaults_apply_when_retry_overrides_are_absent(monkeypatch):
    """Protocol retry limits should fall back to the global retry default.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "MEGFILE_MAX_RETRY_TIMES": "3",
        },
    )

    assert module.DEFAULT_MAX_RETRY_TIMES == 3
    assert module.S3_MAX_RETRY_TIMES == 3
    assert module.HDFS_MAX_RETRY_TIMES == 3
    assert module.HTTP_MAX_RETRY_TIMES == 3
    assert module.SFTP_MAX_RETRY_TIMES == 3
    assert module.WEBDAV_MAX_RETRY_TIMES == 3


def test_config_autoscale_true_without_writer_block_size(monkeypatch):
    """Autoscale should be enabled when env var is true.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "MEGFILE_WRITER_BLOCK_SIZE": None,
            "MEGFILE_WRITER_BLOCK_AUTOSCALE": "true",
        },
    )

    assert module.DEFAULT_WRITER_BLOCK_AUTOSCALE is True


def test_config_writer_block_size_zero_raises(monkeypatch):
    """Writer block size must be greater than zero.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    with pytest.raises(ValueError, match="WRITER_BLOCK_SIZE"):
        _load_config_module(
            monkeypatch,
            {
                "MEGFILE_WRITER_BLOCK_SIZE": "0",
            },
        )


def test_config_reader_block_size_negative_raises(monkeypatch):
    """Reader block size must be greater than zero.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    with pytest.raises(ValueError, match="READER_BLOCK_SIZE"):
        _load_config_module(
            monkeypatch,
            {
                "MEGFILE_READER_BLOCK_SIZE": "-1",
            },
        )


def test_config_file_env_applies_before_constants_on_package_import() -> None:
    """Config file ``[env]`` values should affect exported constants on import.

    :return: None
    :rtype: None
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        home_path = Path(temp_dir)
        config_path = home_path / ".config" / "megfile" / "megfile.conf"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            "[env]\nMEGFILE_MAX_WORKERS = 16\nMEGFILE_MAX_RETRY_TIMES = 5\n",
            encoding="utf-8",
        )

        env = os.environ.copy()
        env["HOME"] = temp_dir
        env.pop("MEGFILE_MAX_WORKERS", None)
        env.pop("MEGFILE_MAX_RETRY_TIMES", None)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import aiomegfile.config as c; "
                    "print(c.GLOBAL_MAX_WORKERS, c.DEFAULT_MAX_RETRY_TIMES)"
                ),
            ],
            capture_output=True,
            check=True,
            env=env,
            text=True,
        )

        assert result.stdout.strip() == "16 5"
