"""Tests for configuration loading."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import pytest

PROJECT_ENV_KEYS = {
    "MEGFILE_MAX_RETRY_TIMES",
    "AIOMEGFILE_MAX_RETRY_TIMES",
    "MEGFILE_MAX_WORKERS",
    "AIOMEGFILE_MAX_WORKERS",
    "MEGFILE_WRITER_BLOCK_SIZE",
    "AIOMEGFILE_WRITER_BLOCK_SIZE",
    "MEGFILE_WRITER_BLOCK_AUTOSCALE",
    "AIOMEGFILE_WRITER_BLOCK_AUTOSCALE",
    "MEGFILE_WRITER_MAX_BUFFER_SIZE",
    "AIOMEGFILE_WRITER_MAX_BUFFER_SIZE",
    "MEGFILE_READER_BLOCK_SIZE",
    "AIOMEGFILE_READER_BLOCK_SIZE",
    "MEGFILE_READER_MAX_BUFFER_SIZE",
    "AIOMEGFILE_READER_MAX_BUFFER_SIZE",
    "MEGFILE_READER_LAZY_PREFETCH",
    "AIOMEGFILE_READER_LAZY_PREFETCH",
    "MEGFILE_S3_MAX_RETRY_TIMES",
    "AIOMEGFILE_S3_MAX_RETRY_TIMES",
    "MEGFILE_HDFS_MAX_RETRY_TIMES",
    "AIOMEGFILE_HDFS_MAX_RETRY_TIMES",
    "MEGFILE_HTTP_MAX_RETRY_TIMES",
    "AIOMEGFILE_HTTP_MAX_RETRY_TIMES",
    "MEGFILE_SFTP_MAX_RETRY_TIMES",
    "AIOMEGFILE_SFTP_MAX_RETRY_TIMES",
    "MEGFILE_WEBDAV_MAX_RETRY_TIMES",
    "AIOMEGFILE_WEBDAV_MAX_RETRY_TIMES",
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


def test_config_accepts_legacy_aiomegfile_environment_overrides(monkeypatch):
    """Config module should accept legacy ``AIOMEGFILE_*`` overrides.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "AIOMEGFILE_MAX_RETRY_TIMES": "11",
            "AIOMEGFILE_HTTP_MAX_RETRY_TIMES": "13",
            "AIOMEGFILE_SFTP_MAX_RETRY_TIMES": "15",
            "AIOMEGFILE_WEBDAV_MAX_RETRY_TIMES": "17",
        },
    )

    assert module.DEFAULT_MAX_RETRY_TIMES == 11
    assert module.HTTP_MAX_RETRY_TIMES == 13
    assert module.SFTP_MAX_RETRY_TIMES == 15
    assert module.WEBDAV_MAX_RETRY_TIMES == 17


def test_config_prefers_megfile_names_over_legacy_names(monkeypatch):
    """New ``MEGFILE_*`` names should override legacy names.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: None
    :rtype: None
    """
    module = _load_config_module(
        monkeypatch,
        {
            "MEGFILE_MAX_RETRY_TIMES": "3",
            "AIOMEGFILE_MAX_RETRY_TIMES": "9",
            "MEGFILE_HTTP_MAX_RETRY_TIMES": "5",
            "AIOMEGFILE_HTTP_MAX_RETRY_TIMES": "7",
        },
    )

    assert module.DEFAULT_MAX_RETRY_TIMES == 3
    assert module.HTTP_MAX_RETRY_TIMES == 5


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
