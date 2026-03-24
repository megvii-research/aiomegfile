"""Project-wide configuration loaded from environment variables and config file."""

from __future__ import annotations

import configparser
import os
import typing as T

from aiomegfile.utils.parse import parse_boolean, parse_quantity

DEFAULT_CONFIG_PATH = "~/.config/megfile/megfile.conf"
CONFIG_PATH = DEFAULT_CONFIG_PATH


class CaseSensitiveConfigParser(configparser.ConfigParser):
    """Config parser that preserves key case."""

    def optionxform(self, optionstr: str) -> str:
        """Return config option string unchanged.

        :param optionstr: Config option string.
        :return: Original option string.
        :rtype: str
        """
        return optionstr


def load_aiomegfile_config(section: str) -> T.Dict[str, str]:
    """Load one section from the main megfile config file.

    :param section: Config section name.
    :return: Mapping of option names to values.
    :rtype: T.Dict[str, str]
    """
    path = os.path.expanduser(CONFIG_PATH)
    if not os.path.isfile(path):
        return {}
    config = CaseSensitiveConfigParser()
    if os.path.exists(path):
        config.read(path)
    if not config.has_section(section):
        return {}
    return dict(config.items(section))


def _load_env_defaults_from_config() -> None:
    """Load ``[env]`` config entries into process environment defaults.

    Values already present in ``os.environ`` keep priority over config file
    values.

    :return: None
    :rtype: None
    """
    for key, value in load_aiomegfile_config("env").items():
        os.environ.setdefault(key.upper(), value)


_load_env_defaults_from_config()

DEFAULT_MAX_RETRY_TIMES = int(os.getenv("MEGFILE_MAX_RETRY_TIMES", default="10") or 10)
GLOBAL_MAX_WORKERS = int(os.getenv("MEGFILE_MAX_WORKERS", default="8") or 8)

DEFAULT_WRITER_BLOCK_AUTOSCALE = not os.getenv("MEGFILE_WRITER_BLOCK_SIZE")
writer_block_autoscale = os.getenv("MEGFILE_WRITER_BLOCK_AUTOSCALE")
if writer_block_autoscale is not None:
    DEFAULT_WRITER_BLOCK_AUTOSCALE = parse_boolean(writer_block_autoscale)

# Multi-upload in aws s3 has a maximum of 10,000 parts,
# so the maximum supported file size is MEGFILE_WRITER_BLOCK_SIZE * 10,000,
# the largest object that can be uploaded in a single PUT is 5 TB in aws s3.
WRITER_BLOCK_SIZE = parse_quantity(os.getenv("MEGFILE_WRITER_BLOCK_SIZE") or 8 * 2**20)
if WRITER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_WRITER_BLOCK_SIZE' must bigger than 0, got {WRITER_BLOCK_SIZE}"
    )
WRITER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("MEGFILE_WRITER_MAX_BUFFER_SIZE") or 128 * 2**20
)

# Reader configuration
READER_BLOCK_SIZE = parse_quantity(os.getenv("MEGFILE_READER_BLOCK_SIZE") or 8 * 2**20)
if READER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_READER_BLOCK_SIZE' must bigger than 0, got {READER_BLOCK_SIZE}"
    )
READER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("MEGFILE_READER_MAX_BUFFER_SIZE") or 128 * 2**20
)
READER_LAZY_PREFETCH = parse_boolean(
    os.getenv("MEGFILE_READER_LAZY_PREFETCH", default="false") or "false"
)

# Protocol-specific retry configuration
S3_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_S3_MAX_RETRY_TIMES", default=str(DEFAULT_MAX_RETRY_TIMES))
    or DEFAULT_MAX_RETRY_TIMES
)
HDFS_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_HDFS_MAX_RETRY_TIMES", default=str(DEFAULT_MAX_RETRY_TIMES))
    or DEFAULT_MAX_RETRY_TIMES
)
HTTP_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_HTTP_MAX_RETRY_TIMES", default=str(DEFAULT_MAX_RETRY_TIMES))
    or DEFAULT_MAX_RETRY_TIMES
)
SFTP_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_SFTP_MAX_RETRY_TIMES", default=str(DEFAULT_MAX_RETRY_TIMES))
    or DEFAULT_MAX_RETRY_TIMES
)
WEBDAV_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_WEBDAV_MAX_RETRY_TIMES", default=str(DEFAULT_MAX_RETRY_TIMES))
    or DEFAULT_MAX_RETRY_TIMES
)

DEFAULT_COPY_BUFFER_SIZE = 16 * 1024  # 16KB, same as shutil.copyfileobj
DEFAULT_HASH_BUFFER_SIZE = 4 * 1024  # 4KB for hash calculations
