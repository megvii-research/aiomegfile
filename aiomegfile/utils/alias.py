"""Alias configuration utilities."""

from __future__ import annotations

import configparser
import os
from dataclasses import dataclass

from aiomegfile.utils.path import PathLike, fspath

CONFIG_PATH = "~/.config/megfile/megfile.conf"
LEGACY_ALIASES_CONFIG = "~/.config/megfile/aliases.conf"


class CaseSensitiveConfigParser(configparser.ConfigParser):
    """Config parser that preserves option case."""

    def optionxform(self, optionstr: str) -> str:
        """Return option string unchanged.

        :param optionstr: Option string.
        :return: Option string without lowercasing.
        :rtype: str
        """
        return optionstr


@dataclass(frozen=True)
class AliasInfo:
    """Alias mapping metadata.

    :param alias: Alias protocol name.
    :param protocol: Target protocol name.
    :param prefix: Optional prefix to prepend before paths.
    """

    alias: str
    protocol: str
    prefix: str = ""

    def unaliased_prefix(self) -> str:
        """Return the unaliased protocol prefix.

        :return: Unaliased protocol prefix string.
        :rtype: str
        """
        return f"{self.protocol}://{self.prefix}"


def _split_protocol(uri: str) -> tuple[str, str]:
    """Split URI into protocol and remainder.

    :param uri: URI string.
    :return: Tuple of protocol and remainder.
    :rtype: tuple[str, str]
    """
    if "://" in uri:
        protocol, rest = uri.split("://", 1)
        return protocol, rest
    return "file", uri


def _load_legacy_aliases(path: str) -> dict[str, AliasInfo]:
    """Load legacy alias mappings from ini-style files.

    :param path: Legacy aliases config path.
    :return: Mapping of alias names to AliasInfo.
    :rtype: dict[str, AliasInfo]
    """
    configs: dict[str, AliasInfo] = {}
    config_path = os.path.expanduser(path)
    if not os.path.isfile(config_path):
        return configs
    parser = CaseSensitiveConfigParser()
    parser.read(config_path)
    for section in parser.sections():
        values = dict(parser.items(section))
        protocol = values.get("protocol")
        if not protocol:
            continue
        prefix = values.get("prefix", "")
        configs[section] = AliasInfo(alias=section, protocol=protocol, prefix=prefix)
    return configs


def _load_alias_section(path: str) -> dict[str, AliasInfo]:
    """Load alias mappings from megfile-style config files.

    :param path: Config file path.
    :return: Mapping of alias names to AliasInfo.
    :rtype: dict[str, AliasInfo]
    """
    config_path = os.path.expanduser(path)
    if not os.path.isfile(config_path):
        return {}
    parser = CaseSensitiveConfigParser()
    parser.read(config_path)
    if not parser.has_section("alias"):
        return {}
    aliases: dict[str, AliasInfo] = {}
    for name, protocol_or_path in parser.items("alias"):
        if "://" in protocol_or_path:
            protocol, prefix = protocol_or_path.split("://", maxsplit=1)
            aliases[name] = AliasInfo(alias=name, protocol=protocol, prefix=prefix)
        else:
            aliases[name] = AliasInfo(alias=name, protocol=protocol_or_path, prefix="")
    return aliases


def load_aliases_config() -> dict[str, AliasInfo]:
    """Load alias configuration from all supported locations.

    :return: Mapping of alias names to AliasInfo.
    :rtype: dict[str, AliasInfo]
    """
    configs = _load_legacy_aliases(LEGACY_ALIASES_CONFIG)
    configs.update(_load_alias_section(CONFIG_PATH))
    return configs


def resolve_alias(path: PathLike) -> tuple[str, AliasInfo | None]:
    """Resolve alias protocol to its target protocol and prefix.

    :param path: Input path or URI.
    :return: Tuple of unaliased URI and AliasInfo (if any).
    :rtype: tuple[str, AliasInfo | None]
    """
    path_str = fspath(path)
    protocol, remainder = _split_protocol(path_str)
    aliases = load_aliases_config()
    alias_info = aliases.get(protocol)
    if alias_info is None:
        return path_str, None
    unaliased_uri = f"{alias_info.protocol}://{alias_info.prefix}{remainder}"
    return unaliased_uri, alias_info


def apply_alias(uri: str, alias_info: AliasInfo | None) -> str:
    """Apply alias mapping to an unaliased URI if possible.

    :param uri: Unaliased URI string.
    :param alias_info: AliasInfo to apply.
    :return: Aliased URI string when applicable.
    :rtype: str
    """
    if alias_info is None:
        return uri
    unaliased_prefix = alias_info.unaliased_prefix()
    if uri.startswith(unaliased_prefix):
        return f"{alias_info.alias}://{uri[len(unaliased_prefix) :]}"
    return uri
