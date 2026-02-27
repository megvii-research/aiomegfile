import os
import typing as T

from aiomegfile.interfaces import StatResult

StatLike = T.Union[StatResult, os.stat_result]


def get_sync_type(src_protocol: str, dst_protocol: str) -> str:
    """Return the sync type based on source and destination protocols.

    :param src_protocol: Source protocol name.
    :param dst_protocol: Destination protocol name.
    :return: ``upload``, ``download``, or ``copy``.
    :rtype: str
    """
    if dst_protocol == "s3" or dst_protocol.startswith("s3+"):
        return "upload"
    if src_protocol == "s3" or src_protocol.startswith("s3+"):
        return "download"
    return "copy"


def compare_time(src_stat: StatLike, dst_stat: StatLike, sync_type: str) -> bool:
    """Return True if modification time indicates no update is required.

    :param src_stat: Source file stat.
    :param dst_stat: Destination file stat.
    :param sync_type: Sync type returned by ``get_sync_type``.
    :return: True if update can be skipped, otherwise False.
    :rtype: bool
    """
    delta = dst_stat.st_mtime - src_stat.st_mtime
    if sync_type in ("upload", "copy"):
        return delta >= 0
    if sync_type == "download":
        return delta <= 0
    return delta >= 0


def is_same_file(src_stat: StatLike, dst_stat: StatLike, sync_type: str) -> bool:
    """Return True if the file stats indicate identical content.

    :param src_stat: Source file stat.
    :param dst_stat: Destination file stat.
    :param sync_type: Sync type returned by ``get_sync_type``.
    :return: True if size matches and mtime comparison allows skipping.
    :rtype: bool
    """
    if getattr(dst_stat, "isdir", False):
        return False
    return src_stat.st_size == dst_stat.st_size and compare_time(
        src_stat, dst_stat, sync_type
    )
