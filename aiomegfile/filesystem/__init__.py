from aiomegfile.filesystem.hdfs import HdfsFileSystem
from aiomegfile.filesystem.http import (
    HttpFileSystem,
    HttpsFileSystem,
)
from aiomegfile.filesystem.local import LocalFileSystem
from aiomegfile.filesystem.s3 import S3FileSystem
from aiomegfile.filesystem.sftp import SftpFileSystem
from aiomegfile.filesystem.stdio import StdioFileSystem
from aiomegfile.filesystem.webdav import (
    WebdavFileSystem,
    WebdavsFileSystem,
)

__all__ = [
    "HdfsFileSystem",
    "HttpFileSystem",
    "HttpsFileSystem",
    "LocalFileSystem",
    "S3FileSystem",
    "SftpFileSystem",
    "StdioFileSystem",
    "WebdavFileSystem",
    "WebdavsFileSystem",
]
