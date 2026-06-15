## 0.1.0 - 2026.06.15
- breaking change
    - To improve performance, we decided to remove S3 symlink support. This feature originally came from `megfile` and was never officially supported, with very few users, so it will no longer be supported in this new library.
    - Removed the `overwrite` parameter because its behavior was somewhat ambiguous. The current behavior is now equivalent to `overwrite=True`.
    - Removed the HDFS protocol because the current implementation does not provide native asyncio support.
    - Merged the CLI optional dependencies into the main dependencies.
- fix
    - Fix `follow_symlinks` bug in `copy` and `unlink`.
    - Fix the bug where sync fast mode does not skip existing files.

## 0.0.6 - 2026.05.25
- feat
    - Support sort param in `smart_glob` and `smart_iglob`.

## 0.0.5 - 2026.05.22
- fix
    - Fix the bug where `smart_iglob` returns empty results.

## 0.0.4 - 2026.04.08
- fix
    - Fix the file locking issue when opening files via SFTP.
- perf
    - Replace the SFTP prefetch reader with `asyncssh open`.
    - Set AsyncSSH’s log level to `error`.

## 0.0.3 - 2026.03.27
- feat
    - Support http, sftp, stdio, webdav and hdfs.
    - Cli support more commands.
- perf
    - Speed up s3 list files and smart sync.
- docs
    - Add more docs.

## 0.0.2 - 2026.03.02
- feat
    - Support cli.
- perf
    - Speed up s3 by asyncio task.

## 0.0.1 - 2026.02.27
- First release.
