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
    - Megfile patch supported.
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
