aiomegfile - Asyncio implementation of megfile
---

[![Build](https://github.com/megvii-research/aiomegfile/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/megvii-research/aiomegfile/actions/workflows/run-tests.yml)
[![Documents](https://github.com/megvii-research/aiomegfile/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/megvii-research/aiomegfile/actions/workflows/publish-docs.yml)
[![Codecov](https://img.shields.io/codecov/c/gh/megvii-research/aiomegfile)](https://app.codecov.io/gh/megvii-research/aiomegfile/)
[![Latest version](https://img.shields.io/pypi/v/aiomegfile.svg)](https://pypi.org/project/aiomegfile/)
[![Support python versions](https://img.shields.io/pypi/pyversions/aiomegfile.svg)](https://pypi.org/project/aiomegfile/)
[![License](https://img.shields.io/pypi/l/aiomegfile.svg)](https://github.com/megvii-research/aiomegfile/blob/main/LICENSE)

* Docs: https://megvii-research.github.io/aiomegfile

`aiomegfile` brings the `megfile` programming model to asyncio applications.
It provides:

- Async smart functions such as `smart_open`, `smart_copy`, and `smart_sync`
- An async `SmartPath` abstraction with a `pathlib`-style interface
- A CLI named `amf` for listing, copying, syncing, streaming, and inspecting files

The public API mirrors `megfile` where possible, but operations are async-first.

## Supported Protocols

Current backends in this repository include:

- Local filesystem with plain paths or `file://`
- `s3://`
- `http://` and `https://` for async read-oriented access
- `sftp://`
- `stdio://` for stdin/stdout/stderr bridging
- `hdfs://` with the `hdfs` extra
- `webdav://` and `webdavs://` with the `webdav` extra

## Installation

Install the core package:

```bash
pip install aiomegfile
```

Install optional extras when you need them:

```bash
pip install "aiomegfile[cli]"
pip install "aiomegfile[hdfs]"
pip install "aiomegfile[webdav]"
```

## Quick Start

### Functional API

```python
import asyncio

from aiomegfile import smart_exists, smart_open


async def main() -> None:
    async with smart_open("/tmp/aiomegfile-demo.txt", "w") as writer:
        await writer.write("hello from aiomegfile\n")

    async with smart_open("/tmp/aiomegfile-demo.txt", "r") as reader:
        content = await reader.read()

    print(content.strip())
    print(await smart_exists("/tmp/aiomegfile-demo.txt"))


if __name__ == "__main__":
    asyncio.run(main())
```

### `SmartPath`

```python
import asyncio

from aiomegfile import SmartPath


async def main() -> None:
    root = SmartPath("s3://example-bucket/demo")
    file_path = root / "message.txt"

    await file_path.write_text("hello from SmartPath\n")
    print(await file_path.read_text())

    async for child in root.iterdir():
        print(await child.as_uri())


if __name__ == "__main__":
    asyncio.run(main())
```

### Syncing Data

```python
import asyncio

from aiomegfile import smart_sync


async def main() -> None:
    await smart_sync("./data", "s3://example-bucket/backup")


if __name__ == "__main__":
    asyncio.run(main())
```

## CLI

Install the CLI extra first:

```bash
pip install "aiomegfile[cli]"
```

Common commands:

```bash
amf ls ./data
amf ls s3://my-bucket/prefix -l
amf cp -r ./data s3://my-bucket/archive
amf sync ./data s3://my-bucket/archive --progress-bar
amf cat https://example.com/data.txt
printf 'payload' | amf to s3://my-bucket/stdin-demo.txt
```

Shell completion can be enabled with:

```bash
amf completion bash
amf completion zsh
amf completion fish
```

## Configuration

Runtime configuration is loaded from `~/.config/megfile/megfile.conf`.
The file supports at least two useful sections:

- `[env]` for environment variables loaded during import
- `[alias]` for custom protocol aliases

Example:

```ini
[env]
AIOMEGFILE_MAX_WORKERS = 16
AIOMEGFILE_READER_BLOCK_SIZE = 16MB

[alias]
datasets = s3://company-datasets/
public = https://static.example.com/
```

With the alias above, `datasets://images/cat.jpg` resolves to
`s3://company-datasets/images/cat.jpg`.

The CLI also provides helpers for common configuration tasks:

```bash
amf config s3 <access_key> <secret_key> --profile-name default
amf config hdfs http://namenode:9870 --profile-name prod
amf config alias datasets s3://company-datasets/
amf config env AIOMEGFILE_MAX_WORKERS=16
```

## Documentation

The full documentation site includes installation notes, protocol details, CLI
reference, and API reference:

https://megvii-research.github.io/aiomegfile

## How to Contribute

We welcome contributions in code, tests, and documentation.

- Run lint checks with `ruff`
- Keep type hints complete
- Add or update tests for behavior changes
- Improve docs when public behavior changes

Issues and pull requests are welcome:

- Issues: https://github.com/megvii-research/aiomegfile/issues
- Pull requests: https://github.com/megvii-research/aiomegfile/pulls
