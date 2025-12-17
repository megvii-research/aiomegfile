aiomegfile - Asyncio implementation of megfile
---

[![Build](https://github.com/megvii-research/aiomegfile/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/megvii-research/aiomegfile/actions/workflows/run-tests.yml)
<!-- [![Documents](https://github.com/megvii-research/aiomegfile/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/megvii-research/aiomegfile/actions/workflows/publish-docs.yml) -->
[![Codecov](https://img.shields.io/codecov/c/gh/megvii-research/aiomegfile)](https://app.codecov.io/gh/megvii-research/aiomegfile/)
<!-- [![Latest version](https://img.shields.io/pypi/v/aiomegfile.svg)](https://pypi.org/project/aiomegfile/)
[![Support python versions](https://img.shields.io/pypi/pyversions/aiomegfile.svg)](https://pypi.org/project/aiomegfile/) -->
<!-- [![License](https://img.shields.io/pypi/l/aiomegfile.svg)](https://github.com/megvii-research/aiomegfile/blob/main/LICENSE) -->

<!-- * Docs: http://megvii-research.github.io/aiomegfile -->

`aiomegfile` is asyncio implementation of `megfile`.

## Quick Start

### Installation

```bash
pip3 install aiomegfile
```

## How to Contribute
* We welcome everyone to contribute code to the `aiomegfile` project, but the contributed code needs to meet the following conditions as much as possible:
    *You can submit code even if the code doesn't meet conditions. The project members will evaluate and assist you in making code changes*

    * **Code format**: Your code needs to pass **code format check**. `aiomegfile` uses `ruff` as lint tool
    * **Static check**: Your code needs complete **type hint**. `aiomegfile` uses `pytype` as static check tool. If `pytype` failed in static check, use `# pytype: disable=XXX` to disable the error and please tell us why you disable it.
    * **Test**: Your code needs complete **unit test** coverage. `aiomegfile` uses `pyfakefs` and `moto` as local file system and s3 virtual environment in unit tests. The newly added code should have a complete unit test to ensure the correctness

* You can help to improve `aiomegfile` in many ways:
    * Write code.
    * Improve [documentation](https://github.com/megvii-research/aiomegfile/blob/main/docs).
    * Report or investigate [bugs and issues](https://github.com/megvii-research/aiomegfile/issues).
    * If you find any problem or have any improving suggestion, [submit a new issuse](https://github.com/megvii-research/aiomegfile/issues) as well. We will reply as soon as possible and evaluate whether to adopt.
    * Review [pull requests](https://github.com/megvii-research/aiomegfile/pulls).
    * Star `aiomegfile` repo.
    * Recommend `aiomegfile` to your friends.
    * Any other form of contribution is welcomed.
