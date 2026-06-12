#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE.pyre file in the root directory of this source tree.

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, cast

LOG: logging.Logger = logging.getLogger(__name__)

Error = Dict[str, Any]
Location = Dict[str, Any]


def _locations(errors: List[Error]) -> Dict[str, Location]:
    """Build SARIF artifact locations for Pyre errors.

    :param errors: Pyre error entries emitted by ``pyre --output=json``.
    :return: Artifact locations keyed by Pyre path.
    """
    locations = {
        error["path"]: {"uri": f"file://{Path.cwd() / error['path']}", "index": 0}
        for error in errors
    }
    for index, location in enumerate(locations.values()):
        location["index"] = index
    return locations


def _to_sarif_result(error: Error, locations: Dict[str, Location]) -> Dict[str, Any]:
    """Convert one Pyre error to a SARIF result.

    :param error: A Pyre error entry.
    :param locations: Artifact locations keyed by Pyre path.
    :return: A SARIF result dictionary.
    """
    LOG.info(f"Transforming:\n{error}")

    return {
        "ruleId": "type-error",
        "ruleIndex": 0,
        "level": "error",
        "message": {"text": error["description"]},
        "locations": [
            {
                "physicalLocation": {
                    "artifactLocation": locations[error["path"]],
                    "region": {
                        "startLine": error["line"],
                        "startColumn": error["column"] + 1,
                    },
                }
            }
        ],
    }


def _to_sarif(errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert Pyre errors to SARIF.

    :param errors: Pyre error entries emitted by ``pyre --output=json``.
    :return: A SARIF report dictionary.
    """
    LOG.info(f"Transforming:\n{errors}")
    locations = _locations(errors)
    return {
        "version": "2.1.0",
        "$schema": "http://json.schemastore.org/sarif-2.1.0-rtm.4",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "Pyre",
                        "informationUri": "https://www.pyre-check.org",
                        "rules": [
                            {
                                "id": "type-error",
                                "shortDescription": {"text": "Type Error"},
                                "helpUri": "https://www.pyre-check.org",
                                "help": {"text": "Pyre is a type checker for Python"},
                            }
                        ],
                    }
                },
                "artifacts": [
                    {"location": location}
                    for location in sorted(
                        locations.values(), key=lambda location: location["index"]
                    )
                ],
                "results": [_to_sarif_result(error, locations) for error in errors],
            }
        ],
    }


def _load_errors(source: str) -> List[Error]:
    """Load Pyre errors from JSON text.

    :param source: JSON text emitted by Pyre.
    :return: Parsed Pyre errors, or an empty list for blank input.
    """
    if not source.strip():
        return []
    errors = json.loads(source)
    if not isinstance(errors, list):
        raise TypeError("Expected Pyre output to be a JSON array")
    return cast(List[Error], errors)


def _main() -> None:
    """Convert Pyre JSON results from stdin to SARIF on stdout."""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG
    )

    sarif = _to_sarif(_load_errors(sys.stdin.read()))
    json.dump(sarif, sys.stdout, indent=4)


if __name__ == "__main__":
    _main()
