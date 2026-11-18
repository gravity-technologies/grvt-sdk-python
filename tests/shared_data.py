"""The test data every SDK shares: the contract (contract/) and the scenarios (scenarios/).

Both live in the monorepo's sdk/ folder, found by walking up from here. To run the tests from a copy of this SDK
outside the monorepo, set GRVT_SDK_SHARED to a folder that holds contract/ and scenarios/.
"""

import json
import os
from pathlib import Path
from typing import Any


def _root() -> Path:
    if env := os.environ.get("GRVT_SDK_SHARED"):
        return Path(env)
    for parent in Path(__file__).resolve().parents:
        if (parent / "contract" / "contract.json").is_file() and (parent / "scenarios").is_dir():
            return parent
    raise FileNotFoundError("no contract/ and scenarios/ above this SDK; set GRVT_SDK_SHARED")


SHARED = _root()


def load_shared(path: str) -> Any:
    """Parses a shared JSON file, e.g. load_shared("scenarios/orderbook.json")."""
    return json.loads((SHARED / path).read_text())
