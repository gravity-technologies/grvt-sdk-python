"""Replays sdk/scenarios/decisions.json, the truth table of the generated write and retry decisions."""

from typing import Any

import pytest
from shared_data import load_shared

from grvt._generated import behavior

TABLE: dict[str, list[dict[str, Any]]] = load_shared("scenarios/decisions.json")


@pytest.mark.parametrize("row", TABLE["write_outcome_unknown"], ids=lambda r: r["case"])
def test_write_outcome_unknown(row: dict[str, Any]):
    assert behavior.write_outcome_unknown(row["status"], row["code_status"]) is row["unknown"]


@pytest.mark.parametrize("row", TABLE["read_retryable"], ids=lambda r: r["case"])
def test_read_retryable(row: dict[str, Any]):
    assert behavior.read_retryable(row["code"], row["status"], row["code_retryable"]) is row["retryable"]


@pytest.mark.parametrize("row", TABLE["retry_read"], ids=lambda r: f"retryable={r['retryable']} attempt={r['attempt']}")
def test_retry_read(row: dict[str, Any]):
    assert behavior.retry_read(row["retryable"], row["attempt"]) is row["retry"]
