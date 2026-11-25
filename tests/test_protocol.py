import json
import random

from grvt._runtime import FixedClock, TimeCounterClientOrderId
from grvt._runtime import clock as clock_module
from grvt._runtime.protocol import (
    UINT32_MAX,
    Backoff,
    Pending,
    RequestTable,
    jsonrpc_frame,
)


def p(write=False):
    return Pending("m", write, None)


def test_request_ids_skip_zero_and_in_flight():
    t = RequestTable(start=UINT32_MAX - 1)
    a = t.add(p())
    b = t.add(p())
    c = t.add(p())
    assert (a, b, c) == (UINT32_MAX - 1, UINT32_MAX, 1)
    t2 = RequestTable(start=1)
    t2.add(p())  # id 1 in flight
    t2._next = 1
    assert t2.add(p()) == 2


def test_request_table_pop_and_drain():
    t = RequestTable()
    rid = t.add(p(write=True))
    t.add(p())
    assert t.writes_in_flight() == 1
    popped = t.pop(rid)
    assert popped is not None and popped.write and t.pop(rid) is None
    assert len(t.drain()) == 1 and len(t) == 0


def test_backoff_full_jitter_bounds():
    b = Backoff(base=1.0, cap=8.0, rng=random.Random(1))
    delays = [b.next_delay() for _ in range(10)]
    assert all(0 <= d <= 8.0 for d in delays)
    assert delays[0] <= 1.0
    b.reset()
    assert b.attempt == 0


def test_jsonrpc_frame_is_valid_json():
    f = jsonrpc_frame("v1/create_order", b'{"order":{"a":1}}', 42)
    assert json.loads(f) == {"jsonrpc": "2.0", "method": "v1/create_order", "params": {"order": {"a": 1}}, "id": 42}


def test_sdk_client_order_id_counter_starts_anywhere_in_31_bits_and_wraps(monkeypatch):
    monkeypatch.setattr("secrets.randbits", lambda k: (1 << k) - 1)
    monkeypatch.setattr(clock_module, "_process_counter", clock_module._new_counter())
    ids = TimeCounterClientOrderId(FixedClock(1_800_000_000 * 10**9))
    a, b = ids.next(), ids.next()
    assert 2**63 <= a < 2**64 and 2**63 <= b < 2**64
    assert (a & 0x7FFFFFFF, b & 0x7FFFFFFF) == (0x7FFFFFFF, 0)
