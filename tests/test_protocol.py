import json
import random

from grvt._runtime import FixedClock, TimeCounterClientOrderId
from grvt._runtime.protocol import (
    UINT32_MAX,
    Backoff,
    Pending,
    RequestTable,
    SeqState,
    Sequencer,
    Verdict,
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


def test_sequencer_snapshot_then_deltas():
    s = Sequencer()
    assert s.on_frame(5, 4) is Verdict.DROP  # before the ack
    s.on_ack(100, 1)
    assert s.state is SeqState.LIVE
    assert s.on_frame(0, None) is Verdict.DELIVER  # snapshot
    assert s.snapshots_left == 0
    assert s.on_frame(100, 99) is Verdict.DROP  # buffered delta already in the snapshot
    assert s.on_frame(101, 100) is Verdict.DELIVER
    assert s.on_frame(101, 100) is Verdict.DROP  # duplicate
    assert s.on_frame(150, 101) is Verdict.DELIVER  # global numbers jump; prev links them
    assert s.on_frame(170, 160) is Verdict.GAP
    assert s.gaps == 1 and s.last == 150


def test_sequencer_first_frame_after_resubscribe():
    s = Sequencer()
    s.on_ack(0, 1)
    s.on_frame(0, None)
    assert s.on_frame(7, 0) is Verdict.DELIVER
    assert s.on_frame(8, 7) is Verdict.DELIVER
    s.reset()  # re-subscribe after a gap
    s.on_ack(20, 1)
    s.on_frame(0, None)
    assert s.on_frame(21, 8) is Verdict.DELIVER  # testnet sends the OLD last sequence number here
    assert s.on_frame(22, 8) is Verdict.GAP  # only the first frame gets that pass
    t = Sequencer()
    t.on_ack(50, 0)
    assert t.on_frame(52, 0) is Verdict.DELIVER  # prev 0 is accepted on the first frame
    assert t.on_frame(53, 51) is Verdict.GAP
    u = Sequencer()
    u.on_ack(50, 0)
    assert u.on_frame(52, 51) is Verdict.GAP  # prev newer than anything known: a real gap


def test_sequencer_reset_and_resync_deadline():
    s = Sequencer()
    s.on_ack(10, 1)
    s.resync_started = 1.0
    assert s.resync_overdue(7.0, 5.0)
    s.on_frame(0, None)  # snapshot clears the resync
    assert not s.resync_overdue(100.0, 5.0)
    s.reset()
    assert s.state is SeqState.AWAITING_ACK


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


def test_sdk_client_order_ids_use_the_api_client_range():
    ids = TimeCounterClientOrderId(FixedClock(1_800_000_000 * 10**9))
    a, b = ids.next(), ids.next()
    assert 2**63 <= a < 2**64 and 2**63 <= b < 2**64 and a != b
    assert (a >> 31) & 0xFFFFFFFF == 1_800_000_000


def test_sdk_client_order_id_counter_starts_anywhere_in_31_bits_and_wraps(monkeypatch):
    monkeypatch.setattr("secrets.randbits", lambda k: (1 << k) - 1)
    ids = TimeCounterClientOrderId(FixedClock(1_800_000_000 * 10**9))
    a, b = ids.next(), ids.next()
    assert 2**63 <= a < 2**64 and 2**63 <= b < 2**64
    assert (a & 0x7FFFFFFF, b & 0x7FFFFFFF) == (0x7FFFFFFF, 0)
