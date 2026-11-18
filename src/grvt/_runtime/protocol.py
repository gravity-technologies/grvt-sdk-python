"""WebSocket protocol state as plain classes: no sockets, no timers, no event loop.

RequestTable   JSON-RPC uint32 id -> pending request
Sequencer      per-subscription sequence tracking and gap detection
Backoff        full-jitter reconnect delays

Server behaviour this follows:
- Subscribe reply comes first, then snapshot frames (sequence_number 0), then buffered deltas newer
  than `latest_sequence_number`, then live deltas, all in order on one connection.
- Every delta carries `prev_sequence_number`; with global sequence numbers the next delta's prev
  equals the previous delta's number, so a mismatch is a gap.
- Subscribing again to the same primary selector replaces the old one and sends a fresh snapshot.
- Seen on testnet: after a re-subscribe on the same socket, the first delta's prev_sequence_number is
  the OLD last sequence number, not the reply's latest_sequence_number. So the first sequenced frame
  after a (re)subscribe reply is accepted when its prev is 0 or not greater than the latest sequence
  number known then; after that, prev must equal the last sequence number seen, otherwise it is a gap.
"""

from __future__ import annotations

import enum
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .._generated import behavior

UINT32_MAX = 0xFFFFFFFF


@dataclass(slots=True)
class Pending:
    method: str
    write: bool
    waiter: Any  # usually an asyncio.Future; opaque here
    # Runs inside the read loop, before any later frame is processed (subscribe acks must
    # update the sequencer before the snapshot frames that follow them).
    on_reply: Callable[[Any, Any], None] | None = None
    context: Any = None


class RequestTable:
    """uint32 ids; 0 is never used (the gateway omits id 0 in replies)."""

    def __init__(self, start: int = 1) -> None:
        self._next = start
        self._pending: dict[int, Pending] = {}

    def __len__(self) -> int:
        return len(self._pending)

    def add(self, entry: Pending) -> int:
        for _ in range(UINT32_MAX):
            rid = self._next
            self._next = 1 if self._next >= UINT32_MAX else self._next + 1
            if rid not in self._pending:
                self._pending[rid] = entry
                return rid
        raise RuntimeError("request table full")

    def pop(self, rid: int) -> Pending | None:
        return self._pending.pop(rid, None)

    def drain(self) -> list[Pending]:
        out = list(self._pending.values())
        self._pending.clear()
        return out

    def writes_in_flight(self) -> int:
        return sum(1 for p in self._pending.values() if p.write)


class Verdict(enum.Enum):
    DELIVER = "deliver"
    DROP = "drop"  # duplicate, or a frame from before the current subscribe reply
    GAP = "gap"


class SeqState(enum.Enum):
    AWAITING_ACK = "awaiting_ack"
    LIVE = "live"


@dataclass(slots=True)
class Sequencer:
    state: SeqState = SeqState.AWAITING_ACK
    last: int = 0
    snapshots_left: int = 0
    gaps: int = 0
    resync_started: float | None = None  # set by the caller when it re-subscribes after a gap
    first: bool = False  # the first sequenced frame after a (re)subscribe reply is still to come

    def reset(self) -> None:
        self.state = SeqState.AWAITING_ACK
        self.last = 0
        self.snapshots_left = 0

    def on_ack(self, latest_sequence_number: int, num_snapshots: int) -> None:
        self.state = SeqState.LIVE
        self.last = latest_sequence_number
        self.snapshots_left = num_snapshots
        self.first = True
        if num_snapshots == 0:
            self.resync_started = None

    def on_frame(self, seq: int | None, prev: int | None) -> Verdict:
        if self.state is SeqState.AWAITING_ACK:
            return Verdict.DROP
        if not seq:  # snapshot (sequence_number 0) or an unsequenced frame
            if self.snapshots_left:
                self.snapshots_left -= 1
                if self.snapshots_left == 0:
                    self.resync_started = None
            return Verdict.DELIVER
        if seq <= self.last:
            return Verdict.DROP
        first, self.first = self.first, False
        # The re-subscribe quirk above: self.last is still the reply's latest_sequence_number here.
        if prev is not None and prev != self.last and not (first and prev <= self.last):
            self.gaps += 1
            return Verdict.GAP
        self.last = seq
        return Verdict.DELIVER

    def resync_overdue(self, now: float, limit: float) -> bool:
        return self.resync_started is not None and now - self.resync_started > limit


@dataclass(slots=True)
class Backoff:
    """Full jitter: sleep uniform(0, min(cap, base * 2**attempt))."""

    base: float = behavior.RECONNECT_BACKOFF_BASE
    cap: float = behavior.RECONNECT_BACKOFF_CAP
    attempt: int = 0
    rng: random.Random = field(default_factory=random.Random)

    def next_delay(self) -> float:
        d = self.rng.uniform(0, min(self.cap, self.base * (2**self.attempt)))
        self.attempt = min(self.attempt + 1, 30)
        return d

    def reset(self) -> None:
        self.attempt = 0


def jsonrpc_frame(method: str, params_json: bytes, rid: int) -> bytes:
    """Build the request bytes without re-encoding already-encoded params."""
    return (
        b'{"jsonrpc":"2.0","method":"'
        + method.encode()
        + b'","params":'
        + params_json
        + b',"id":'
        + str(rid).encode()
        + b"}"
    )
