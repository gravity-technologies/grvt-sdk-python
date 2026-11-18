"""Build a local price-level book from an SDK order-book subscription."""

from dataclasses import dataclass
from decimal import Decimal
from threading import RLock
from typing import Literal

import msgspec

from ._generated import models
from ._runtime.errors import GrvtError
from ._runtime.hooks import Event, EventKind, GapRange
from ._runtime.scalars import Timestamp


class BookError(GrvtError):
    def __init__(self, reason: str, gap: GapRange | None = None):
        self.reason = reason
        self.gap = gap
        super().__init__(f"order book: {reason}; reset from a fresh subscription snapshot")


@dataclass(frozen=True)
class BookSnapshot:
    instrument: str
    sequence_number: int
    event_time: Timestamp
    bids: tuple[models.OrderbookLevel, ...]
    asks: tuple[models.OrderbookLevel, ...]


class OrderBook:
    """Use mode='snapshot' for a complete-snapshot channel; default is delta.

    Feed frames from one SDK subscription, which validates the initial sequence
    against its subscribe acknowledgment. Forward gap events to on_event.
    Delta books require a fresh subscription snapshot after a gap. Snapshot mode
    accepts each complete frame, including sequence zero after reconnect. No REST recovery is implied.
    """

    def __init__(self, instrument: str, *, mode: Literal["delta", "snapshot"] = "delta"):
        if mode not in ("delta", "snapshot"):
            raise ValueError("book mode must be delta or snapshot")
        self._lock = RLock()
        self._invalidated = False
        self.instrument = instrument
        self.mode = mode
        self.ready = False
        self.gap: GapRange | None = None
        self._scope: tuple[str, str] | None = None
        self._sequence = 0
        self._time = Timestamp(0)
        self._bids: dict[Decimal, models.OrderbookLevel] = {}
        self._asks: dict[Decimal, models.OrderbookLevel] = {}

    def invalidate(self, gap: GapRange | None = None) -> None:
        with self._lock:
            self._invalidate(gap)

    def _invalidate(self, gap: GapRange | None) -> None:
        self._invalidated = True
        self.ready = False
        self.gap = gap
        self._bids.clear()
        self._asks.clear()

    def _fail(self, reason: str, gap: GapRange | None = None) -> None:
        self.invalidate(gap)
        raise BookError(reason, gap)

    def on_event(self, event: Event) -> None:
        with self._lock:
            if (
                self.mode == "delta"
                and event.kind == EventKind.GAP
                and (self._scope is None or self._scope == (event.stream, event.selector))
            ):
                self._invalidate(event.gap)

    def reset(self, frame: models.WSOrderbookLevelsFeedDataV1) -> None:
        """Start from a snapshot read from a fresh subscription, not an old queue."""
        with self._lock:
            if self.mode == "delta" and frame.sequence_number != "0":
                self._fail("snapshot_required")
            self._invalidated = False
            self.ready = False
            self._apply(frame)

    def apply(self, frame: models.WSOrderbookLevelsFeedDataV1) -> None:
        with self._lock:
            self._apply(frame)

    def _apply(self, frame: models.WSOrderbookLevelsFeedDataV1) -> None:
        if self._invalidated:
            raise BookError("reset_required", self.gap)
        scope = (frame.stream, frame.selector)
        if frame.feed.instrument != self.instrument or (self._scope is not None and scope != self._scope):
            self._fail("wrong_stream")
        try:
            if any(
                not value.isascii() or not value.isdecimal()
                for value in (frame.sequence_number, frame.prev_sequence_number)
            ):
                self._fail("invalid_sequence")
            sequence, previous = int(frame.sequence_number), int(frame.prev_sequence_number)
            if not 0 <= sequence < 2**64 or not 0 <= previous < 2**64:
                self._fail("invalid_sequence")
        except (ValueError, TypeError):
            self._fail("invalid_sequence")
            return
        if self.ready and not (self.mode == "snapshot" and sequence == 0) and sequence <= self._sequence:
            self._fail("out_of_order")
        snapshot = self.mode == "snapshot" or sequence == 0
        if not snapshot:
            if not self.ready:
                raise BookError("not_ready", self.gap)
            if self._sequence != 0 and previous != self._sequence:
                self._fail(
                    "gap", GapRange(after_sequence=self._sequence, through_sequence=None, reason="sequence_mismatch")
                )
            if sequence <= self._sequence or sequence <= previous:
                self._fail("out_of_order")
        bids, asks = frame.feed.bids or [], frame.feed.asks or []
        for level in [*bids, *asks]:
            if not level.price.is_finite() or not level.size.is_finite() or level.price <= 0 or level.size < 0:
                self._fail("invalid_level")
        if snapshot:
            self._bids.clear()
            self._asks.clear()
        for levels, side in ((bids, self._bids), (asks, self._asks)):
            for level in levels:
                if level.size == 0:
                    side.pop(level.price, None)
                else:
                    side[level.price] = msgspec.structs.replace(level)
        self._scope = scope
        self._sequence = sequence
        self._time = frame.feed.event_time
        self.ready = True
        self.gap = None

    def snapshot(self) -> BookSnapshot:
        with self._lock:
            return self._snapshot()

    def _snapshot(self) -> BookSnapshot:
        if not self.ready:
            raise BookError("not_ready", self.gap)
        return BookSnapshot(
            self.instrument,
            self._sequence,
            self._time,
            tuple(msgspec.structs.replace(self._bids[p]) for p in sorted(self._bids, reverse=True)),
            tuple(msgspec.structs.replace(self._asks[p]) for p in sorted(self._asks)),
        )
