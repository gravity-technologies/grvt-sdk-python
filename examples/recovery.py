"""Offline recovery walkthrough. Run with the installed grvt package; no keys needed."""

from dataclasses import dataclass
from decimal import Decimal

from grvt import BookError, Event, EventKind, GapRange, OrderBook, Timestamp, backfill_pages_sync, models


def snapshot(price: str) -> models.WSOrderbookLevelsFeedDataV1:
    # A fixture stands in for a snapshot from a newly opened subscription.
    return models.WSOrderbookLevelsFeedDataV1(
        stream="v1.book.d",
        selector="BTC_USDT_Perp@500",
        sequence_number="0",
        prev_sequence_number="0",
        feed=models.OrderbookLevels(
            event_time=Timestamp(0),
            instrument="BTC_USDT_Perp",
            bids=[models.OrderbookLevel(price=Decimal(price), size=Decimal("1"), num_orders=1)],
            asks=[],
        ),
    )


def recover_book() -> None:
    book = OrderBook("BTC_USDT_Perp")
    old_snapshot = snapshot("100")
    book.apply(old_snapshot)
    gap = GapRange(after_sequence=10, through_sequence=None, reason="disconnect")
    # Forward the hook event before consuming more frames from the subscription.
    book.on_event(Event(kind=EventKind.GAP, stream=old_snapshot.stream, selector=old_snapshot.selector, gap=gap))
    for operation in (book.snapshot, lambda: book.apply(old_snapshot)):
        try:
            operation()
        except BookError:
            pass
        else:
            raise AssertionError("a gapped book must reject reads and queued frames")
    # In a live application: close the old subscription, open a fresh one, then
    # read its snapshot. Do not use reset to accept a frame from the old queue.
    fresh_snapshot = snapshot("101")
    book.reset(fresh_snapshot)
    assert book.snapshot().bids[0].price == Decimal("101")
    print("fresh snapshot restored book; missed private events remain unresolved")


@dataclass
class HistoryPage:
    next: str | None
    events: tuple[str, ...]


def inspect_history() -> None:
    gap = GapRange(after_sequence=10, through_sequence=None, reason="disconnect")
    pages = {None: HistoryPage("second-page", ("fill-1",)), "second-page": HistoryPage(None, ())}
    calls: list[str | None] = []

    def fetch(cursor: str | None) -> HistoryPage:
        # In a live application this callback chooses the history endpoint,
        # account and overlapping time window. A sequence is not a REST cursor.
        calls.append(cursor)
        return pages[cursor]

    for result in backfill_pages_sync(fetch, gap=gap):
        assert result.continuity == "unproven"
        print("history", result.page.events, "exhausted", result.history_exhausted, result.continuity)
    assert calls == [None, "second-page"]
    # An empty final page is not proof that delayed events cannot appear later.
    assert result.history_exhausted and result.continuity == "unproven"


if __name__ == "__main__":
    recover_book()
    inspect_history()
