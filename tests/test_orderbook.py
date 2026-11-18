import json

import pytest
from shared_data import load_shared

from grvt import BookError, OrderBook, models
from grvt._runtime.codec import decode
from grvt._runtime.hooks import Event, EventKind, GapRange

CASES = load_shared("scenarios/orderbook.json")


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_book_scenarios(case):
    book = OrderBook("X", mode=case["mode"])
    for step in case["steps"]:
        if step["action"] == "gap":
            book.on_event(
                Event(
                    kind=EventKind.GAP,
                    stream="book.d",
                    selector="X@100",
                    gap=GapRange(after_sequence=None, through_sequence=None, reason=step["reason"]),
                )
            )
            continue
        if step["action"] == "invalidate":
            book.invalidate()
            continue
        frame = decode(json.dumps(step["frame"]).encode(), models.WSOrderbookLevelsFeedDataV1)
        action = getattr(book, step["action"])
        if "error" in step:
            with pytest.raises(BookError) as error:
                action(frame)
            assert error.value.reason == step["error"]
            assert not book.ready
            if step["error"] == "gap":
                assert error.value.gap is not None
                assert error.value.gap.after_sequence == 101
                assert error.value.gap.through_sequence is None
            with pytest.raises(BookError):
                book.snapshot()
        else:
            action(frame)
            snapshot = book.snapshot()
            assert [format(v.price, "f") for v in snapshot.bids] == step["bids"]
            assert [format(v.price, "f") for v in snapshot.asks] == step["asks"]
            assert snapshot.event_time.nanoseconds == 1793172225200080949
            # Neither caller-owned frames nor returned levels can mutate the book.
            if frame.feed.bids:
                frame.feed.bids[0].num_orders = 999
            if snapshot.bids:
                snapshot.bids[0].num_orders = 888
                assert book.snapshot().bids[0].num_orders == 1


def test_gap_event_before_binding_prevents_old_snapshot():
    book = OrderBook("X")
    gap = GapRange(after_sequence=10, through_sequence=None, reason="disconnect")
    book.on_event(Event(kind=EventKind.GAP, stream="book.d", selector="X@100", gap=gap))
    frame = decode(json.dumps(CASES[0]["steps"][0]["frame"]).encode(), models.WSOrderbookLevelsFeedDataV1)
    with pytest.raises(BookError) as error:
        book.apply(frame)
    assert error.value.gap == gap
    book.reset(frame)
    book.on_event(Event(kind=EventKind.GAP, stream="book.d", selector="Y@100", gap=gap))
    assert book.ready
    book.on_event(Event(kind=EventKind.GAP, stream="book.d", selector="X@100", gap=gap))
    assert not book.ready
