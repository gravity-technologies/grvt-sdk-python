"""Read-only, unauthenticated checks against Grvt testnet market data through the generated client.

Run with GRVT_SMOKE=1.
"""

import asyncio
import os

import pytest

from grvt import AsyncGrvt, EventKind
from grvt import models as m
from grvt._runtime import codec
from grvt._runtime import lite as lite_codec

pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(os.environ.get("GRVT_SMOKE") != "1", reason="set GRVT_SMOKE=1 to hit testnet"),
]


async def take(sub, n, timeout=20):
    out = []

    async def run():
        async for item in sub:
            out.append(item)
            if len(out) == n:
                return

    await asyncio.wait_for(run(), timeout)
    await sub.close()
    return out


async def test_testnet_market_data():
    events = []

    def on_event(event):
        if event.kind is not EventKind.FRAME:
            events.append(event)

    async with AsyncGrvt("testnet", on_event=on_event) as client:
        inst = await client.get_instrument(instrument="BTC_USDT_Perp")
        assert isinstance(inst, m.ApiGetInstrumentResponse)
        assert inst.result is not None
        assert inst.result.instrument_hash == "0x030501" and inst.result.base_decimals == 9

        all_inst = await client.get_all_instruments(is_active=True)
        assert all_inst.result is not None
        assert any(i.instrument == "BTC_USDT_Perp" for i in all_inst.result)
        currencies = await client.get_currency()
        assert currencies.result is not None
        assert any(c.symbol == "USDT" for c in currencies.result)

        # WS JSON-RPC `result` is the same struct as the REST body
        raw = await client.rt.socket("md").request("v1/instrument", b'{"instrument":"BTC_USDT_Perp"}', write=False)
        assert codec.decode(bytes(raw), m.ApiGetInstrumentResponse) == inst

        ticks = await take(client.ticker_delta_stream(instrument="BTC_USDT_Perp", rate=500), 3)
        assert ticks[0].sequence_number == "0" and ticks[0].feed.instrument == "BTC_USDT_Perp"
        assert ticks[2].prev_sequence_number == ticks[1].sequence_number

        books = await take(client.orderbook_snap_stream(instrument="BTC_USDT_Perp", rate=500, depth=10), 1)
        book = books[0].feed
        assert isinstance(books[0], m.WSOrderbookLevelsFeedDataV1) and book is not None and book.bids

        assert not any(e.kind is EventKind.GAP for e in events)
        print(
            f"\n  {len(all_inst.result)} active instruments; ticker seqs {[t.sequence_number for t in ticks]}; "
            f"book best bid {book.bids[0].price}; events {[e.kind.value for e in events]}"
        )


async def test_testnet_market_data_lite_matches_full():
    """encoding="lite" against the real lite gateway: same decoded results as full, smaller frames."""
    sizes: dict[str, list[int]] = {"full": [], "lite": []}
    got = {}
    for encoding in ("full", "lite"):

        def on_event(event, frames=sizes[encoding]):
            if event.kind is EventKind.FRAME and event.detail["dir"] == "in" and b'"v1.book.s"' in event.raw:
                frames.append(len(event.raw))

        async with AsyncGrvt("testnet", encoding=encoding, on_event=on_event) as client:
            inst = await client.get_instrument(instrument="BTC_USDT_Perp")
            all_inst = await client.get_all_instruments(is_active=True)
            if encoding == "lite":  # a WS RPC: lite envelope, result double-wrapped as in full
                raw = await client.rt.socket("md").request("v1/instrument", b'{"i":"BTC_USDT_Perp"}', write=False)
                assert lite_codec.decode(bytes(raw), m.ApiGetInstrumentResponse) == inst
            books = await take(client.orderbook_snap_stream(instrument="BTC_USDT_Perp", rate=500, depth=10), 3)
            # mini ticker deltas are Partial models; ticker frames carry keys the contract does not list
            ticks = await take(client.ticker_snap_stream(instrument="BTC_USDT_Perp", rate=500), 1)
            minis = await take(client.mini_ticker_delta_stream(instrument="BTC_USDT_Perp", rate=500), 2)
            got[encoding] = (inst, all_inst, books, ticks, minis)
    full, lite = got["full"], got["lite"]
    assert lite[0] == full[0] and lite[0].result is not None and lite[0].result.instrument_hash == "0x030501"
    assert lite[1] == full[1]
    for books in (full[2], lite[2]):  # live books move, so compare the shape, not the levels
        assert [b.stream for b in books] == ["v1.book.s"] * 3 and books[0].selector == "BTC_USDT_Perp@500-10"
        assert all(len(b.feed.bids) == 10 and len(b.feed.asks) == 10 for b in books)
        assert all(b.feed.bids[0].size > 0 and b.feed.bids[0].num_orders > 0 for b in books)
    assert lite[3][0].feed.instrument == "BTC_USDT_Perp" and lite[3][0].feed.mark_price is not None
    assert lite[4][0].feed is not None and lite[4][0].feed.instrument == "BTC_USDT_Perp"
    assert sum(sizes["lite"]) < sum(sizes["full"]) * 0.8
    print(f"\n  book.s frame bytes full {sizes['full']} lite {sizes['lite']}")
