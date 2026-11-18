"""WS manager against the in-process fake gateway."""

import asyncio
import json
import time
from dataclasses import replace

import aiohttp
import pytest
from conftest import CREATE_ORDER, ORDERS, TICKER, wait_until

from grvt._runtime import ApiError, EventKind, OrderStateUnknown, Runtime, TransportError


def rt_for(gw, **kw):
    events = []
    kw.setdefault("on_event", lambda e: events.append(e) if e.kind is not EventKind.FRAME else None)
    rt = Runtime(gw.env, encoding="full", **kw)
    return rt, events


async def take(sub, n, timeout=3.0):
    out = []

    async def run():
        async for item in sub:
            out.append(item)
            if len(out) == n:
                return

    await asyncio.wait_for(run(), timeout)
    return out


async def test_disconnect_reconnects_and_resubscribes(gateway):
    rt, events = rt_for(gateway)
    async with rt:
        sub = rt.stream(TICKER, "BTC_USDT_Perp@500")
        await sub.start()
        await gateway.drop_all(code=1008)  # what the server sends a slow consumer
        await wait_until(lambda: gateway.connect_count == 2 and len(gateway.subscribes()) == 2)
        await asyncio.sleep(0.05)
        await gateway.push("v1.ticker.d", "BTC_USDT_Perp@500")
        items = await take(sub, 3)
        assert [i.sequence_number for i in items] == ["0", "0", "101"]
        kinds = [e.kind for e in events]
        assert kinds.index(EventKind.DISCONNECT) < kinds.index(EventKind.RECONNECT)
        gap = next(e.gap for e in events if e.kind is EventKind.GAP and e.gap.reason == "disconnect")
        assert gap.after_sequence == 100 and gap.through_sequence is None
        disc = next(e for e in events if e.kind is EventKind.DISCONNECT)
        assert disc.detail["code"] == 1008 and "slow consumer" in disc.detail["reason"]


async def test_ws_order_entry_timeout_is_order_state_unknown(gateway):
    gateway.ignore_rpcs.add("v1/create_order")
    rt, _ = rt_for(gateway, api_key="test-key", order_transport="ws", ws_request_timeout=0.2)
    async with rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(
                CREATE_ORDER, {"order": {"x": 1, "metadata": {"client_order_id": "77"}}}, client_order_id="77"
            )
        assert ei.value.client_order_id == "77"
        assert isinstance(ei.value.__cause__, TimeoutError)
        # login attached to the WS handshake
        hs = gateway.handshakes[0]
        assert hs["Cookie"] == "gravity=cookie-123" and hs["X-Grvt-Account-Id"] == "ACC:1"
        assert gateway.rest_calls == []  # never fell back to REST, never resent
        assert sum(r["method"] == "v1/create_order" for r in gateway.received) == 1


async def _failing_send(rt, error: Exception, *, written: bool):
    ws = await rt.socket("td").connected()
    send_str = ws.send_str

    async def broken_send(data: str, compress: int | None = None) -> None:
        if written:
            await send_str(data, compress)
        raise error

    ws.send_str = broken_send


@pytest.mark.parametrize("write", [True, False])
async def test_ws_send_failure_after_bytes_queued_may_have_left(gateway, write):
    gateway.ignore_rpcs.add("v1/create_order")
    gateway.ignore_rpcs.add("v1/instrument")
    rt, _ = rt_for(gateway, api_key="test-key", order_transport="ws")
    async with rt:
        await _failing_send(rt, aiohttp.ClientConnectionResetError("Connection lost"), written=True)
        if write:
            with pytest.raises(OrderStateUnknown) as ei:
                await rt.call(CREATE_ORDER, {"order": {"metadata": {"client_order_id": "77"}}})
            assert ei.value.client_order_ids == ["77"]
            assert isinstance(ei.value.__cause__, TransportError) and ei.value.__cause__.sent
        else:
            with pytest.raises(TransportError) as te:
                await rt.socket("td").request("v1/instrument", b"{}", write=False)
            assert te.value.sent


async def test_ws_order_entry_ok(gateway):
    rt, _ = rt_for(gateway, api_key="test-key", order_transport="ws")
    async with rt:
        out = await rt.call(CREATE_ORDER, {"order": {"x": 1}})
        assert out == {"result": {"echo": {"order": {"x": 1}}}}


async def test_ws_read_timeout_is_transport_error(gateway):
    gateway.ignore_rpcs.add("v1/instrument")
    rt, _ = rt_for(gateway, ws_request_timeout=0.2)
    async with rt:
        with pytest.raises(TransportError):
            await rt.socket("md").request("v1/instrument", b"{}", write=False)


async def test_slow_consumer_does_not_block_read_loop(gateway):
    rt, _ = rt_for(gateway)
    async with rt:
        sub = rt.stream(TICKER, "BTC_USDT_Perp@500", queue_size=4)  # conflating stream
        await sub.start()
        for _ in range(200):
            await gateway.push("v1.ticker.d", "BTC_USDT_Perp@500")
        t0 = time.perf_counter()
        raw = await rt.socket("md").request("v1/instrument", b'{"instrument":"X"}', write=False)
        assert time.perf_counter() - t0 < 1.0
        assert json.loads(bytes(raw)) == {"result": {"echo": {"instrument": "X"}}}
        await wait_until(lambda: sub.dropped >= 190)
        items = await take(sub, 4)
        assert items[-1].sequence_number == "300"  # newest kept, oldest dropped


async def test_bad_api_key_stops_reconnecting(gateway):
    gateway.login_status = 401
    rt, events = rt_for(gateway, api_key="wrong")
    async with rt:
        sub = rt.stream(ORDERS, "1-all")
        with pytest.raises(ApiError) as ei:
            await sub.start()
        assert ei.value.status == 401 and not ei.value.retryable
        assert gateway.connect_count == 0
        assert any(e.kind is EventKind.ERROR and e.detail.get("fatal") for e in events)


async def test_frame_size_check(gateway):

    from grvt._runtime import FrameTooLarge

    gateway.env = replace(gateway.env, ws_max_frame_bytes=64)
    rt, _ = rt_for(gateway, api_key="test-key", order_transport="ws")
    async with rt:
        with pytest.raises(FrameTooLarge):
            await rt.call(CREATE_ORDER, {"order": {"pad": "x" * 100}})
        assert not any(r["method"] == "v1/create_order" for r in gateway.received)


@pytest.mark.parametrize(
    "error", [False, 0, "", {"code": 1003}, {"code": [1003], "message": "bad"}, {"code": 1002, "message": "internal"}]
)
async def test_malformed_or_server_write_reply_is_unknown(gateway, error):
    gateway.ignore_rpcs.add("v1/create_order")
    rt, _ = rt_for(gateway, api_key="test-key", order_transport="ws", ws_request_timeout=0.05)
    async with rt:
        result = asyncio.create_task(rt.call(CREATE_ORDER, {"order": {"metadata": {"client_order_id": "77"}}}))
        await wait_until(lambda: any(r["method"] == "v1/create_order" for r in gateway.received))
        request = next(r for r in gateway.received if r["method"] == "v1/create_order")
        await gateway.conns[0].send_json({"id": request["id"], "error": error, "result": {"result": {"ack": True}}})
        with pytest.raises(OrderStateUnknown) as ei:
            await result
        assert ei.value.client_order_ids == ["77"]
