"""REST transport, errors, retries, hooks, logging and the sync facade."""

import logging
from decimal import Decimal

import msgspec
import pytest
from conftest import CREATE_ORDER, INSTRUMENT, TICKER, instrument_payload

from grvt._runtime import (
    ApiError,
    Call,
    EventKind,
    GrvtError,
    OrderStateUnknown,
    RateLimitError,
    RpcSpec,
    Runtime,
    SyncRunner,
    TransportError,
)
from grvt._runtime.log import redact
from grvt._runtime.transport import AiohttpTransport


class Instrument(msgspec.Struct):
    instrument: str
    create_time: str  # quoted 64-bit int stays str
    tick_size: Decimal


class InstrumentResponse(msgspec.Struct):
    result: Instrument


async def test_read_decodes_typed_response(gateway):
    gateway.rest_handler = lambda route, body: (
        200,
        {
            "result": {
                "instrument": "BTC_USDT_Perp",
                "create_time": "1793172225200080949",
                "tick_size": "0.1",
                "extra": 1,
            }
        },
    )
    spec = RpcSpec("v1/instrument", "md", "/full/v1/instrument", InstrumentResponse)
    async with Runtime(gateway.env, encoding="full") as rt:
        out = await rt.call(spec, {"instrument": "BTC_USDT_Perp"})
    assert out.result.tick_size == Decimal("0.1") and out.result.create_time == "1793172225200080949"
    assert gateway.rest_calls[0][1] == {"instrument": "BTC_USDT_Perp"}


async def test_error_envelope_becomes_api_error(gateway):
    gateway.rest_handler = lambda r, b: (400, {"code": 1003, "message": "malformed", "status": 400})
    async with Runtime(gateway.env, encoding="full") as rt:
        with pytest.raises(ApiError) as ei:
            await rt.call(INSTRUMENT, {})
    e = ei.value
    assert (e.code, e.message, e.status, e.rpc, e.retryable) == (1003, "malformed", 400, "v1/instrument", False)
    assert len(gateway.rest_calls) == 1  # not retryable -> one attempt


async def test_reads_retry_with_events(gateway):
    gateway.rest_handler = lambda r, b: (503, {"code": 1015, "message": "unavailable", "status": 503})
    events = []
    async with Runtime(gateway.env, encoding="full", on_event=events.append) as rt:
        with pytest.raises(ApiError):
            await rt.call(INSTRUMENT, {})
    assert len(gateway.rest_calls) == 3
    assert [e.kind for e in events] == [EventKind.RETRY, EventKind.RETRY]


async def test_rate_limit_error(gateway):
    gateway.rest_handler = lambda r, b: (429, {"code": 1006, "message": "slow down", "status": 429})
    async with Runtime(gateway.env, encoding="full") as rt:
        with pytest.raises(RateLimitError):
            await rt.call(INSTRUMENT, {})


async def test_writes_never_retry(gateway):
    gateway.rest_handler = lambda r, b: (503, {"code": 1015, "message": "unavailable", "status": 503})
    async with Runtime(gateway.env, encoding="full", api_key="test-key") as rt:
        with pytest.raises(OrderStateUnknown):
            await rt.call(CREATE_ORDER, {"order": {}})
    assert [c[0] for c in gateway.rest_calls] == ["create_order"]
    headers = gateway.rest_calls[0][2]
    assert headers["Cookie"] == "gravity=cookie-123" and headers["X-Grvt-Account-Id"] == "ACC:1"


async def test_limit_rejection_drops_the_instruments_metadata(gateway):
    gateway.rest_handler = lambda r, b: (400, {"code": 2064, "message": "Invalid limit price tick", "status": 400})
    spec = RpcSpec(
        "v1/create_order",
        "td",
        "/full/v1/create_order",
        dict,
        auth=True,
        write=True,
        refresh_metadata_on=frozenset({2064}),
    )
    order = {"order": {"legs": [{"instrument": "BTC_USDT_Perp", "size": "1"}, {"instrument": "ETH_USDT_Perp"}]}}
    async with Runtime(gateway.env, encoding="full", api_key="test-key") as rt:
        for name in ("BTC_USDT_Perp", "ETH_USDT_Perp", "SOL_USDT_Perp"):
            rt.metadata.put("instrument", name, {"tick_size": "0.1"})
        with pytest.raises(ApiError):
            await rt.call(spec, order)
        assert rt.metadata.peek("instrument", "BTC_USDT_Perp") is None
        assert rt.metadata.peek("instrument", "ETH_USDT_Perp") is None
        assert rt.metadata.peek("instrument", "SOL_USDT_Perp") is not None  # not in this order
    assert len(gateway.rest_calls) == 1  # never resent


class AmbiguousTransport(AiohttpTransport):
    """create_order times out after the request left the process; everything else is normal."""

    async def post(self, url, body, headers, timeout):
        if url.endswith("create_order"):
            raise TransportError("ReadTimeout", sent=True)
        return await super().post(url, body, headers, timeout)


async def test_write_timeout_is_order_state_unknown(gateway):
    async with Runtime(gateway.env, encoding="full", api_key="test-key", transport=AmbiguousTransport()) as rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(CREATE_ORDER, {"order": {"metadata": {"client_order_id": "5"}}}, client_order_id="5")
    assert ei.value.client_order_id == "5" and ei.value.client_order_ids == ["5"]


async def test_bulk_write_timeout_carries_every_client_order_id(gateway):
    async with Runtime(gateway.env, encoding="full", api_key="test-key", transport=AmbiguousTransport()) as rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(
                CREATE_ORDER,
                {"orders": [{"metadata": {"client_order_id": "5"}}, {"metadata": {"client_order_id": "6"}}]},
                client_order_id=["5", "6"],
            )
    assert ei.value.client_order_ids == ["5", "6"] and ei.value.client_order_id is None
    assert "client_order_ids=['5', '6']" in str(ei.value)


async def test_before_send_runs_before_sign_and_can_modify(gateway):
    order = []

    def before(call: Call):
        order.append(("hook", dict(call.params)))
        call.params["order"]["tagged"] = True

    async def sign(rt, params):
        order.append(("sign", dict(params["order"])))
        return dict(params, signature="sig")

    async with Runtime(gateway.env, encoding="full", api_key="test-key", before_send=before) as rt:
        await rt.call(CREATE_ORDER, {"order": {}}, sign=sign)
    assert order[0][0] == "hook" and order[1] == ("sign", {"tagged": True})
    assert gateway.rest_calls[-1][1] == {"order": {"tagged": True}, "signature": "sig"}


async def test_auth_needed_without_api_key(gateway):
    async with Runtime(gateway.env, encoding="full") as rt:
        with pytest.raises(GrvtError, match="api_key"):
            await rt.call(CREATE_ORDER, {})


async def test_debug_log_redacts(gateway, caplog, monkeypatch):
    monkeypatch.setenv("GRVT_LOG", "debug")
    caplog.set_level(logging.DEBUG, logger="grvt")
    async with Runtime(gateway.env, encoding="full", api_key="test-key") as rt:
        await rt.call(CREATE_ORDER, {"order": {}, "signature": {"r": "0x" + "a" * 64, "s": "0x" + "b" * 64}})
    text = caplog.text
    assert "cookie-123" not in text and "a" * 64 not in text and "b" * 64 not in text
    assert "create_order" in text


async def test_session_repr_never_shows_the_cookie(gateway):
    async with Runtime(gateway.env, encoding="full", api_key="test-key") as rt:
        await rt.call(CREATE_ORDER, {"order": {}})
        assert rt.session.cookie == "cookie-123"
        for text in (repr(rt.session), str(rt.session), f"{rt.session!r}"):
            assert "cookie-123" not in text and "<redacted>" in text


def test_redact_patterns():
    s = redact('{"api_key":"k1","private_key":"0x' + "1" * 64 + '","r":"0x2","x":"keep"} Cookie: gravity=abc;')
    assert "k1" not in s and "1" * 64 not in s and "abc" not in s and "keep" in s


async def test_sync_clock(gateway):
    async with Runtime(gateway.env, encoding="full") as rt:
        await rt.sync_clock()
        assert abs(rt.clock.now_ns() - 1_700_000_000_000 * 1_000_000) < 5e9


def test_sync_runner_drives_async_runtime(gateway_sync):
    env = gateway_sync
    runner = SyncRunner()
    try:
        rt = runner.build(lambda: Runtime(env, encoding="full"))
        out = runner.run(rt.call(INSTRUMENT, {"instrument": "X"}))
        assert out == {"result": instrument_payload("X")}
        sub = rt.stream(TICKER, "S@500")
        it = runner.iterate(sub)
        first = next(it)
        assert first.sequence_number == "0"
        runner.run(sub.close())
        runner.run(rt.close())
    finally:
        runner.close()


@pytest.mark.parametrize("bulk", [False, True])
async def test_unknown_write_uses_final_submitted_ids(gateway, bulk):
    def before(call):
        for order in call.params.get("orders", [call.params.get("order")]):
            order["metadata"]["client_order_id"] = "99"
        if bulk:
            call.params["orders"] = call.params["orders"][:1]

    params = (
        {"orders": [{"metadata": {"client_order_id": "1"}}, {"metadata": {"client_order_id": "2"}}]}
        if bulk
        else {"order": {"metadata": {"client_order_id": "1"}}}
    )
    async with Runtime(
        gateway.env, encoding="full", api_key="test-key", transport=AmbiguousTransport(), before_send=before
    ) as rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(CREATE_ORDER, params, client_order_id=["1", "2"])
        assert ei.value.client_order_ids == ["99"]
        assert isinstance(ei.value.__cause__, TransportError)
