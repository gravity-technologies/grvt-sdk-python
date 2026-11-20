"""REST transport, errors, retries, hooks, logging and the sync facade."""

import logging
from decimal import Decimal

import pytest
from conftest import BULK_ORDERS, CREATE_ORDER, INSTRUMENT, SIGNATURE, TICKER, instrument_payload, order

from grvt._runtime import (
    ApiError,
    Call,
    ConfigError,
    OrderStateUnknown,
    Runtime,
    SyncRunner,
    TransportError,
)
from grvt._runtime.log import redact
from grvt._runtime.transport import AiohttpTransport


async def test_read_decodes_typed_response(gateway):
    gateway.rest_handler = lambda route, body: (
        200,
        {"result": {**instrument_payload(), "create_time": "1793172225200080949", "extra": 1}},
    )
    async with Runtime(gateway.env, **gateway.client_options) as rt:
        out = await rt.call(INSTRUMENT, {"instrument": "BTC_USDT_Perp"})
    assert out.result.tick_size == Decimal("0.1")
    assert out.result.create_time.nanoseconds == 1793172225200080949  # exact: no float on the way
    assert gateway.rest_calls[0][1] == {"instrument": "BTC_USDT_Perp"}


async def test_error_envelope_becomes_api_error(gateway):
    gateway.rest_handler = lambda r, b: (400, {"code": 1003, "message": "malformed", "status": 400})
    async with Runtime(gateway.env, **gateway.client_options) as rt:
        with pytest.raises(ApiError) as ei:
            await rt.call(INSTRUMENT, {"instrument": "BTC_USDT_Perp"})
    e = ei.value
    assert (e.code, e.message, e.status, e.rpc, e.retryable) == (1003, "malformed", 400, "v1/instrument", False)
    assert len(gateway.rest_calls) == 1  # not retryable -> one attempt


async def test_private_rest_call_sends_the_session_cookie_and_account_id(gateway):
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key") as rt:
        await rt.call(CREATE_ORDER, order())
    headers = gateway.rest_calls[0][2]
    assert headers["Cookie"] == "gravity=cookie-123" and headers["X-Grvt-Account-Id"] == "ACC:1"


async def test_limit_rejection_drops_the_instruments_metadata(gateway):
    gateway.rest_handler = lambda r, b: (400, {"code": 2064, "message": "Invalid limit price tick", "status": 400})
    assert 2064 in CREATE_ORDER.refresh_metadata_on
    legs = [
        {"instrument": "BTC_USDT_Perp", "size": "1", "is_buying_asset": True},
        {"instrument": "ETH_USDT_Perp", "size": "1", "is_buying_asset": False},
    ]
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key") as rt:
        for name in ("BTC_USDT_Perp", "ETH_USDT_Perp", "SOL_USDT_Perp"):
            rt.metadata.put("instrument", name, {"tick_size": "0.1"})
        with pytest.raises(ApiError):
            await rt.call(CREATE_ORDER, order(legs=legs))
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
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key", transport=AmbiguousTransport()) as rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(CREATE_ORDER, order("5"), client_order_id="5")
    assert ei.value.client_order_id == "5" and ei.value.client_order_ids == ["5"]


async def test_bulk_write_unknown_has_no_single_client_order_id_and_names_them_all(gateway):
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key", transport=AmbiguousTransport()) as rt:
        with pytest.raises(OrderStateUnknown) as ei:
            await rt.call(
                BULK_ORDERS,
                {"sub_account_id": "42", "orders": [order("5")["order"], order("6")["order"]]},
                client_order_id=["5", "6"],
            )
    assert ei.value.client_order_id is None
    assert "client_order_ids=['5', '6']" in str(ei.value)


async def test_before_send_runs_before_sign_and_can_modify(gateway):
    seen = []
    params = order()
    del params["order"]["signature"]  # added by sign below

    def before(call: Call):
        seen.append(("hook", dict(call.params["order"])))
        call.params["order"]["post_only"] = True

    async def sign(rt, params):
        seen.append(("sign", dict(params["order"])))
        return {"order": {**params["order"], "signature": SIGNATURE}}

    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key", before_send=before) as rt:
        await rt.call(CREATE_ORDER, params, sign=sign)
    assert seen[0][0] == "hook" and "post_only" not in seen[0][1]
    assert seen[1][0] == "sign" and seen[1][1]["post_only"] is True
    assert gateway.rest_calls[-1][1]["order"] == {**params["order"], "post_only": True, "signature": SIGNATURE}


async def test_debug_log_redacts(gateway, caplog, monkeypatch):
    monkeypatch.setenv("GRVT_LOG", "debug")
    caplog.set_level(logging.DEBUG, logger="grvt")
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key") as rt:
        await rt.call(CREATE_ORDER, order(signature={**SIGNATURE, "r": "0x" + "a" * 64, "s": "0x" + "b" * 64}))
    text = caplog.text
    assert "cookie-123" not in text and "a" * 64 not in text and "b" * 64 not in text
    assert "create_order" in text


async def test_session_repr_never_shows_the_cookie(gateway):
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key") as rt:
        await rt.call(CREATE_ORDER, order())
        assert rt.session.cookie == "cookie-123"
        for text in (repr(rt.session), str(rt.session), f"{rt.session!r}"):
            assert "cookie-123" not in text and "<redacted>" in text


def test_redact_patterns():
    s = redact('{"api_key":"k1","private_key":"0x' + "1" * 64 + '","r":"0x2","x":"keep"} Cookie: gravity=abc;')
    assert "k1" not in s and "1" * 64 not in s and "abc" not in s and "keep" in s


async def test_sync_clock(gateway):
    async with Runtime(gateway.env, **gateway.client_options) as rt:
        await rt.sync_clock()
        assert abs(rt.clock.now_ns() - 1_700_000_000_000 * 1_000_000) < 5e9


def test_sync_runner_drives_async_runtime(gateway_sync):
    runner = SyncRunner()
    try:
        rt = runner.build(lambda: Runtime(gateway_sync.env, **gateway_sync.client_options))
        out = runner.run(rt.call(INSTRUMENT, {"instrument": "X"}))
        assert out.result.instrument == "X"
        sub = rt.stream(TICKER, "S@500")
        it = runner.iterate(sub)
        first = next(it)
        assert first.sequence_number == "0"
        runner.run(sub.close())
        runner.run(rt.close())
    finally:
        runner.close()


def _change_id(params):
    params["order"]["metadata"]["client_order_id"] = "99"


def _remove_id(params):
    del params["order"]["metadata"]["client_order_id"]


def _drop_order(params):
    params["orders"] = params["orders"][:1]


@pytest.mark.parametrize("change", [_change_id, _remove_id, _drop_order])
async def test_before_send_must_not_change_order_identity(gateway, change):
    # The IDs are how an unknown outcome is looked up: a hook that changes them fails before anything is sent.
    bulk = change is _drop_order
    params = {"sub_account_id": "42", "orders": [order("1")["order"], order("2")["order"]]} if bulk else order("1")
    async with Runtime(
        gateway.env, **gateway.client_options, api_key="test-key", before_send=lambda call: change(call.params)
    ) as rt:
        with pytest.raises(ConfigError, match="client order IDs"):
            await rt.call(BULK_ORDERS if bulk else CREATE_ORDER, params, client_order_id=["1", "2"] if bulk else "1")
    assert gateway.rest_calls == []
