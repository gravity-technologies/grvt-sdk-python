"""The generated package: imports, every contract RPC/stream reachable, and real calls on the fake gateway."""

import dataclasses
import inspect
from decimal import Decimal

import msgspec
import pytest
from conftest import instrument_payload
from eth_account import Account
from eth_account.messages import encode_typed_data
from shared_data import load_shared

import grvt
from grvt import AsyncGrvt, Grvt, PrivateKeySigner
from grvt import models as m
from grvt._generated import signing, specs
from grvt._runtime import Environment, RpcSpec, StreamSpec, Subscription, SyncSubscription
from grvt._runtime.env import ENVIRONMENTS

CONTRACT = load_shared("contract/contract.json")
KEY = "0x" + "22" * 32


def method_name(op):
    """Flat SDK name: calls keep method_name, every stream gets a _stream suffix."""
    return op["method_name"] + "_stream" if "channel" in op else op["method_name"]


def test_every_rpc_and_stream_is_reachable():
    client = AsyncGrvt("testnet")
    calls = CONTRACT["rpcs"] + CONTRACT["streams"]
    names = [method_name(c) for c in calls]
    assert len(set(names)) == len(names), "flat method names must be unique"
    assert not hasattr(client, "login_api_key")
    count = 0
    for c, name in zip(calls, names, strict=True):
        fn = getattr(client, name)
        sync_fn = getattr(Grvt, name)
        assert callable(fn) and callable(sync_fn)
        if "channel" in c:
            assert not inspect.iscoroutinefunction(fn)
        else:
            assert inspect.iscoroutinefunction(fn), c["id"]
        count += 1
    assert count == len(CONTRACT["rpcs"]) + len(CONTRACT["streams"])
    rpc_specs = [v for v in vars(specs).values() if isinstance(v, RpcSpec)]
    stream_specs = [v for v in vars(specs).values() if isinstance(v, StreamSpec)]
    assert {s.path for s in rpc_specs} >= {r["http_path"] for r in CONTRACT["rpcs"]}
    assert {s.stream for s in stream_specs} == {s["channel"] for s in CONTRACT["streams"]}
    assert specs.ORDER_CREATE_ORDER.write and specs.ORDER_CREATE_ORDER.order_entry
    assert not specs.ORDER_GET_ORDER.write


def test_models_cover_contract_and_decode_partial_frames():
    for t in CONTRACT["types"] + CONTRACT["edge"]["types"]:
        cls = getattr(m, t["name"])
        assert set(cls.__struct_fields__) == {f.get("sdk_name", f["name"]) for f in t.get("fields") or []}
    delta = msgspec.json.decode(
        b'{"stream":"v1.ticker.d","selector":"B@500","sequence_number":"5","prev_sequence_number":"4",'
        b'"feed":{"instrument":"B","mark_price":"1.5"}}',
        type=m.PartialWSTickerFeedDataV1,
    )
    assert delta.feed is not None
    assert delta.feed.mark_price == Decimal("1.5") and delta.feed.best_bid_price is None
    assert grvt.enums.TimeInForce.GOOD_TILL_TIME == "GOOD_TILL_TIME"


def test_error_table_registered():
    from grvt._runtime.errors import api_error

    e = api_error(2001, "expired", 401, "v1/create_order")
    assert type(e).__name__ == "SignatureExpiredError" and isinstance(e, grvt.ApiError)


async def test_create_order_round_trip(gateway):

    def rest(route, body):
        if route == "instrument":
            return 200, {"result": instrument_payload(body["instrument"])}
        return 200, {"result": body["order"]}

    gateway.rest_handler = rest
    signer = PrivateKeySigner(KEY)
    async with AsyncGrvt(gateway.env, api_key="test-key", signer=signer, **gateway.client_options) as client:
        order = m.OrderInput(
            sub_account_id="8289849667772468",
            time_in_force=grvt.enums.TimeInForce.GOOD_TILL_TIME,
            legs=[
                m.OrderLeg(
                    instrument="BTC_USDT_Perp",
                    # Decimals are typed str (the contract does not mark them), but a Decimal is accepted.
                    size=Decimal("0.01"),  # pyright: ignore[reportArgumentType]
                    limit_price=Decimal("65000.1"),
                    is_buying_asset=True,
                )
            ],
        )
        resp = await client.create_order(order=order)
        await client.create_order(order=order)
        domain = client.rt.domain
    assert isinstance(resp, m.ApiCreateOrderResponse) and resp.result and resp.result.signature
    assert resp.result.signature.r and resp.result.signature.r.startswith("0x")
    assert sum(c[0] == "instrument" for c in gateway.rest_calls) == 1  # metadata cached
    sent = gateway.rest_calls[-1][1]["order"]
    assert sent["legs"][0]["size"] == "0.01" and sent["metadata"]["client_order_id"]
    sig = sent["signature"]
    message = {
        "subAccountID": 8289849667772468,
        "isMarket": False,
        "timeInForce": 1,
        "postOnly": False,
        "reduceOnly": False,
        "nonce": sig["nonce"],
        "expiration": int(sig["expiration"]),
        "legs": [
            {
                "assetID": 0x030501,
                "contractSize": 10_000_000,
                "limitPrice": 65_000_100_000_000,
                "isBuyingContract": True,
            }
        ],
    }
    typed = signing.EIP712_ORDER.to_typed_data(domain, message)
    who = Account.recover_message(
        encode_typed_data(full_message=typed), vrs=(sig["v"], int(sig["r"], 16), int(sig["s"], 16))
    )
    assert who.lower() == signer.address and sig["chain_id"] == "326"


async def test_bulk_orders_signs_each_order(gateway):

    def rest(route, body):
        if route == "instrument":
            return 200, {"result": instrument_payload(body["instrument"])}
        return 200, {"orders": body.get("orders", []), "cancel_acks": []}

    gateway.rest_handler = rest
    signer = PrivateKeySigner(KEY)
    leg = m.OrderLeg(
        instrument="BTC_USDT_Perp", size=Decimal("0.01"), limit_price=Decimal("65000"), is_buying_asset=True
    )
    plain = m.OrderInput(sub_account_id="1", time_in_force=grvt.enums.TimeInForce.GOOD_TILL_TIME, legs=[leg])
    builder = m.OrderInput(
        sub_account_id="1",
        time_in_force=grvt.enums.TimeInForce.GOOD_TILL_TIME,
        legs=[leg],
        builder="0x" + "22" * 20,
        builder_fee=Decimal("0.001"),
        metadata=m.OrderMetadataInput(client_order_id="42"),
    )
    async with AsyncGrvt(gateway.env, api_key="test-key", signer=signer, **gateway.client_options) as client:
        await client.bulk_orders(sub_account_id="1", orders=[plain, builder])
        domain = client.rt.domain
        sent = gateway.rest_calls[-1][1]["orders"]
        # pure bulk cancel: nothing to sign, no signer needed
        await client.bulk_orders(sub_account_id="1", client_order_ids=["42"])
        assert gateway.rest_calls[-1][1]["client_order_i_ds"] == ["42"]
        assert "client_order_ids" not in gateway.rest_calls[-1][1]
        assert "orders" not in gateway.rest_calls[-1][1] or not gateway.rest_calls[-1][1]["orders"]
    assert sent[0]["metadata"]["client_order_id"] and sent[1]["metadata"]["client_order_id"] == "42"
    assert sent[0]["signature"]["nonce"] != sent[1]["signature"]["nonce"]
    for o, struct in zip(sent, [signing.EIP712_ORDER, signing.EIP712_ORDER_WITH_BUILDER_FEE], strict=True):
        sig = o["signature"]
        message = {
            "subAccountID": 1,
            "isMarket": False,
            "timeInForce": 1,
            "postOnly": False,
            "reduceOnly": False,
            "nonce": sig["nonce"],
            "expiration": int(sig["expiration"]),
            "legs": [
                {
                    "assetID": 0x030501,
                    "contractSize": 10_000_000,
                    "limitPrice": 65_000_000_000_000,
                    "isBuyingContract": True,
                }
            ],
        }
        if "builder" in o:
            message.update(builder="0x" + "22" * 20, builderFee=10)
        who = Account.recover_message(
            encode_typed_data(full_message=struct.to_typed_data(domain, message)),
            vrs=(sig["v"], int(sig["r"], 16), int(sig["s"], 16)),
        )
        assert who.lower() == signer.address


async def test_stream_method_builds_selector(gateway):
    async with AsyncGrvt(gateway.env, **gateway.client_options) as client:
        sub = client.orderbook_delta_stream(instrument="BTC_USDT_Perp", rate=500)
        assert isinstance(sub, Subscription)
        await sub.start()
        assert gateway.subscribes()[0]["params"]["selectors"] == ["BTC_USDT_Perp@500"]
        assert client.trade_stream(instrument="X").selector == "X"
        assert client.order_stream(sub_account_id=123, instrument="BTC_USDT_Perp").selector == "123-BTC_USDT_Perp"


def test_sync_client(gateway_sync):
    client = Grvt(gateway_sync.env, **gateway_sync.client_options)
    try:
        out = client.get_instrument(instrument="X")
        assert out.result is None or isinstance(out, m.ApiGetInstrumentResponse)
        sub = client.ticker_delta_stream(instrument="S", rate=500)
        assert isinstance(sub, SyncSubscription)
        with sub:
            first = next(iter(sub))
        assert first.sequence_number == "0"
    finally:
        client.close()


def test_only_snapshot_streams_may_drop():
    streams = [v for v in vars(specs).values() if isinstance(v, StreamSpec)]
    assert len(streams) == len(CONTRACT["streams"])
    for s in streams:
        assert s.conflate == s.snapshot_feed, s.stream


async def test_transfer_and_withdrawal_need_no_signature_from_the_caller(gateway):
    def rest(route, body):
        if route == "currency":
            return 200, {
                "result": [{"id": 3, "symbol": "USDT", "balance_decimals": 6, "quantity_multiplier": "1000000"}]
            }
        return 200, {"result": {"ack": True}} if route == "withdrawal" else {"result": {"ack": True, "tx_id": "1"}}

    gateway.rest_handler = rest
    async with AsyncGrvt(
        gateway.env, **gateway.client_options, api_key="test-key", signer=PrivateKeySigner(KEY)
    ) as client:
        await client.transfer(
            from_account_id="0x" + "11" * 20,
            from_sub_account_id=0,
            to_account_id="0x" + "11" * 20,
            to_sub_account_id=1,
            currency="USDT",
            num_tokens=Decimal("1.5"),
            transfer_type=grvt.enums.TransferType.STANDARD,
        )
        sent = gateway.rest_calls[-1][1]
        assert sent["signature"]["r"].startswith("0x") and "from_wallet_type" not in sent
        await client.withdrawal(
            from_account_id="0x" + "11" * 20,
            to_eth_address="0x" + "22" * 20,
            currency="USDT",
            num_tokens=Decimal("1.5"),
        )
        assert gateway.rest_calls[-1][1]["signature"]["r"].startswith("0x")


CUSTOM = Environment(
    name="custom",
    md_url="https://md.gateway.example.com",
    td_url="https://td.gateway.example.com",
    edge_url="https://edge.gateway.example.com",
    md_ws_url="wss://md.gateway.example.com/ws/full",
    td_ws_url="wss://td.gateway.example.com/ws/full",
    md_ws_lite_url="wss://md.gateway.example.com/ws/lite",
    td_ws_lite_url="wss://td.gateway.example.com/ws/lite",
    chain_id=4242,
)


async def test_custom_environment_is_used_as_given():
    async with AsyncGrvt(CUSTOM) as client:
        assert client.rt.env == CUSTOM
        assert client.rt.domain.chain_id == 4242  # signs for the custom chain, not a named environment's
    for field, empty in [
        *((f, "") for f in ("md_url", "td_url", "edge_url", "md_ws_url", "td_ws_url")),
        ("chain_id", 0),
    ]:
        with pytest.raises(ValueError, match=field):
            AsyncGrvt(dataclasses.replace(CUSTOM, **{field: empty}))
    assert sorted(ENVIRONMENTS) == ["production", "testnet"]
    assert grvt.PRODUCTION is ENVIRONMENTS["production"]
    with pytest.raises(grvt.ConfigError, match="unknown env"):
        AsyncGrvt("prod")


def test_required_models_and_sdk_filled_inputs():
    from grvt._runtime import codec

    with pytest.raises(TypeError, match="instrument"):
        m.OrderLeg(size=Decimal("1"), is_buying_asset=False)  # pyright: ignore[reportCallIssue]
    leg = m.OrderLeg(instrument="X", size=Decimal("1"), is_buying_asset=False)
    assert codec.decode(codec.encode(leg))["is_buying_asset"] is False
    body = {"sub_account_id": "1", "time_in_force": "GOOD_TILL_TIME", "legs": []}
    assert codec.convert(body, m.OrderInput).signature is None
    with pytest.raises(msgspec.ValidationError, match="signature"):
        codec.convert(body, m.Order)


def test_required_nullable_list_is_different_from_an_absent_list():
    from grvt._runtime import codec

    body = {"event_time": "1", "instrument": "X", "bids": None, "asks": []}
    book = codec.convert(body, m.OrderbookLevels)
    assert book.bids is None and book.asks == []
    del body["bids"]
    with pytest.raises(msgspec.ValidationError, match="bids"):
        codec.convert(body, m.OrderbookLevels)
