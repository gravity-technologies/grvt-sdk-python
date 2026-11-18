"""The lite encoding: per-type tables, encode/decode, the lite envelope, and lite clients on the fake gateway.

The shared runtime scenarios also run in lite (test_scenarios.py), and so do the generated round trips that take
`any_gateway`; the lite fake fails any test whose client sent a key that is not a lite name.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

import msgspec
import pytest
from conftest import CONTRACT, instrument_payload

from grvt import AsyncGrvt
from grvt import models as m
from grvt._generated import lite as gen
from grvt._generated import specs
from grvt._runtime import ApiError, Environment, RpcSpec, Runtime, StreamSpec, codec, lite
from grvt._runtime.env import TESTNET
from grvt._runtime.errors import response_error
from grvt._runtime.protocol import jsonrpc_frame
from grvt._runtime.scalars import Timestamp

GATEWAY_TYPES = {t["name"]: t["fields"] or [] for t in CONTRACT["types"]}
MODELS = sorted(gen.TYPES, key=lambda t: t.__name__)


def test_twins_match_the_contract():
    rpcs = [s for s in vars(specs).values() if isinstance(s, RpcSpec) and s.gateway != "edge"]
    streams = [s for s in vars(specs).values() if isinstance(s, StreamSpec)]
    needed = {s.request_type for s in rpcs} | {s.response_type for s in rpcs} | {s.feed_type for s in streams}
    assert needed <= set(gen.TYPES)
    for model in MODELS:
        twin = gen.TYPES[model][0]
        mf, tf = msgspec.structs.fields(model), msgspec.structs.fields(twin)
        # Same attributes, types and defaults as the model; only the JSON names differ.
        assert [(f.name, f.type, f.required) for f in mf] == [(f.name, f.type, f.required) for f in tf] or [
            (f.name, f.required) for f in mf
        ] == [(f.name, f.required) for f in tf], model
        assert len({f.encode_name for f in tf}) == len(tf), model
        # SDK copies (FooInput, PartialFoo) keep the wire type's names, so only wire types are compared here.
        wire = GATEWAY_TYPES.get(model.__name__)
        if wire is not None:
            assert [(f["name"], f["lite"]) for f in wire] == [
                (a.encode_name, b.encode_name) for a, b in zip(mf, tf, strict=True)
            ]
    edge = {t["name"] for t in CONTRACT["edge"]["types"]}
    assert not edge & {t.__name__ for t in gen.TYPES}, "edge types have no lite form"


def test_rpc_specs_have_lite_paths_except_edge():
    for spec in vars(specs).values():
        if isinstance(spec, RpcSpec):
            if spec.gateway == "edge":
                assert spec.lite_path is None and spec.request_type is None
            else:
                assert spec.lite_path == spec.path.replace("/full/", "/lite/", 1)


def sample(t: msgspec.inspect.Type, depth: int = 0):
    """A full-name builtins value of the type with every field set."""
    if isinstance(t, msgspec.inspect.UnionType):
        return sample(next(x for x in t.types if not isinstance(x, msgspec.inspect.NoneType)), depth)
    if isinstance(t, msgspec.inspect.StructType):
        if depth > 20:  # no contract type nests this deep; guards a recursive one
            return None
        return {f.encode_name: sample(f.type, depth + 1) for f in t.fields}
    if isinstance(t, msgspec.inspect.ListType):
        item = sample(t.item_type, depth)
        return None if item is None else [item, item]
    if isinstance(t, msgspec.inspect.EnumType):
        return next(iter(t.cls)).value
    if isinstance(t, msgspec.inspect.CustomType) and t.cls is Timestamp:
        return "1700000000000000001"
    return {
        msgspec.inspect.IntType: 7,
        msgspec.inspect.BoolType: True,
        msgspec.inspect.DecimalType: "1.25",
    }.get(type(t), "x")


@pytest.mark.parametrize("model", MODELS, ids=lambda t: t.__name__)
def test_every_type_round_trips(model):
    full = sample(msgspec.inspect.type_info(model))
    value = codec.convert(full, model)
    wire = lite.encode(model, value)
    keys = set(msgspec.json.decode(wire))
    assert keys == {f.encode_name for f in msgspec.structs.fields(gen.TYPES[model][0])} & keys
    decoded = lite.decode(wire, model)
    assert type(decoded) is model and decoded == value  # every nested twin became its model too
    assert lite.encode(model, full) == wire  # a full-name dict encodes the same


def test_repeated_names_follow_the_type():
    # "s" and "s1" are the stream and selector of the frame, the status and session of the ticker inside it.
    frame = (
        b'{"s":"v1.ticker.s","s1":"BTC_USDT_Perp@500","sn":"7","ps":"6",'
        b'"f":{"i":"BTC_USDT_Perp","s":"TRADING","s1":"REGULAR","mp":"65000.5","fi":"9"}}'
    )
    got = lite.decode(frame, m.WSTickerFeedDataV1)
    assert got.stream == "v1.ticker.s" and got.selector == "BTC_USDT_Perp@500" and got.sequence_number == "7"
    assert got.feed.status == "TRADING" and got.feed.session == "REGULAR"
    assert got.feed.mark_price == Decimal("65000.5")  # "fi" is not in the contract: skipped

    # And "s" is the size of a book level, repeated in a list inside the frame.
    book = (
        b'{"s":"v1.book.s","s1":"BTC_USDT_Perp@500-10","sn":"9","ps":"8","f":{"et":"1700000000000000000",'
        b'"i":"BTC_USDT_Perp","b":[{"p":"64999.9","s":"1.5","no":3}],"a":[{"p":"65000.1","s":"0.25","no":1}]}}'
    )
    full = (
        b'{"stream":"v1.book.s","selector":"BTC_USDT_Perp@500-10","sequence_number":"9","prev_sequence_number":"8",'
        b'"feed":{"event_time":"1700000000000000000","instrument":"BTC_USDT_Perp",'
        b'"bids":[{"price":"64999.9","size":"1.5","num_orders":3}],'
        b'"asks":[{"price":"65000.1","size":"0.25","num_orders":1}]}}'
    )
    assert lite.decode(book, m.WSOrderbookLevelsFeedDataV1) == codec.decode(full, m.WSOrderbookLevelsFeedDataV1)
    encoded = lite.encode(m.WSOrderbookLevelsFeedDataV1, codec.decode(full, m.WSOrderbookLevelsFeedDataV1))
    assert msgspec.json.decode(encoded) == msgspec.json.decode(book)


def test_decode_errors_name_the_lite_field():
    with pytest.raises(msgspec.ValidationError, match=r"\$\.f\.b\[0\]\.s"):
        lite.decode(
            b'{"s":"v1.book.s","s1":"x","sn":"1","ps":"0","f":{"et":"1","i":"X","b":[{"p":"1","s":true,"no":1}],'
            b'"a":[]}}',
            m.WSOrderbookLevelsFeedDataV1,
        )


def test_encode_rejects_a_key_the_type_does_not_have():
    with pytest.raises(ValueError, match="ApiGetInstrumentRequest has no field 'instrument_name'"):
        lite.encode(m.ApiGetInstrumentRequest, {"instrument": "X", "instrument_name": "BTC_USDT_Perp"})
    with pytest.raises(ValueError, match="has no field 'order.legs.qty'"):
        order = {"sub_account_id": "1", "time_in_force": "GOOD_TILL_TIME"}
        lite.encode(
            m.ApiCreateOrderRequestInput,
            {"order": {**order, "legs": [{"instrument": "X", "size": "1", "is_buying_asset": True, "qty": "1"}]}},
        )
    with pytest.raises(TypeError, match="EdgeLoginResponse is not a gateway type"):
        lite.encode(m.EdgeLoginResponse, {})


def test_envelope_frames():
    assert lite.jsonrpc_frame("subscribe", b'{"s":"v1.book.s"}', 7) == (
        b'{"j":"2.0","m":"subscribe","p":{"s":"v1.book.s"},"i":7}'
    )
    reply = lite.decode_frame(b'{"j":"2.0","r":{"r":{"a":true}},"i":3,"m":"v1/cancel_order"}')
    assert reply.id == 3 and reply.method == "v1/cancel_order"
    assert lite.decode(bytes(reply.result), m.AckResponse) == m.AckResponse(result=m.Ack(ack=True))
    # Error replies may carry id 0 and an empty method.
    err = lite.decode_frame(b'{"j":"2.0","e":{"c":1003,"m":"bad request"},"i":0,"m":""}')
    assert err.id == 0 and lite.error(err.error) == {"code": 1003, "message": "bad request"}
    feed = lite.decode_frame(b'{"s":"v1.book.s","s1":"X@500-10","sn":"5","ps":"4","f":{}}')
    assert (feed.stream, feed.selector, feed.sequence_number, feed.prev_sequence_number) == (
        "v1.book.s",
        "X@500-10",
        5,
        4,
    )
    # A REST-style error body: "m" is its message and "s" its status.
    body = lite.decode_frame(b'{"c":1000,"m":"unauthorized","s":401}')
    assert body.code == 1000 and body.message == "unauthorized" and body.id is None
    error = response_error(lite.http_error({"c": 2001, "m": "expired", "s": 401, "ri": 9}), 401, "v1/create_order")
    assert isinstance(error, ApiError) and error.code == 2001 and error.status == 401


async def test_full_client_keeps_the_full_wire():
    async with Runtime("testnet", encoding="full") as rt:
        socket = rt.socket("md")
        assert socket.url == TESTNET.md_ws_url and not socket.lite
        assert socket._decode_frame is codec.decode_frame and socket._frame is jsonrpc_frame
    async with Runtime("testnet") as rt:  # lite is the default
        socket = rt.socket("md")
        assert socket.url == "wss://market-data.testnet.grvt.io/ws/lite" and socket.lite
        assert rt.socket("td").url == "wss://trades.testnet.grvt.io/ws/lite"


def test_encoding_option_is_validated():
    with pytest.raises(ValueError, match="encoding must be 'full' or 'lite'"):
        Runtime("testnet", encoding="compact")  # pyright: ignore[reportArgumentType]


def test_custom_env_lite_ws_urls():
    custom = Environment("custom", "https://a", "https://a", "https://a", "wss://md/ws/full", "wss://td/ws/full", 1)
    rt = Runtime(custom)  # lite is the default: the lite URLs come from the full ones
    assert rt.socket("md").url == "wss://md/ws/lite" and rt.socket("td").url == "wss://td/ws/lite"
    rt = Runtime(dataclasses.replace(custom, td_ws_lite_url="wss://other/lite"))
    assert rt.socket("md").url == "wss://md/ws/lite" and rt.socket("td").url == "wss://other/lite"  # explicit wins

    odd = dataclasses.replace(custom, md_ws_url="wss://md/stream")
    Runtime(odd, encoding="full")  # full needs no lite URLs
    with pytest.raises(ValueError, match="does not end in /ws/full.*set md_ws_lite_url"):
        Runtime(odd)  # reported when the client is built
    assert Runtime(dataclasses.replace(odd, md_ws_lite_url="wss://md/lite")).socket("md").url == "wss://md/lite"


async def test_lite_reads_match_full(gateway, lite_gateway):
    """The same call on each fake decodes to the same model; the lite one went to the lite path with lite keys."""
    got = []
    for gw in (gateway, lite_gateway):
        async with AsyncGrvt(gw.env, **gw.client_options) as client:
            got.append(await client.get_instrument(instrument="ETH_USDT_Perp"))
    assert got[0] == got[1] and got[1].result.instrument == "ETH_USDT_Perp"
    assert lite_gateway.rest_calls[-1][1] == {"instrument": "ETH_USDT_Perp"}  # the fake renamed {"i": ...} back


async def test_lite_http_error_body(lite_gateway):
    lite_gateway.rest_handler = lambda route, body: (400, {"code": 1003, "message": "malformed", "status": 400})
    async with AsyncGrvt(lite_gateway.env, **lite_gateway.client_options) as client:
        with pytest.raises(ApiError) as ei:
            await client.get_instrument(instrument="BTC_USDT_Perp")
    assert ei.value.code == 1003 and ei.value.status == 400 and "malformed" in str(ei.value)


async def test_lite_ws_rpc_and_error(lite_gateway):
    lite_gateway.ws_results["v1/cancel_order"] = {"result": {"ack": True}}  # double-wrapped, as the server does
    async with AsyncGrvt(lite_gateway.env, api_key="test-key", order_transport="ws", encoding="lite") as client:
        ack = await client.cancel_order(sub_account_id=1, client_order_id=2)
        assert ack == m.AckResponse(result=m.Ack(ack=True))
        sent = next(r for r in lite_gateway.received if r["method"] == "v1/cancel_order")
        assert sent["params"]["sub_account_id"] == "1" and sent["params"]["client_order_id"] == "2"


async def test_lite_book_stream_matches_full(gateway, lite_gateway):
    snapshot = {"event_time": "1", "instrument": "BTC_USDT_Perp", "bids": [instrument_level()], "asks": []}
    got = []
    for gw in (gateway, lite_gateway):
        gw.snapshot_feeds["v1.book.s"] = snapshot
        async with AsyncGrvt(gw.env, **gw.client_options) as client:
            async with client.orderbook_snap_stream(instrument="BTC_USDT_Perp", rate=500, depth=10) as sub:
                got.append(await anext(sub))
    assert got[0] == got[1] and got[1].feed.bids[0].size == Decimal("2")
    assert lite_gateway.subscribes()[0]["params"]["selectors"] == ["BTC_USDT_Perp@500-10"]


def instrument_level() -> dict:
    return {"price": "1", "size": "2", "num_orders": 3}


def test_instrument_payload_has_only_contract_fields():
    # The fake's instrument fixture must survive the lite fake's renaming, or the lite tests prove nothing.
    fields = {f["name"] for f in GATEWAY_TYPES["InstrumentDisplay"]}
    assert set(instrument_payload()) <= fields
