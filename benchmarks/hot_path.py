"""Hot-path timings: order hashing and signing, request encoding, reply and stream decoding.

Run from sdk/languages/python after `make sdk-gen-python`:

    uv run --locked python benchmarks/hot_path.py

Each line is the best of 7 runs, in microseconds per call. The inputs are fixed: a 2-leg order and a
10+10 level `v1.book.d` frame (about 1.2 KB).
"""

from __future__ import annotations

import asyncio
import json
import timeit
from collections.abc import Callable, Coroutine
from typing import Any

import msgspec

from grvt._generated import models as m
from grvt._generated import signing, specs
from grvt._generated.signing import EIP712_ORDER
from grvt._runtime import FixedClock, PrivateKeySigner, Runtime, codec, grvt_domain, lite
from grvt._runtime.hooks import Hooks
from grvt._runtime.ws import Subscription, WsSocket

KEY = "0x" + "11" * 32
SIGNER = PrivateKeySigner(KEY)
DOMAIN = grvt_domain(326)
MSG: dict[str, Any] = {
    "subAccountID": 8289849667772468,
    "isMarket": False,
    "timeInForce": 1,
    "postOnly": False,
    "reduceOnly": False,
    "legs": [
        {
            "assetID": 0x030501,
            "contractSize": 1_200_000_000,
            "limitPrice": 65_000_100_000_000,
            "isBuyingContract": True,
        },
        {
            "assetID": 0x030502,
            "contractSize": 3_000_000_000,
            "limitPrice": 3_100_000_000_000,
            "isBuyingContract": False,
        },
    ],
    "nonce": 1234567,
    "expiration": 1_730_800_000_000_000_000,
}


def instrument(name: str, hash_: str) -> dict[str, Any]:
    return {
        "instrument": name,
        "instrument_hash": hash_,
        "base": name.split("_")[0],
        "quote": "USDT",
        "kind": "PERPETUAL",
        "base_decimals": 9,
        "quote_decimals": 6,
        "tick_size": "0.1",
        "min_size": "0.001",
        "min_notional": "0",
    }


# get_all_instruments: 100 instruments as the gateway lists them (about 40 KB).
ALL_INSTRUMENTS = json.dumps(
    {
        "result": [
            {
                **instrument(f"COIN{i}_USDT_Perp", hex(0x030000 + i)),
                "venues": ["ORDERBOOK", "RFQ"],
                "settlement_period": "PERPETUAL",
                "create_time": "1700000000000000000",
                "max_position_size": "1000000",
                "funding_interval_hours": 8,
                "adjusted_funding_rate_cap": "0.003",
                "adjusted_funding_rate_floor": "-0.003",
                "status": "TRADING",
            }
            for i in range(100)
        ]
    }
).encode()


ORDER_REQUEST = m.ApiCreateOrderRequestInput(
    order=m.OrderInput(
        sub_account_id="8289849667772468",
        time_in_force=m.e.TimeInForce.GOOD_TILL_TIME,
        legs=[
            m.OrderLeg(
                instrument="BTC_USDT_Perp",
                size=m.Decimal("1.2"),
                limit_price=m.Decimal("65000.1"),
                is_buying_asset=True,
            ),
            m.OrderLeg(
                instrument="ETH_USDT_Perp", size=m.Decimal("3"), limit_price=m.Decimal("3100"), is_buying_asset=False
            ),
        ],
        metadata=m.OrderMetadataInput(client_order_id="123456789"),
    )
)


def _level(price: str, size: str, n: int) -> dict[str, Any]:
    return {"price": price, "size": size, "num_orders": n}


BOOK_FRAME = json.dumps(
    {
        "stream": "v1.book.d",
        "selector": "BTC_USDT_Perp@500",
        "sequence_number": "872634876",
        "feed": {
            "event_time": "1793172225200080949",
            "instrument": "BTC_USDT_Perp",
            "bids": [_level(f"{65000 - i * 0.5:.1f}", f"{0.125 * (i + 1):.3f}", i + 1) for i in range(10)],
            "asks": [_level(f"{65000.5 + i * 0.5:.1f}", f"{0.25 * (i + 1):.3f}", i + 2) for i in range(10)],
        },
        "prev_sequence_number": "872634875",
    },
    separators=(",", ":"),
).encode()

ORDER_REPLY = json.dumps(
    {
        "result": {
            "order_id": "0x1028403",
            "sub_account_id": "8289849667772468",
            "is_market": False,
            "time_in_force": "GOOD_TILL_TIME",
            "post_only": False,
            "reduce_only": False,
            "legs": [
                {"instrument": "BTC_USDT_Perp", "size": "1.2", "limit_price": "65000.1", "is_buying_asset": True},
                {"instrument": "ETH_USDT_Perp", "size": "3", "limit_price": "3100", "is_buying_asset": False},
            ],
            "signature": {
                "signer": "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
                "r": "0x" + "ab" * 32,
                "s": "0x" + "cd" * 32,
                "v": 28,
                "expiration": "1730800000000000000",
                "nonce": 1234567,
                "chain_id": "326",
            },
            "metadata": {"client_order_id": "123456789", "create_time": "1730700000000000000"},
            "state": {
                "status": "OPEN",
                "reject_reason": "UNSPECIFIED",
                "book_size": ["1.2", "3"],
                "traded_size": ["0", "0"],
                "update_time": "1730700000000000000",
                "avg_fill_price": ["0", "0"],
            },
        }
    },
    separators=(",", ":"),
).encode()


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Drive a coroutine that never suspends (cached metadata, in-process key)."""
    try:
        coro.send(None)
    except StopIteration as e:
        return e.value
    raise AssertionError("coroutine suspended")


def make_runtime() -> Runtime:
    rt = Runtime("testnet", signer=SIGNER, clock=FixedClock(1_730_700_000_000_000_000))
    rt.metadata.put("instrument", "BTC_USDT_Perp", instrument("BTC_USDT_Perp", "0x030501"))
    rt.metadata.put("instrument", "ETH_USDT_Perp", instrument("ETH_USDT_Perp", "0x030502"))
    return rt


def make_stream(*, lite: bool = False) -> tuple[WsSocket, Subscription[Any]]:
    sock = WsSocket("md", "ws://unused", None, Hooks(None, None), lite=lite)
    sub: Subscription[Any] = Subscription(sock, specs.STREAM_ORDERBOOK_ORDERBOOK_DELTA, "BTC_USDT_Perp@500", 4096)
    sub._started = True  # pyright: ignore[reportPrivateUsage]  (already subscribed: skip the network)
    sub.key = ("v1.book.d", "BTC_USDT_Perp@500")
    sock._by_key[sub.key] = sub  # pyright: ignore[reportPrivateUsage]
    return sock, sub


def measure(name: str, fn: Callable[[], Any], n: int = 20000) -> float:
    fn()
    best = min(timeit.repeat(fn, number=n, repeat=7)) / n * 1e6
    print(f"{name:28s} {best:7.2f} us")
    return best


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    rt = make_runtime()
    sock, sub = make_stream()
    signed = run(signing.sign_order_create_order(rt, ORDER_REQUEST))
    feed_type = specs.STREAM_ORDERBOOK_ORDERBOOK_DELTA.feed_type
    plain = msgspec.json.Decoder()

    def stream_route_decode() -> Any:
        sub.seq.on_ack(872634875, 0)  # the frame is always the next one in sequence
        sock._on_frame(BOOK_FRAME)  # pyright: ignore[reportPrivateUsage]
        return run(sub.__anext__())

    # The same frame and order in the lite encoding (encoding="lite"): renamed per type at decode and encode.
    lite_sock, lite_sub = make_stream(lite=True)
    lite_frame = lite.encode(feed_type, codec.decode(BOOK_FRAME, feed_type))
    request_type = specs.ORDER_CREATE_ORDER.request_type
    all_type = m.ApiGetAllInstrumentsResponse
    lite_all = lite.encode(all_type, codec.decode(ALL_INSTRUMENTS, all_type))

    def lite_stream_route_decode() -> Any:
        lite_sub.seq.on_ack(872634875, 0)
        lite_sock._on_frame(lite_frame)  # pyright: ignore[reportPrivateUsage]
        return run(lite_sub.__anext__())

    print(f"Python SDK hot paths (book frame {len(BOOK_FRAME)} bytes)")
    measure("order hash", lambda: EIP712_ORDER.digest(DOMAIN, MSG))
    measure("order hash+sign", lambda: SIGNER.sign_digest(EIP712_ORDER.digest(DOMAIN, MSG)))
    measure("sign create_order (model)", lambda: run(signing.sign_order_create_order(rt, ORDER_REQUEST)))
    measure("encode create_order", lambda: codec.encode(signed))
    measure("decode create_order reply", lambda: codec.decode(ORDER_REPLY, m.ApiCreateOrderResponse))
    measure("decode book.d", lambda: codec.decode(BOOK_FRAME, feed_type))
    measure("stream route+decode book.d", stream_route_decode)
    measure("msgspec untyped book.d", lambda: plain.decode(BOOK_FRAME))
    measure(
        f"decode all_instruments {len(ALL_INSTRUMENTS) // 1000}KB", lambda: codec.decode(ALL_INSTRUMENTS, all_type), 500
    )
    print(f"lite encoding (book frame {len(lite_frame)} bytes)")
    measure("encode create_order", lambda: lite.encode(request_type, signed))
    measure("decode book.d", lambda: lite.decode(lite_frame, feed_type))
    measure("stream route+decode book.d", lite_stream_route_decode)
    measure(f"decode all_instruments {len(lite_all) // 1000}KB", lambda: lite.decode(lite_all, all_type), 500)
    loop.run_until_complete(rt.close())
    loop.close()


if __name__ == "__main__":
    main()
