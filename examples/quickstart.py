"""Read testnet data; add --trade to place, observe and cancel one testnet order."""

import argparse
import asyncio
import os
import secrets
import sys
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any
from urllib.parse import urlparse

from grvt import AsyncGrvt, Environment, OrderStateUnknown, PrivateKeySigner, enums, models, resolve_order


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade", action="store_true", help="send one real testnet order; it may fill")
    parser.add_argument("--instrument", default="BTC_USDT_Perp")
    parser.add_argument("--max-notional", type=Decimal, default=Decimal("200"), help="quote asset spending limit")
    parser.add_argument("--fixture-url", help=argparse.SUPPRESS)
    args = parser.parse_args()
    env = "testnet"
    encoding = "lite"
    if args.fixture_url:
        url = urlparse(args.fixture_url)
        if (
            url.scheme != "http"
            or url.hostname != "127.0.0.1"
            or url.path
            or url.query
            or url.fragment
            or url.username
            or url.password
            or not url.port
        ):
            parser.error("fixture URL must be an HTTP loopback origin")
        env = Environment(
            "fixture",
            args.fixture_url,
            args.fixture_url,
            args.fixture_url,
            args.fixture_url.replace("http:", "ws:"),
            args.fixture_url.replace("http:", "ws:"),
            326,
        )
        encoding = "full"  # the local fixture gateway speaks full names only
    credentials: dict[str, Any] = {}
    account = ""
    if args.trade:
        names = ("GRVT_API_KEY", "GRVT_PRIVATE_KEY", "GRVT_SUB_ACCOUNT_ID")
        if any(not os.environ.get(name, "").strip() for name in names):
            parser.error("trading requires GRVT_API_KEY, GRVT_PRIVATE_KEY and GRVT_SUB_ACCOUNT_ID")
        account = os.environ["GRVT_SUB_ACCOUNT_ID"]
        if not account.isascii() or not account.isdigit() or not 0 < int(account) < 2**64:
            parser.error("GRVT_SUB_ACCOUNT_ID must be a positive uint64")
        credentials = dict(api_key=os.environ["GRVT_API_KEY"], signer=PrivateKeySigner(os.environ["GRVT_PRIVATE_KEY"]))
        if not args.max_notional.is_finite() or args.max_notional <= 0:
            parser.error("max-notional must be finite and positive")
    async with AsyncGrvt(env=env, encoding=encoding, http_attempt_timeout=5, **credentials) as client:
        ticker = (await client.ticker(instrument=args.instrument)).result
        instrument = (await client.get_instrument(instrument=args.instrument)).result
        print("mark price", ticker.mark_price, flush=True)
        if not args.trade:
            return 0
        tick, step = instrument.tick_size, instrument.min_size
        bid = ticker.best_bid_price
        if tick <= 0 or step <= 0 or bid is None or bid <= tick:
            raise ValueError("instrument has no usable bid, tick or size")
        price = ((bid / tick).to_integral_value(rounding=ROUND_FLOOR) - 1) * tick
        if price <= 0:
            print("no positive passive price; no order sent", file=sys.stderr)
            return 1
        count = max(Decimal(1), (instrument.min_notional / (price * step)).to_integral_value(rounding=ROUND_CEILING))
        size = step * count
        if size * price > args.max_notional:
            raise ValueError("minimum order exceeds max-notional; no order sent")
        order_id = str(secrets.randbits(63) | (1 << 63))
        print("client_order_id", order_id, "price", price, "size", size, flush=True)
        order = models.OrderInput(
            sub_account_id=account,
            time_in_force=enums.TimeInForce.GOOD_TILL_TIME,
            post_only=True,
            legs=[models.OrderLeg(instrument=args.instrument, size=size, limit_price=price, is_buying_asset=True)],
            metadata=models.OrderMetadataInput(client_order_id=order_id),
        )
        try:
            await client.create_order(order=order)
        except OrderStateUnknown:
            print("create outcome unknown; looking up the same ID", flush=True)
        # Acknowledgements do not prove terminal state. Observe before and after one cancel.
        cancelled = False
        for _ in range(8):
            try:
                found = await resolve_order(client, sub_account_id=account, client_order_id=order_id)
            except Exception as error:
                print("lookup unavailable:", type(error).__name__, flush=True)
                await asyncio.sleep(0.5)
                continue
            if found.status == "found" and found.order.state:
                status = found.order.state.status.value
                print("order status", status, flush=True)
                if status in ("CANCELLED", "FILLED", "REJECTED"):
                    return 0 if status != "REJECTED" else 3
                if status == "OPEN" and not cancelled:
                    cancelled = True
                    try:
                        await client.cancel_order(sub_account_id=account, client_order_id=order_id)
                    except Exception as error:
                        # Even a failed cancel may have reached the server. Observe; do not retry.
                        print("cancel response unavailable:", type(error).__name__, flush=True)
            await asyncio.sleep(0.5)
        print("outcome still unknown; retain client_order_id and reconcile manually; do not resend", flush=True)
        return 2


async def bounded_main():
    try:
        async with asyncio.timeout(60):
            return await main()
    except TimeoutError:
        print("deadline reached; retain any printed client_order_id and reconcile; do not resend", flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(bounded_main()))
