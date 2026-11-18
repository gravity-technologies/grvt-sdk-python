"""Replays sdk/scenarios/runtime.json, the runtime scenarios every SDK shares, against the fake gateway.

The format is described in sdk/scenarios/README.md.
"""

from __future__ import annotations

import asyncio
import re
from decimal import Decimal
from typing import Any

import aiohttp
import pytest
from conftest import DROP, instrument_payload, wait_until
from shared_data import load_shared

import grvt
from grvt import (
    AsyncGrvt,
    GapRange,
    PaginationCycle,
    PrivateKeySigner,
    StreamOverflow,
    backfill_pages,
    backfill_pages_sync,
    pages,
    pages_sync,
    refresh_cancel_countdown,
)
from grvt import models as m
from grvt._runtime import ApiError, DecodeError, EventKind, OrderStateUnknown, RateLimitError, TransportError
from grvt._runtime.transport import AiohttpTransport

BEHAVIOR = load_shared("contract/behavior.json")
KEY = "0xbfe25ff07bd38cb3fbc28a2a296d41aabd68a78c929d257935906489fc9c6d89"
REF = re.compile(r"^\$([a-z_]+)(?:\.(\d+))?(?:\*(\d+))?([+-]\d+)?$")


def resolve(v: Any) -> Any:
    """Replaces every "$key", "$key.index", "$key*n" and "$key+n" string with the value from behavior.json."""
    if isinstance(v, list):
        return [resolve(e) for e in v]
    if isinstance(v, dict):
        return {k: resolve(e) for k, e in v.items()}
    ref = REF.match(v) if isinstance(v, str) else None
    if ref is None:
        return v
    value = BEHAVIOR[ref[1]]["value"]
    if ref[2] is not None:
        value = value[int(ref[2])]
    if ref[3] is not None:
        value *= int(ref[3])
    if ref[4] is not None:
        value += int(ref[4])
    return value


SCENARIOS = resolve(load_shared("scenarios/runtime.json"))


class Script:
    """The scripted replies in order; the last one repeats."""

    def __init__(self, replies: list[dict]) -> None:
        self.replies = list(replies)
        self.current: dict = {}
        self.attempts = 0

    def next(self) -> dict:
        self.attempts += 1
        if len(self.replies) > 1:
            self.current = self.replies.pop(0)
        elif self.replies:
            self.current = self.replies[0]
        return self.current


class ScriptedTransport(AiohttpTransport):
    """Counts the attempts at one path and refuses a scripted one before connecting, as a failed connect does."""

    def __init__(self, path: str, script: Script) -> None:
        super().__init__()
        self.path, self.script = path, script

    async def post(self, url, body, headers, timeout):
        if url.endswith(self.path) and self.script.next().get("refuse"):
            raise TransportError("connect failed: connection refused", sent=False)
        return await super().post(url, body, headers, timeout)


def outcome(err: BaseException | None) -> str:
    for kind, name in (
        (OrderStateUnknown, "order_state_unknown"),
        (DecodeError, "decode_error"),
        (RateLimitError, "rate_limited"),
        (ApiError, "api_error"),
        (TransportError, "transport_error"),
        (ValueError, "validation_error"),
    ):
        if isinstance(err, kind):
            return name
    return "ok" if err is None else f"unexpected {err!r}"


def order(client_order_id: str | None) -> m.OrderInput:
    return m.OrderInput(
        sub_account_id="8289849667772468",
        time_in_force=grvt.enums.TimeInForce.GOOD_TILL_TIME,
        legs=[
            m.OrderLeg(
                instrument="BTC_USDT_Perp",
                size=Decimal("0.01"),  # pyright: ignore[reportArgumentType]
                limit_price=Decimal("65000.1"),  # pyright: ignore[reportArgumentType]
                is_buying_asset=True,
            )
        ],
        metadata=None if client_order_id is None else m.OrderMetadataInput(client_order_id=client_order_id),
    )


async def run_call(gateway, sc: dict) -> None:
    call, want, replies = sc["call"], sc["expect"], sc["replies"]
    ws = call.get("transport") == "ws"
    route = {"read": "ticker", "write": "create_order", "countdown": "cancel_on_disconnect"}[call["kind"]]
    script = Script(replies)

    def rest(r, body):
        if r == "instrument":
            return 200, {"result": instrument_payload(body["instrument"])}
        if r != route:
            return 200, {"result": {}}
        reply = script.current
        if reply.get("drop"):
            return 200, DROP
        if "raw" in reply:
            return reply["status"], reply["raw"].encode()
        return reply["status"], reply["body"]

    gateway.rest_handler = rest
    events: list = []
    options: dict[str, Any] = {
        **gateway.client_options,
        "transport": ScriptedTransport(f"/{route}", script),
        "on_event": events.append,
    }
    if ws:
        options["order_transport"] = "ws"
        gateway.ignore_rpcs.add("v1/create_order")
    if "ws_request_timeout_ms" in call:
        options["ws_request_timeout"] = call["ws_request_timeout_ms"] / 1000
    reply = replies[0] if replies else {}
    async with AsyncGrvt(gateway.env, api_key="test-key", signer=PrivateKeySigner(KEY), **options) as client:
        if ws and (reply.get("refuse") or reply.get("send_fails")):
            socket = await client.rt.socket("td").connected()
            send_str = socket.send_str

            # aiohttp's own errors: a closing transport refuses before writing; a lost connection after.
            async def broken_send(data: str, compress: int | None = None) -> None:
                if reply.get("send_fails"):
                    await send_str(data, compress)
                    raise aiohttp.ClientConnectionResetError("Connection lost")
                raise aiohttp.ClientConnectionResetError("Cannot write to closing transport")

            socket.send_str = broken_send  # type: ignore[method-assign]

        if call["kind"] == "read":
            task = asyncio.create_task(client.ticker(instrument="BTC_USDT_Perp"))
        elif call["kind"] == "countdown":
            task = asyncio.create_task(
                refresh_cancel_countdown(client, sub_account_id="123", countdown_seconds=call["seconds"])
            )
        else:
            task = asyncio.create_task(client.create_order(order=order(call.get("client_order_id"))))

        def sent() -> list[dict]:
            return [r["params"] for r in gateway.received if r["method"] == "v1/create_order"]

        if ws and not (reply.get("refuse") or reply.get("send_fails")):
            await wait_until(lambda: len(sent()) == 1)
            request = next(r for r in gateway.received if r["method"] == "v1/create_order")
            if reply.get("drop"):
                await gateway.drop_all()
            elif "error" in reply:
                await gateway.conns[0].send_json({"id": request["id"], "error": reply["error"]})
        try:
            await task
            err = None
        except Exception as e:
            err = e

    assert outcome(err) == want["outcome"], err
    if want["outcome"] == "api_error":
        assert isinstance(err, ApiError) and err.code == want["code"]
    if "status" in want or "retryable" in want:
        assert isinstance(err, ApiError)
        assert err.status == want.get("status", err.status) and err.retryable == want.get("retryable", err.retryable)
    if "rpc" in want:
        assert isinstance(err, OrderStateUnknown) and err.rpc == want["rpc"]
    if "retries" in want:
        assert sum(e.kind is EventKind.RETRY for e in events) == want["retries"]
    if want["outcome"] == "transport_error" and "sent" in want:
        assert isinstance(err, TransportError) and err.sent == want["sent"]
    if want["outcome"] == "order_state_unknown" and "client_order_ids" in want:
        assert isinstance(err, OrderStateUnknown) and err.client_order_ids == want["client_order_ids"]
    bodies = sent() if ws else [b for r, b, _ in gateway.rest_calls if r == route]
    if "attempts" in want:
        assert (len(bodies) if ws else script.attempts) == want["attempts"]
    if want["outcome"] == "order_state_unknown":
        assert isinstance(err, OrderStateUnknown) and err.client_order_ids == [
            bodies[-1]["order"]["metadata"]["client_order_id"]
        ]
    if want.get("sent_client_order_id") == "generated":
        assert 2**63 <= int(bodies[0]["order"]["metadata"]["client_order_id"]) < 2**64
    elif "sent_client_order_id" in want:
        assert bodies[0]["order"]["metadata"]["client_order_id"] == want["sent_client_order_id"]


ORDER = {  # the smallest v1.order feed that decodes
    "sub_account_id": "42",
    "time_in_force": "GOOD_TILL_TIME",
    "legs": [],
    "signature": {"signer": "0x0", "r": "0x0", "s": "0x0", "v": 27, "expiration": "0", "nonce": 0, "chain_id": "326"},
    "metadata": {"client_order_id": "1"},
}


async def run_stream(gateway, sc: dict) -> None:
    call, want = sc["call"], sc["expect"]
    events: list = []
    options: dict[str, Any] = {**gateway.client_options, "api_key": "test-key", "on_event": events.append}
    if "resync_timeout_ms" in call:
        options["resync_timeout"] = call["resync_timeout_ms"] / 1000
    if "queue_size" in call:
        options["queue_size"] = call["queue_size"]
    if "cookie_max_age_s" in call:
        gateway.first_cookie_max_age = call["cookie_max_age_s"]

    def count(kind: EventKind) -> int:
        return sum(e.kind is kind for e in events)

    def calls(method: str) -> int:
        return sum(r["method"] == method for r in gateway.received)

    async with AsyncGrvt(gateway.env, **options) as client:
        if call["stream"] == "v1.order":
            gateway.snapshot_feeds["v1.order"] = ORDER
            subs, selectors = [client.order_stream(sub_account_id=42)], ["42"]
        else:
            open_stream = {"v1.ticker.d": client.ticker_delta_stream, "v1.ticker.s": client.ticker_snap_stream}
            instruments = call.get("instruments", ["BTC_USDT_Perp"])
            subs = [open_stream[call["stream"]](instrument=i, rate=500) for i in instruments]
            selectors = [f"{i}@500" for i in instruments]
        for s in subs:
            await s.start()
        sub = subs[0]
        for step in sc["steps"]:
            if "push" in step:
                feed = {"feed": step["feed"]["value"]} if "feed" in step else {}
                await gateway.push(call["stream"], selectors[0], *step["push"], **feed)
            elif step.get("drop"):
                await gateway.drop_all()
            elif "ignore_subscribes" in step:
                gateway.ignore_subscribes = step["ignore_subscribes"]
            elif "ack_latest" in step:
                gateway.ack_latest = step["ack_latest"]
            elif "wait_acks" in step:
                await wait_until(lambda n=step["wait_acks"]: count(EventKind.SUBSCRIBE) >= n, timeout=5)
            elif "wait_reconnects" in step:
                await wait_until(lambda n=step["wait_reconnects"]: count(EventKind.RECONNECT) >= n, timeout=5)
            elif "wait_dropped" in step:
                await wait_until(lambda n=step["wait_dropped"]: sub.dropped >= n, timeout=5)
        # Each subscription in order: the first's expectations, then other_subscriptions.
        wanted = [want, *want.get("other_subscriptions", [])]
        items: list[list] = []
        for s, w in zip(subs, wanted, strict=True):
            items.append([])
            while len(items[-1]) < len(w["items"]):
                try:
                    items[-1].append(int((await asyncio.wait_for(s.__anext__(), 5)).sequence_number))
                except DecodeError:
                    items[-1].append("decode_error")
                except StreamOverflow as e:
                    items[-1].append("stream_overflow")
                    assert e.capacity == want["overflow_capacity"]
        if "unsubscribes" in want:
            await sub.close()
            await wait_until(lambda: calls("unsubscribe") >= want["unsubscribes"])
            assert calls("unsubscribe") == want["unsubscribes"]
        for s in subs:
            await s.close()
    for selector, got, w in zip(selectors, items, wanted, strict=True):
        assert got == w["items"], selector
        gaps: list[dict] = []
        for e in events:
            if e.kind is EventKind.GAP and e.selector == selector:
                gaps.append({"reason": e.gap.reason, "after": e.gap.after_sequence, "through": e.gap.through_sequence})
                if len(gaps) <= len(w["gaps"]) and "frame" in w["gaps"][len(gaps) - 1]:
                    # the legacy seq and prev; the legacy expected prev is after
                    gaps[-1]["frame"] = [e.detail["seq"], e.detail["prev"]]
                    assert e.detail["expected_prev"] == e.gap.after_sequence
        assert gaps == w["gaps"], selector
    for selector in selectors:
        assert sum(selector in r["params"]["selectors"] for r in gateway.subscribes()) == want["subscribes"]
    if "subscribe_request" in want:
        assert gateway.subscribes()[0]["params"] == want["subscribe_request"]
    check_session(gateway, events, want)


def check_session(gateway, events: list, want: dict) -> None:
    logins = gateway.login_times
    if "logins" in want:
        assert len(logins) == want["logins"]
    if "relogin_after_ms" in want:
        low, high = want["relogin_after_ms"]
        assert low <= (logins[1] - logins[0]) * 1000 <= high
    if "session_rotated" in want:
        rotated = [e for e in events if e.kind is EventKind.SESSION_ROTATED]
        assert all(e.websocket_reconnect_required for e in rotated)
        assert [e.detail["generation"] for e in rotated] == want["session_rotated"]
    if "handshake_logins" in want:
        cookies = [h.get("Cookie", "") for h in gateway.handshakes]
        logins = [1 if c == "gravity=cookie-123" else int(c.rsplit("-", 1)[1]) for c in cookies]
        assert logins == want["handshake_logins"]


class FetchFailed(Exception):
    pass


async def run_pages(sc: dict, blocking: bool) -> None:
    """Runs pages/backfill_pages, or their blocking versions, over a fetch that answers with the script."""
    call, want = sc["call"], sc["expect"]
    script, cursors = Script(sc["replies"]), []

    def fetch(cursor: str | None) -> m.ApiFillHistoryResponse:
        cursors.append(cursor)
        if script.next().get("fail"):
            raise FetchFailed
        return m.ApiFillHistoryResponse(result=[], next=script.current["next"])

    async def fetch_async(cursor: str | None) -> m.ApiFillHistoryResponse:
        return fetch(cursor)

    gap = GapRange(call["gap"]["after"], call["gap"]["through"], call["gap"]["reason"]) if "gap" in call else None
    iterator: Any
    if blocking:
        iterator = pages_sync(fetch) if gap is None else backfill_pages_sync(fetch, gap=gap)
    else:
        iterator = pages(fetch_async) if gap is None else backfill_pages(fetch_async, gap=gap)
    assert cursors == [], "fetched before the first page was read"
    read: list = []
    err: Exception | None = None
    try:
        while len(read) != call.get("take"):
            read.append(next(iterator) if blocking else await anext(iterator))
    except (StopIteration, StopAsyncIteration):
        pass
    except (PaginationCycle, FetchFailed) as e:
        err = e
    got = {type(None): "ok", PaginationCycle: "pagination_cycle", FetchFailed: "fetch_error"}[type(err)]
    assert got == want["outcome"]
    if isinstance(err, PaginationCycle):
        assert err.cursor == want["cursor"]
    assert len(read) == want["pages"]
    assert cursors == want["cursors"]
    if gap is not None:
        assert [p.history_exhausted for p in read] == want["history_exhausted"]
        assert all(p.continuity == want["continuity"] and p.gap == gap for p in read)


def cases(calls_only: bool = False):
    """Each scenario once; pages and backfill also run their blocking versions. calls_only: calls and streams."""
    for sc in SCENARIOS:
        if calls_only and sc["call"]["kind"] in ("pages", "backfill"):
            continue
        reason = sc.get("skip", {}).get("python")
        marks = [pytest.mark.skip(reason=reason)] if reason else []
        yield pytest.param(sc, False, id=sc["name"], marks=marks)
        if sc["call"]["kind"] in ("pages", "backfill"):
            yield pytest.param(sc, True, id=sc["name"] + " (blocking)", marks=marks)


@pytest.mark.parametrize(("sc", "blocking"), list(cases()))
async def test_shared_runtime_scenario(gateway, sc, blocking):
    kind = sc["call"]["kind"]
    if kind in ("pages", "backfill"):
        await run_pages(sc, blocking)
    else:
        await (run_stream if kind == "stream" else run_call)(gateway, sc)


@pytest.mark.parametrize(("sc", "blocking"), list(cases(calls_only=True)))
async def test_shared_runtime_scenario_lite(lite_gateway, sc, blocking):
    """The same calls and streams with encoding="lite": the outcome must not depend on the encoding."""
    await (run_stream if sc["call"]["kind"] == "stream" else run_call)(lite_gateway, sc)
