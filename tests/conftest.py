"""In-process fake gateway (aiohttp) that speaks the Grvt WS + REST protocol closely enough for tests.

The `lite_gateway` fixture is the same fake speaking the lite encoding: it renames at its edge with its own
tables, built from contract.json (not from the SDK's generated ones), records every request with full names
so tests read them the same way, and fails the test if the client sent a key that is not a lite name.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import msgspec
import pytest
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestServer
from shared_data import load_shared

from grvt._runtime import Environment, RpcSpec, StreamSpec, SyncRunner

CONTRACT = load_shared("contract/contract.json")


def instrument_payload(name="BTC_USDT_Perp"):
    return {
        "instrument": name,
        "instrument_hash": "0x030501",
        "base": "BTC",
        "quote": "USDT",
        "kind": "PERPETUAL",
        "venues": [],
        "base_decimals": 9,
        "quote_decimals": 6,
        "tick_size": "0.1",
        "min_size": "0.001",
        "create_time": "0",
        "min_notional": "0",
    }


class TickerFeed(msgspec.Struct):
    stream: str
    selector: str
    sequence_number: str
    feed: dict
    prev_sequence_number: str = "0"


TICKER = StreamSpec("v1.ticker.d", "md", TickerFeed, conflate=True)
ORDERS = StreamSpec("v1.order", "td", TickerFeed, auth=True)  # private stream: never conflated
CREATE_ORDER = RpcSpec("v1/create_order", "td", "/full/v1/create_order", dict, auth=True, write=True, order_entry=True)
INSTRUMENT = RpcSpec("v1/instrument", "md", "/full/v1/instrument", dict)

# push's default feed, {"n": seq}. Any other value, None included, is sent as the feed.
FIXTURE = object()

# A rest_handler payload that closes the connection after reading the request, with no answer.
DROP = object()


class LiteNames:
    """Lite <-> full renaming for the fake, straight from contract.json."""

    def __init__(self) -> None:
        self.types = {t["name"]: t["fields"] or [] for t in CONTRACT["types"]}
        self.rpcs = {r["ws_method"]: r for r in CONTRACT["rpcs"]}
        self.feeds = {s["channel"]: s["feed_type"] for s in CONTRACT["streams"]}
        self.ws = CONTRACT["transport"]["ws"]
        methods = {m["method"]: m for m in self.ws["stream_methods"]}
        self.params = {k: methods[k]["params"] for k in ("subscribe", "unsubscribe")}
        self.results = {k: methods[k]["result"] for k in ("subscribe", "unsubscribe")}
        self.http_error = CONTRACT["transport"]["lite"]["http_error_body"]
        self.violations: list[str] = []  # keys the client sent that the type has no lite name for

    def to_full(self, type_name: str, value: Any) -> Any:
        """Renames what the client sent. A key that is not a lite name of the type is a violation."""
        if isinstance(value, list):
            return [self.to_full(type_name, v) for v in value]
        if not isinstance(value, dict):
            return value
        fields = {f["lite"]: f for f in self.types[type_name]}
        out = {}
        for k, v in value.items():
            f = fields.get(k)
            if f is None:
                self.violations.append(f"{type_name}.{k}")
                continue
            out[f["name"]] = self.to_full(f["type"], v) if f["kind"] == "struct" else v
        return out

    def to_lite(self, type_name: str, value: Any) -> Any:
        """Renames what the fake sends. Keys the contract does not list go out unchanged, as the server's do."""
        if isinstance(value, list):
            return [self.to_lite(type_name, v) for v in value]
        if not isinstance(value, dict):
            return value
        fields = {f["name"]: f for f in self.types[type_name]}
        out = {}
        for k, v in value.items():
            f = fields.get(k)
            if f is None:
                out[k] = v
            else:
                out[f["lite"]] = self.to_lite(f["type"], v) if f["kind"] == "struct" else v
        return out

    def request_to_full(self, data: dict) -> dict:
        req = self.to_full(self.ws["request_type"], data)
        method = req["method"]
        params = self.params.get(method) or self.rpcs[method]["request"]
        req["params"] = self.to_full(params, req.get("params"))
        return req

    def message_to_lite(self, obj: dict) -> dict:
        if "id" not in obj and "selector" in obj:  # a stream frame
            return self.to_lite(self.feeds[obj["stream"]], obj)
        obj = dict(obj)
        result = obj.pop("result", None)
        out = self.to_lite(self.ws["response_type"], obj)
        if result is not None:
            method = obj["method"]
            rtype = self.results.get(method) or self.rpcs[method]["response"]
            out[self._lite_key(self.ws["response_type"], "result")] = self.to_lite(rtype, result)
        return out

    def _lite_key(self, type_name: str, name: str) -> str:
        return next(f["lite"] for f in self.types[type_name] if f["name"] == name)


class LiteSocket:
    """A server-side socket that sends lite: what the fake and the tests send is renamed on the way out."""

    def __init__(self, ws: web.WebSocketResponse, names: LiteNames) -> None:
        self.ws, self.names = ws, names

    async def send_json(self, obj: dict) -> None:
        await self.ws.send_json(self.names.message_to_lite(obj))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.ws, name)


class FakeGateway:
    def __init__(self, lite: bool = False) -> None:
        self.lite = LiteNames() if lite else None
        # Options every test client of this gateway takes: each fake serves one encoding only.
        self.client_options: dict[str, Any] = {"encoding": "lite" if lite else "full"}
        self.conns: list[web.WebSocketResponse] = []
        self.received: list[dict] = []
        self.rest_calls: list[tuple[str, dict, dict]] = []
        self.handshakes: list[dict] = []
        self.connect_count = 0
        self.latest: dict[str, int] = {}  # selector -> last sequence number sent
        self.ignore_subscribes = 0  # swallow the next N subscribe requests
        self.ignore_rpcs: set[str] = set()
        self.ws_results: dict[str, Any] = {}  # method -> the result a WS RPC answers with (default: an echo)
        self.rest_handler: Callable[[str, dict], tuple[int, Any]] | None = None
        self.login_status = 200
        self.login_times: list[float] = []
        self.first_cookie_max_age = 3600  # later logins' cookies last an hour
        self.ack_latest: int | None = None  # the next subscribe reply's latest sequence number
        self.snapshot_feeds: dict[str, Any] = {}  # stream -> snapshot feed, for a typed stream that must decode
        self.new_conn = asyncio.Event()
        # Keeps a subscribe reply and its snapshot together: the client returns on the reply, so a test's push
        # could otherwise land between them and arrive before the snapshot.
        self.stream_lock = asyncio.Lock()

    # -- WS ------------------------------------------------------------------------------
    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        self.handshakes.append(dict(request.headers))
        raw = web.WebSocketResponse()
        await raw.prepare(request)
        ws: Any = raw if self.lite is None else LiteSocket(raw, self.lite)
        self.conns.append(ws)
        self.connect_count += 1
        self.new_conn.set()
        try:
            async for msg in raw:
                if msg.type is not WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if self.lite is not None:
                    data = self.lite.request_to_full(data)
                self.received.append(data)
                m = data["method"]
                if m == "subscribe":
                    if self.ignore_subscribes:
                        self.ignore_subscribes -= 1
                        continue
                    await self._subscribe(ws, data)
                elif m == "unsubscribe":
                    await ws.send_json(
                        {
                            "jsonrpc": "2.0",
                            "id": data["id"],
                            "method": m,
                            "result": {"stream": data["params"]["stream"], "unsubs": data["params"]["selectors"]},
                        }
                    )
                elif m in self.ignore_rpcs:
                    continue
                else:
                    await ws.send_json(
                        {
                            "jsonrpc": "2.0",
                            "id": data["id"],
                            "method": m,
                            "result": self.ws_results.get(m, {"result": {"echo": data["params"]}}),
                        }
                    )
        finally:
            if ws in self.conns:
                self.conns.remove(ws)
        return raw

    async def _subscribe(self, ws, data) -> None:
        p = data["params"]
        sel = p["selectors"][0]
        latest = self.latest.get(sel, 100)
        if self.ack_latest is not None:
            latest, self.ack_latest = self.ack_latest, None
        self.latest[sel] = latest
        async with self.stream_lock:
            await self._send_subscribed(ws, data, p, sel, latest)

    async def _send_subscribed(self, ws, data, p, sel: str, latest: int) -> None:
        await ws.send_json(
            {
                "jsonrpc": "2.0",
                "id": data["id"],
                "method": "subscribe",
                "result": {
                    "stream": p["stream"],
                    "subs": [sel],
                    "unsubs": [],
                    "num_snapshots": [1],
                    "first_sequence_number": [str(latest + 1)],
                    "latest_sequence_number": [str(latest)],
                },
            }
        )
        await ws.send_json(
            {
                "stream": p["stream"],
                "selector": sel,
                "sequence_number": "0",
                "prev_sequence_number": "0",
                "feed": self.snapshot_feeds.get(p["stream"], {"kind": "snapshot"}),
            }
        )

    async def push(
        self, stream: str, selector: str, seq: int | None = None, prev: int | None = None, feed: Any = FIXTURE
    ) -> None:
        last = self.latest.get(selector, 100)
        seq = last + 1 if seq is None else seq
        prev = last if prev is None else prev
        self.latest[selector] = seq
        async with self.stream_lock:
            await self._send_delta(stream, selector, seq, prev, feed)

    async def _send_delta(self, stream: str, selector: str, seq: int, prev: int, feed: Any) -> None:
        for ws in list(self.conns):
            await ws.send_json(
                {
                    "stream": stream,
                    "selector": selector,
                    "sequence_number": str(seq),
                    "prev_sequence_number": str(prev),
                    "feed": {"n": seq} if feed is FIXTURE else feed,
                }
            )

    async def drop_all(self, code: int = 1001) -> None:
        self.new_conn.clear()
        # Like push: let an in-flight subscribe reply be followed by its snapshot before closing.
        async with self.stream_lock:
            for ws in list(self.conns):
                await ws.close(code=code)

    def subscribes(self) -> list[dict]:
        return [r for r in self.received if r["method"] == "subscribe"]

    # -- REST ----------------------------------------------------------------------------
    async def rest(self, request: web.Request) -> web.Response:
        route = request.match_info["route"]
        body = await request.json() if request.can_read_body else {}
        if self.lite is not None:
            rpc = self.lite.rpcs[f"{request.path.split('/')[2]}/{route}"]
            body = self.lite.to_full(rpc["request"], body)
            response = await self._rest(route, body, request)
            try:
                payload = json.loads(response.body)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return response  # no body, or a malformed one: it goes out as is
            if response.status == 200:
                payload = self.lite.to_lite(rpc["response"], payload)
            elif isinstance(payload, dict):
                payload = {self.lite.http_error.get(k, k): v for k, v in payload.items()}
            return web.json_response(payload, status=response.status)
        return await self._rest(route, body, request)

    async def _rest(self, route: str, body: dict, request: web.Request) -> web.Response:
        self.rest_calls.append((route, body, dict(request.headers)))
        if self.rest_handler is not None:
            status, payload = self.rest_handler(route, body)
            if isinstance(payload, Exception):
                raise payload
            if payload is DROP:
                assert request.transport is not None
                request.transport.close()
                return web.Response()
            if isinstance(payload, bytes):  # sent as is, e.g. a malformed body
                return web.Response(body=payload, status=status, content_type="application/json")
            return web.json_response(payload, status=status)
        if route == "instrument":
            return web.json_response({"result": instrument_payload(body["instrument"])})
        return web.json_response({"result": {"route": route, "echo": body}})

    async def login(self, request: web.Request) -> web.Response:
        body = await request.json()
        if self.login_status != 200 or body.get("api_key") != "test-key":
            return web.json_response({"status": "failure", "error": "Invalid Credentials"}, status=401)
        self.login_times.append(asyncio.get_running_loop().time())
        n = len(self.login_times)
        cookie, max_age = ("cookie-123", self.first_cookie_max_age) if n == 1 else (f"cookie-123-{n}", 3600)
        r = web.json_response({"status": "success", "funding_account_address": "0xabc"})
        r.headers["Set-Cookie"] = f"gravity={cookie}; Path=/; Max-Age={max_age}; HttpOnly"
        r.headers["X-Grvt-Account-Id"] = "ACC:1"
        return r

    async def time(self, request: web.Request) -> web.Response:
        return web.json_response({"server_time": 1_700_000_000_000})

    def app(self) -> web.Application:
        app = web.Application()
        # The lite gateway serves no full routes: a lite client that reaches for one gets a 404.
        encoding = "full" if self.lite is None else "lite"
        app.router.add_get(f"/ws/{encoding}", self.ws)
        app.router.add_post(f"/{encoding}/v1/{{route}}", self.rest)
        app.router.add_post(f"/{encoding}/v2/{{route}}", self.rest)
        app.router.add_post("/auth/api_key/login", self.login)
        app.router.add_get("/time", self.time)
        return app


@pytest.fixture
async def gateway():
    gw = FakeGateway()
    server = TestServer(gw.app())
    await server.start_server()
    base = str(server.make_url("")).rstrip("/")
    ws = base.replace("http://", "ws://") + "/ws/full"
    gw.env = Environment("fake", base, base, base, ws, ws, chain_id=326)  # type: ignore[attr-defined]
    yield gw
    await server.close()


@pytest.fixture
async def lite_gateway():
    """The fake gateway in the lite encoding. Fails the test if the client sent any non-lite key."""
    gw = FakeGateway(lite=True)
    server = TestServer(gw.app())
    await server.start_server()
    base = str(server.make_url("")).rstrip("/")
    ws = base.replace("http://", "ws://")
    gw.env = Environment(  # type: ignore[attr-defined]
        "fake-lite",
        base,
        base,
        base,
        ws + "/ws/full",
        ws + "/ws/full",
        chain_id=326,
        md_ws_lite_url=ws + "/ws/lite",
        td_ws_lite_url=ws + "/ws/lite",
    )
    yield gw
    await server.close()
    assert gw.lite is not None
    assert gw.lite.violations == [], f"the lite client sent non-lite keys: {gw.lite.violations}"


async def wait_until(pred: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


@pytest.fixture
def gateway_sync():
    """The fake gateway on its own loop thread, for the blocking client test."""

    runner = SyncRunner()
    gw = FakeGateway.__new__(FakeGateway)

    async def start():
        FakeGateway.__init__(gw)
        server = TestServer(gw.app())
        await server.start_server()
        return server

    server = runner.run(start())
    base = str(server.make_url("")).rstrip("/")
    ws = base.replace("http://", "ws://") + "/ws/full"
    yield Environment("fake", base, base, base, ws, ws, chain_id=326)
    runner.run(server.close())
    runner.close()


@pytest.fixture(params=["full", "lite"])
def any_gateway(request):
    """The fake gateway in each encoding; pass gateway.client_options to the client."""
    return request.getfixturevalue("gateway" if request.param == "full" else "lite_gateway")
