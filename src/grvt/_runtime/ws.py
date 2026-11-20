"""One managed WebSocket per gateway ("md" market data, "td" trading).

What a socket does:
- connects lazily, with the session cookie + X-Grvt-Account-Id on the handshake when logged in
- keeps the desired subscriptions and re-sends them after every reconnect
- JSON-RPC requests: uint32 id -> future, per-request timeout
- reconnects with full-jitter backoff; stops for good on an auth failure
- detects sequence gaps and re-subscribes the same selector for a fresh snapshot; escalates to a
  reconnect if that snapshot does not come within `resync_timeout`
- the read loop only parses the envelope and enqueues; consumers decode. It never awaits a consumer.
- a connection with no frame of any kind (data, ping or pong) for `idle_timeout` is dropped and
  reconnected; the watchdog pings every third of it, so a quiet but healthy connection has pongs

Server facts: the server pings every 5s; a client whose outgoing queue fills is closed with 1008
"slow consumer"; frames above ReadLimitSize are rejected.
"""

# Subscription and WsSocket are one unit: the socket drives each subscription's queue and ack directly.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import collections
import time
from collections.abc import Callable
from math import isfinite
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import aiohttp
import msgspec

from .._generated import behavior
from . import codec
from .auth import Session
from .deadline import CallDeadline
from .errors import (
    ApiError,
    ConfigError,
    DecodeError,
    FrameTooLarge,
    GrvtError,
    OrderStateUnknown,
    StreamOverflow,
    TransportError,
    response_error,
)
from .hooks import Event, EventKind, GapRange, Hooks
from .log import debug_enabled, log, redact
from .protocol import Backoff, Pending, RequestTable, Sequencer, Verdict, jsonrpc_frame
from .spec import StreamSpec

if TYPE_CHECKING:
    from . import lite as _lite


T = TypeVar("T")


class Subscription(Generic[T]):
    """Async iterator over one stream selector. Iterating subscribes; `close()` unsubscribes.

    After a gap, re-subscription requests a fresh snapshot where supported.
    Older frames can remain queued. A snapshot does not recover missed private events.
    """

    def __init__(self, socket: WsSocket, spec: StreamSpec, selector: str, queue_size: int):
        self.spec = spec
        self.selector = selector
        self.key: tuple[str, str] | None = None  # (stream, selector as the server echoes it)
        self.seq = Sequencer()
        self._socket = socket
        self._decode = socket.lite_codec.decode if socket.lite else codec.decode
        self._queue: collections.deque[bytes] = collections.deque()
        self._max = queue_size
        self._wake: asyncio.Future[None] | None = None
        self._error: BaseException | None = None
        self._started = False
        self._closed = False
        self._ack: asyncio.Future[None] | None = None
        self._sent_on: Any = None  # the socket the last subscribe went out on
        self.dropped = 0

    # -- consumer side --------------------------------------------------------------
    def __aiter__(self) -> Subscription[T]:
        return self

    async def __anext__(self) -> T:
        if not self._started:
            self._started = True
            await self._socket.subscribe(self)
        while True:
            if self._error is not None:
                err, self._error = self._error, None
                if isinstance(err, StopAsyncIteration) or self._closed:
                    raise StopAsyncIteration
                raise err
            if self._queue:
                try:
                    return self._decode(self._queue.popleft(), self.spec.feed_type)
                except msgspec.MsgspecError as e:
                    raise DecodeError(self.spec.stream, str(e)) from e
            if self._closed:
                raise StopAsyncIteration
            self._wake = asyncio.get_running_loop().create_future()
            await self._wake

    async def start(self) -> Subscription[T]:
        """Subscribe now and wait for the server's ack (iteration does this implicitly)."""
        if not self._started:
            self._started = True
            await self._socket.subscribe(self)
        return self

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._socket.unsubscribe(self)
            self._notify()

    async def __aenter__(self) -> Subscription[T]:
        return await self.start()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- read-loop side (never blocks) ------------------------------------------------
    def _push(self, raw: bytes) -> bool:
        """Returns False when the consumer overflowed a stream that may not drop."""
        if len(self._queue) >= self._max:
            if self.spec.conflate:
                self._queue.popleft()
                self.dropped += 1
            else:
                self._queue.clear()
                self._error = StreamOverflow(self.spec.stream, self.selector, self._max)
                self._notify()
                return False
        self._queue.append(raw)
        self._notify()
        return True

    def _fail(self, err: BaseException) -> None:
        if self._ack is not None and not self._ack.done():
            self._ack.set_exception(err)  # the waiting subscribe() raises it
            self._ack.exception()
        else:
            self._error = err
        self._notify()

    def _notify(self) -> None:
        w = self._wake
        if w is not None and not w.done():
            w.set_result(None)


async def _reject_redirect(
    session: aiohttp.ClientSession, context: Any, params: aiohttp.TraceRequestRedirectParams
) -> None:
    # The trace fires before aiohttp can issue the next handshake with session headers.
    params.response.close()
    raise ApiError(0, "websocket redirects are disabled", params.response.status, retryable=False)


class WsSocket:
    def __init__(
        self,
        name: str,
        url: str,
        session: Session | None,
        hooks: Hooks,
        *,
        max_frame_bytes: int | None = None,
        request_timeout: float = behavior.WS_REQUEST_TIMEOUT,
        resync_timeout: float = behavior.RESYNC_TIMEOUT,
        idle_timeout: float = behavior.WS_IDLE_TIMEOUT,
        use_global_sequence_number: bool = True,
        on_session: Callable[[], None] | None = None,
        lite: bool = False,
    ):
        self.name = name
        # Lite: every frame in and out uses lite names (grvt._runtime.lite); the read loop decodes them in one pass.
        self.lite = lite
        self.lite_codec: Any = None  # grvt._runtime.lite, imported only by lite sockets
        self._decode_frame: Callable[[bytes], Any] = codec.decode_frame
        self._frame = jsonrpc_frame
        if lite:
            from . import lite as lite_codec

            self.lite_codec = lite_codec
            self._decode_frame, self._frame = lite_codec.decode_frame, lite_codec.jsonrpc_frame
        self.url = url
        self.session = session
        self.on_session = on_session  # called once the handshake's session is ready (starts its renewal)
        self.hooks = hooks
        self.max_frame_bytes = max_frame_bytes
        self.request_timeout = request_timeout
        self.resync_timeout = resync_timeout
        self.idle_timeout = idle_timeout
        self.use_global_sequence_number = use_global_sequence_number
        self.requests = RequestTable()
        self.backoff = Backoff()
        self._http: aiohttp.ClientSession | None = None
        self._subs: dict[int, Subscription[Any]] = {}  # id(sub) -> sub; the desired set
        self._by_key: dict[tuple[str, str], Subscription[Any]] = {}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._up = asyncio.Event()
        self._fatal: GrvtError | None = None
        self._task: asyncio.Task[None] | None = None
        self._watchdog: asyncio.Task[None] | None = None
        self._closing = False
        self._connected_once = False
        self._session_gen = 0
        self._reconnect_reason: str | None = None
        self._last_rx = 0.0  # monotonic time of the last frame of any kind on the current connection
        self._last_ping = 0.0

    # -- lifecycle --------------------------------------------------------------------
    def _start(self) -> None:
        if self._task is None and not self._closing:
            self._task = asyncio.create_task(self._supervise(), name=f"grvt-ws-{self.name}")
            self._watchdog = asyncio.create_task(self._watch(), name=f"grvt-ws-{self.name}-watchdog")

    async def connected(self, timeout: float = behavior.WS_CONNECT_TIMEOUT) -> aiohttp.ClientWebSocketResponse:
        self._start()
        if self._fatal is not None:
            raise self._fatal
        ws = self._ws
        if ws is not None and not ws.closed:
            return ws
        try:
            await asyncio.wait_for(self._up.wait(), timeout)
        except TimeoutError:
            raise TransportError(f"{self.name} socket not connected after {timeout}s", sent=False) from None
        if self._fatal is not None:
            raise self._fatal
        assert self._ws is not None
        return self._ws

    async def close(self) -> None:
        self._closing = True
        for t in (self._watchdog, self._task):
            if t is not None:
                t.cancel()
        if self._ws is not None:
            await self._ws.close()
        for t in (self._watchdog, self._task):
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._fail_pending("client closed")
        for sub in list(self._subs.values()):
            sub._closed = True
            sub._notify()
        if self._http is not None:
            await self._http.close()

    def force_reconnect(self, reason: str) -> None:
        self._reconnect_reason = reason
        ws = self._ws
        if ws is not None and not ws.closed:
            asyncio.ensure_future(ws.close())

    # -- supervisor -------------------------------------------------------------------
    async def _supervise(self) -> None:
        while not self._closing:
            try:
                ws = await self._connect()
            except GrvtError as e:
                if isinstance(e, ApiError) and not e.retryable:
                    self._go_fatal(e)
                    return
                await self._sleep_backoff(e)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - any connect failure is retried
                await self._sleep_backoff(e)
                continue

            up_since = self._last_rx = self._last_ping = time.monotonic()
            self._ws = ws
            kind = EventKind.RECONNECT if self._connected_once else EventKind.CONNECT
            self._connected_once = True
            self._up.set()
            if self.hooks.on_event is not None:
                self.hooks.emit(Event(kind, socket=self.name))
            for sub in list(self._subs.values()):
                await self._send_subscribe(sub)

            await self._read_loop(ws)
            if time.monotonic() - up_since > behavior.RECONNECT_BACKOFF_RESET_AFTER:
                self.backoff.reset()  # only a connection that stayed up resets the backoff

            self._up.clear()
            self._ws = None
            code = ws.close_code
            reason = self._reconnect_reason or ("slow consumer (server 1008)" if code == 1008 else "closed")
            self._reconnect_reason = None
            self._fail_pending(f"socket closed ({reason}, code {code})")
            for sub in self._subs.values():
                if not self._closing and self.hooks.on_event is not None:
                    self.hooks.emit(
                        Event(
                            EventKind.GAP,
                            socket=self.name,
                            stream=sub.spec.stream,
                            selector=sub.selector,
                            gap=GapRange(sub.seq.last or None, None, "disconnect"),
                        )
                    )
                sub.seq.reset()
            if self.hooks.on_event is not None:
                self.hooks.emit(Event(EventKind.DISCONNECT, socket=self.name, code=code, reason=reason))
            if not self._closing:
                await asyncio.sleep(self.backoff.next_delay())

    async def _sleep_backoff(self, err: BaseException) -> None:
        delay = self.backoff.next_delay()
        log.debug("grvt: %s connect failed (%s); retry in %.2fs", self.name, err, delay)
        if self.hooks.on_event is not None:
            self.hooks.emit(
                Event(EventKind.RETRY, socket=self.name, attempt=self.backoff.attempt, delay=delay, error=str(err))
            )
        await asyncio.sleep(delay)

    def _go_fatal(self, err: GrvtError) -> None:
        self._fatal = err
        self._up.set()  # wake waiters so they see the error
        if self.hooks.on_event is not None:
            self.hooks.emit(Event(EventKind.ERROR, socket=self.name, fatal=True, error=str(err)))
        for sub in self._subs.values():
            sub._fail(err)

    async def _connect(self) -> aiohttp.ClientWebSocketResponse:
        headers: dict[str, str] = {}
        if self.session is not None and self.session.enabled:
            await self.session.ensure()
            if self.on_session is not None:
                self.on_session()
            headers = self.session.headers()
            self._session_gen = self.session.generation
        if self._http is None:
            trace = aiohttp.TraceConfig()
            trace.on_request_redirect.append(_reject_redirect)
            self._http = aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(), trace_configs=[trace])
        try:
            # aiohttp declares ClientWSTimeout's fields with attr.ib, which pyright cannot see.
            timeout = aiohttp.ClientWSTimeout(ws_close=5.0)  # pyright: ignore[reportCallIssue]
            # No aiohttp heartbeat, and pings come to the read loop: the idle check in _watch must see
            # every frame, control frames included.
            return await self._http.ws_connect(
                self.url,
                headers=headers,
                autoping=False,
                max_msg_size=behavior.WS_MAX_INCOMING_FRAME,
                timeout=timeout,
            )
        except aiohttp.WSServerHandshakeError as e:
            if e.status in (401, 403):
                # Rejected handshake (e.g. sticky-session check): retrying will not help.
                raise ApiError(
                    1000, f"websocket handshake rejected: {e.message}", e.status, None, retryable=False
                ) from e
            raise TransportError(f"handshake failed: {e.status} {e.message}") from e

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                self._last_rx = time.monotonic()
                if msg.type is aiohttp.WSMsgType.TEXT:
                    self._on_frame(msg.data.encode())
                elif msg.type is aiohttp.WSMsgType.BINARY:
                    self._on_frame(msg.data)
                elif msg.type is aiohttp.WSMsgType.PING:
                    await ws.pong(msg.data)
                elif msg.type is aiohttp.WSMsgType.ERROR:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - a broken frame handler must not kill the supervisor
            log.exception("grvt: %s read loop error", self.name)
            if self.hooks.on_event is not None:
                self.hooks.emit(Event(EventKind.ERROR, socket=self.name, fatal=False, error=repr(e)))
            await ws.close()

    # -- incoming ---------------------------------------------------------------------
    def _on_frame(self, data: bytes) -> None:
        if self.hooks.on_event is not None:
            self.hooks.emit(Event(EventKind.FRAME, socket=self.name, raw=data, direction="in"))
        if debug_enabled():
            log.debug("grvt: %s <- %s", self.name, redact(data[:2000]))
        try:
            f = self._decode_frame(data)
        except Exception:
            log.warning("grvt: %s undecodable frame dropped", self.name)
            return
        if f.stream is not None and f.selector is not None and f.id is None:
            self._on_feed(f, data)
        elif f.id is not None:
            self._on_reply(f)

    def _on_reply(self, f: codec.Frame | _lite.Frame) -> None:
        p = self.requests.pop(f.id)  # type: ignore[arg-type]
        if p is None:
            log.debug("grvt: %s reply for unknown id %s (late or not ours)", self.name, f.id)
            return
        # Frame.result is None when the frame has none, although msgspec makes us type it as plain Raw.
        if f.error is not None or (f.code and f.result is None):  # pyright: ignore[reportUnnecessaryComparison]
            value = f.error if f.error is not None else {"code": f.code, "message": f.message}
            if self.lite and f.error is not None:
                value = self.lite_codec.error(value)
            exc = response_error(value, None, p.method)
            if p.on_reply is not None:
                p.on_reply(exc, p.context)
            if not p.waiter.done():
                p.waiter.set_exception(exc)
            return
        if p.on_reply is not None:
            p.on_reply(f.result, p.context)
        if not p.waiter.done():
            p.waiter.set_result(f.result)

    def _on_feed(self, f: codec.Frame | _lite.Frame, data: bytes) -> None:
        sub = self._by_key.get((f.stream, f.selector))  # type: ignore[arg-type]
        if sub is None:
            return
        verdict = sub.seq.on_frame(f.sequence_number, f.prev_sequence_number)
        if verdict is Verdict.DELIVER:
            if not sub._push(data):
                self._resync(sub, "overflow")
        elif verdict is Verdict.GAP:
            if self.hooks.on_event is not None:
                self.hooks.emit(
                    Event(
                        EventKind.GAP,
                        gap=GapRange(
                            sub.seq.last,
                            f.prev_sequence_number
                            if sub.spec.snapshot_feed
                            and f.prev_sequence_number is not None
                            and f.prev_sequence_number >= sub.seq.last
                            else None,
                            "sequence_mismatch",
                        ),
                        socket=self.name,
                        stream=sub.spec.stream,
                        selector=f.selector,
                        expected_prev=sub.seq.last,
                        prev=f.prev_sequence_number,
                        seq=f.sequence_number,
                    )
                )
            if sub.spec.snapshot_feed:  # each frame is complete; just move on
                sub.seq.last = f.sequence_number or sub.seq.last
                sub._push(data)
            else:
                self._resync(sub, "gap")

    # -- subscriptions ----------------------------------------------------------------
    async def subscribe(self, sub: Subscription[Any]) -> None:
        self._subs[id(sub)] = sub
        sub._ack = asyncio.get_running_loop().create_future()
        ack = sub._ack
        await self.connected()
        # A fresh connect re-sends every desired sub; only send if that did not already happen.
        if self._ws is not None and not ack.done() and sub._sent_on is not self._ws:
            await self._send_subscribe(sub)
        try:
            await asyncio.wait_for(asyncio.shield(ack), self.request_timeout)
        except TimeoutError:
            raise TransportError(
                f"no subscribe reply within {self.request_timeout}s", rpc=f"subscribe {sub.spec.stream}", sent=True
            ) from None

    async def _send_subscribe(self, sub: Subscription[Any]) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        sub.seq.reset()
        sub._sent_on = ws  # type: ignore[attr-defined]
        if sub._ack is None or sub._ack.done():
            sub._ack = asyncio.get_running_loop().create_future()
        params = self._stream_params(sub, True)
        fut = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_consume_exception)
        rid = self.requests.add(Pending("subscribe", False, fut, self._on_subscribe_reply, sub))
        try:
            await self._send(ws, self._frame("subscribe", params, rid))
        except TransportError as e:
            # The socket is going down; the reconnect re-sends every desired subscription.
            self.requests.pop(rid)
            log.debug("grvt: %s subscribe send failed: %s", self.name, e)

    def _stream_params(self, sub: Subscription[Any], subscribe: bool) -> bytes:
        if self.lite:
            return self.lite_codec.stream_params(
                subscribe, sub.spec.stream, [sub.selector], self.use_global_sequence_number
            )
        params = {
            "stream": sub.spec.stream,
            "selectors": [sub.selector],
            "use_global_sequence_number": self.use_global_sequence_number,
        }
        return codec.encode(params)

    def _on_subscribe_reply(self, result: Any, sub: Subscription[Any]) -> None:
        # Runs inside the read loop, before the snapshot frames that follow the ack.
        if isinstance(result, BaseException):
            sub._fail(result)
            return
        if self.lite:
            r = self.lite_codec.subscribe_result(bytes(result))
        else:
            r = codec.decode(bytes(result), codec.SubscribeResult)
        canonical = r.subs[0] if r.subs else sub.selector
        if sub.key is not None and sub.key != (sub.spec.stream, canonical):
            self._by_key.pop(sub.key, None)
        sub.key = (sub.spec.stream, canonical)
        self._by_key[sub.key] = sub
        latest = r.latest_sequence_number[0] if r.latest_sequence_number else 0
        snaps = r.num_snapshots[0] if r.num_snapshots else 0
        sub.seq.on_ack(latest, snaps)
        if sub._ack is not None and not sub._ack.done():
            sub._ack.set_result(None)
        if self.hooks.on_event is not None:
            self.hooks.emit(
                Event(
                    EventKind.SUBSCRIBE,
                    socket=self.name,
                    stream=sub.spec.stream,
                    selector=canonical,
                    latest_sequence_number=latest,
                    num_snapshots=snaps,
                )
            )

    def _resync(self, sub: Subscription[Any], why: str) -> None:
        if why == "overflow" and self.hooks.on_event is not None:
            self.hooks.emit(
                Event(
                    EventKind.GAP,
                    socket=self.name,
                    stream=sub.spec.stream,
                    selector=sub.selector,
                    gap=GapRange(None, None, "overflow"),
                )
            )
        sub.seq.reset()  # drop every frame until the new subscribe reply arrives
        sub.seq.resync_started = time.monotonic()
        log.debug("grvt: %s re-subscribing %s %s after %s", self.name, sub.spec.stream, sub.selector, why)
        asyncio.ensure_future(self._send_subscribe(sub))

    async def unsubscribe(self, sub: Subscription[Any]) -> None:
        self._subs.pop(id(sub), None)
        if sub.key is not None:
            self._by_key.pop(sub.key, None)
        ws = self._ws
        if ws is None or ws.closed:
            return
        params = self._stream_params(sub, False)
        fut = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_consume_exception)
        rid = self.requests.add(Pending("unsubscribe", False, fut))
        try:
            await self._send(ws, self._frame("unsubscribe", params, rid))
        except TransportError:
            self.requests.pop(rid)

    # -- JSON-RPC requests ----------------------------------------------------------------
    async def request(
        self,
        method: str,
        params_json: bytes,
        *,
        write: bool,
        timeout: float | None = None,
        client_order_id: Any = None,
        deadline: CallDeadline | None = None,
    ) -> Any:
        """Send one JSON-RPC request; returns the raw `result` (msgspec.Raw)."""
        wait = self.request_timeout if timeout is None else timeout
        if not isfinite(wait) or wait <= 0:
            raise ConfigError("timeout must be finite and positive")
        ws = await self.connected()  # failure here: nothing was sent
        loop = asyncio.get_running_loop()
        if deadline is not None:
            deadline.check()
        fut = loop.create_future()
        rid = self.requests.add(Pending(method, write, fut, None, client_order_id))
        frame = self._frame(method, params_json, rid)
        dispatched = False
        try:
            if deadline is not None:
                deadline.check()

            def dispatch() -> None:
                nonlocal dispatched
                if deadline is not None:
                    deadline.check()
                dispatched = True

            await self._send(ws, frame, dispatch)
            return await asyncio.wait_for(fut, wait)
        except asyncio.CancelledError as e:
            if write and dispatched:
                raise e from OrderStateUnknown(method, "write cancelled while awaiting its response", client_order_id)
            raise
        except TransportError as e:
            if write and dispatched and e.sent:
                raise OrderStateUnknown(method, str(e), client_order_id) from e
            raise
        except TimeoutError as e:
            if write and dispatched:
                raise OrderStateUnknown(method, "write response deadline exceeded", client_order_id) from e
            if dispatched:
                raise TransportError("WebSocket reply timeout", rpc=method, sent=True) from e
            raise
        finally:
            self.requests.pop(rid)
            if not fut.done():
                fut.cancel()

    async def _send(
        self, ws: aiohttp.ClientWebSocketResponse, frame: bytes, on_dispatch: Callable[[], None] | None = None
    ) -> None:
        if self.max_frame_bytes is not None and len(frame) > self.max_frame_bytes:
            # TODO: fall back to REST for this call once the runtime can tell the RPC is safe to reroute.
            raise FrameTooLarge(len(frame), self.max_frame_bytes)
        if ws.closed:
            raise TransportError("socket closed before send", sent=False)
        if self.hooks.on_event is not None:
            self.hooks.emit(Event(EventKind.FRAME, socket=self.name, raw=frame, direction="out"))
        if debug_enabled():
            log.debug("grvt: %s -> %s", self.name, redact(frame))
        if on_dispatch is not None:
            on_dispatch()
        try:
            await ws.send_str(frame.decode())
        except (ConnectionError, RuntimeError, aiohttp.ClientError) as e:
            # aiohttp refuses a closing transport before writing any byte. Any other failure (a drain
            # error, an unknown message) may come after bytes left, so a write's outcome is unknown.
            refused = isinstance(e, ConnectionResetError) and "closing transport" in str(e)
            raise TransportError(f"send failed: {e}", sent=not refused) from e

    def _fail_pending(self, reason: str) -> None:
        for p in self.requests.drain():
            if p.waiter.done():
                continue
            if p.write:
                error = OrderStateUnknown(p.method, reason, p.context)
                error.__cause__ = TransportError(reason, rpc=p.method, sent=True)
                p.waiter.set_exception(error)
            else:
                p.waiter.set_exception(TransportError(reason, rpc=p.method, sent=True))

    # -- watchdog: overdue resyncs, session rotation, idle connections and pings ------------
    async def _watch(self) -> None:
        while not self._closing:
            await asyncio.sleep(behavior.WATCHDOG_INTERVAL)
            ws = self._ws
            if ws is None or ws.closed:
                continue
            now = time.monotonic()
            if now - self._last_rx > self.idle_timeout:
                self.force_reconnect(f"no frames for {self.idle_timeout}s")
                continue
            if now - self._last_ping >= self.idle_timeout / 3:
                self._last_ping = now
                asyncio.ensure_future(ws.ping()).add_done_callback(_consume_exception)
            for sub in self._subs.values():
                if sub.seq.resync_overdue(now, self.resync_timeout):
                    sub.seq.resync_started = None
                    self.force_reconnect(f"no snapshot within {self.resync_timeout}s of re-subscribe")
                    break
            s = self.session
            if (
                s is not None
                and s.enabled
                and s.generation != self._session_gen
                and self.requests.writes_in_flight() == 0
            ):
                # Break-before-make: wait for a quiet moment (no writes in flight), then
                # reconnect with the new cookie; subscriptions re-sync from snapshots. A full version
                # opens the new socket first, subscribes, moves order entry, then drains the old one.
                self._session_gen = s.generation
                self.force_reconnect("session rotated")


def _consume_exception(fut: asyncio.Future[Any]) -> None:
    if not fut.cancelled():
        fut.exception()
