"""The one object generated clients hold. Same size no matter how many RPCs or streams exist.

Generated code calls only:
    rt.call(RPC_SPEC, params, sign=sign_fn, client_order_id=...)   one RPC, REST or WS
    rt.stream(STREAM_SPEC, selector)                              a Subscription (async iterator)
    rt.sign(STRUCT_TYPE, message)                                 EIP-712 signature for this env
    rt.clock / rt.nonces / rt.client_order_ids / rt.metadata      inputs for sign functions
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from math import isfinite
from typing import Any, Literal, cast, overload

from .._generated import behavior
from . import codec
from .auth import LOGIN_PATH, Session
from .clock import ClientOrderIdProvider, Clock, NonceProvider, RandomNonce, SystemClock, TimeCounterClientOrderId
from .deadline import CallDeadline
from .env import Environment, resolve_env
from .errors import ApiError, DecodeError, GrvtError, OrderStateUnknown, uncertain_api_error
from .hooks import BeforeSend, Call, Event, EventKind, Hooks, OnEvent
from .log import configure_from_env, log
from .rest import rest_call
from .signing import Domain, PrivateKeySigner, Signature, Signer, StructType, grvt_domain
from .signing.checks import leg_instruments
from .signing.signer import check_signature
from .spec import RpcSpec, StreamSpec
from .transport import AiohttpTransport, HttpTransport
from .ws import Subscription, WsSocket

SignFn = Callable[["Runtime", Any], Awaitable[Any]]
_EMPTY = b"{}"


def _observe_task(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


class MetadataCache:
    """Instrument/currency metadata needed to sign (instrument hash, decimals). Single-flight per key."""

    def __init__(self) -> None:
        self._values: dict[tuple[str, str], Any] = {}
        self._inflight: dict[tuple[str, str], asyncio.Future[Any]] = {}

    async def get(self, kind: str, key: str, loader: Callable[[], Awaitable[Any]]) -> Any:
        k = (kind, key)
        if k in self._values:
            return self._values[k]
        fut = self._inflight.get(k)
        if fut is None:

            async def load() -> Any:
                try:
                    value = await loader()
                    self._values[k] = value
                    return value
                finally:
                    self._inflight.pop(k, None)

            fut = asyncio.create_task(load())
            self._inflight[k] = fut
            fut.add_done_callback(_observe_task)
        return await asyncio.shield(fut)

    async def close(self) -> None:
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def peek(self, kind: str, key: str) -> Any:
        return self._values.get((kind, key))

    def put(self, kind: str, key: str, value: Any) -> None:
        self._values[(kind, key)] = value

    def drop(self, kind: str, key: str) -> None:
        """Forget one entry; the next call that needs it loads it again."""
        self._values.pop((kind, key), None)


class Runtime:
    def __init__(
        self,
        env: str | Environment = "testnet",
        *,
        api_key: str | None = None,
        sub_account_id: int | str | None = None,
        signer: Signer | None = None,
        order_transport: str = "rest",
        encoding: Literal["full", "lite"] = "lite",
        check_orders: bool = True,
        clock: Clock | None = None,
        nonces: NonceProvider | None = None,
        client_order_ids: ClientOrderIdProvider | None = None,
        transport: HttpTransport | None = None,
        before_send: BeforeSend | None = None,
        on_event: OnEvent | None = None,
        http_attempt_timeout: float = behavior.HTTP_ATTEMPT_TIMEOUT,
        call_timeout: float = behavior.CALL_TIMEOUT,
        ws_request_timeout: float = behavior.WS_REQUEST_TIMEOUT,
        resync_timeout: float = behavior.RESYNC_TIMEOUT,
        queue_size: int = behavior.QUEUE_SIZE,
    ) -> None:
        if order_transport not in ("rest", "ws"):
            raise ValueError("order_transport must be 'rest' or 'ws'")
        if encoding not in ("full", "lite"):
            raise ValueError("encoding must be 'full' or 'lite'")
        self.sub_account_id = self.account(sub_account_id, required=False)
        configure_from_env()
        self.env = resolve_env(env, encoding)
        self.encoding = encoding
        self._lite = encoding == "lite"  # gateway calls and streams use lite names on the wire; edge stays full
        self._lite_codec: Any = None  # grvt._runtime.lite, imported only by lite clients
        if self._lite:
            from . import lite

            self._lite_codec = lite
        self.signer = signer
        self.order_transport = order_transport
        self.check_orders = check_orders  # False skips the instrument-limit check before signing
        self.clock = clock or SystemClock()
        self.nonces = nonces or RandomNonce()
        self.client_order_ids = client_order_ids or TimeCounterClientOrderId(self.clock)
        self.http = transport or AiohttpTransport()
        self.hooks = Hooks(before_send, on_event)
        for name, value in (
            ("http_attempt_timeout", http_attempt_timeout),
            ("call_timeout", call_timeout),
            ("ws_request_timeout", ws_request_timeout),
            ("resync_timeout", resync_timeout),
        ):
            if not isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        self.http_attempt_timeout = http_attempt_timeout
        self.call_timeout = call_timeout
        self._closed = False
        self.queue_size = queue_size
        self.metadata = MetadataCache()
        self.session = Session(api_key, self.env.edge_url + LOGIN_PATH, self.http.post, timeout=http_attempt_timeout)
        self._domain = grvt_domain(self.env.chain_id)
        self._sockets = {
            g: WsSocket(
                g,
                self.env.ws_url(g, lite=self._lite),
                self.session,
                self.hooks,
                max_frame_bytes=self.env.ws_max_frame_bytes,
                request_timeout=ws_request_timeout,
                resync_timeout=resync_timeout,
                on_session=self._start_rotation,
                lite=self._lite,
            )
            for g in ("md", "td")
        }
        self._rotation: asyncio.Task[None] | None = None
        self._session_retry_delay = behavior.SESSION_RETRY_DELAY  # wait after a failed session renewal

    # -- lifecycle ------------------------------------------------------------------------
    async def __aenter__(self) -> Runtime:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        self._closed = True
        if self._rotation is not None:
            self._rotation.cancel()
            await asyncio.gather(self._rotation, return_exceptions=True)
        for s in self._sockets.values():
            await s.close()
        await self.metadata.close()
        await self.session.close()
        await self.http.close()

    def socket(self, gateway: str) -> WsSocket:
        return self._sockets[gateway]

    # -- RPC --------------------------------------------------------------------------------
    @overload
    def account(self, value: int | str | None, *, required: Literal[True] = True) -> str: ...

    @overload
    def account(self, value: int | str | None, *, required: Literal[False]) -> str | None: ...

    def account(self, value: int | str | None, *, required: bool = True) -> str | None:
        if value is None:
            value = getattr(self, "sub_account_id", None)
        if value is None:
            if required:
                raise ValueError("sub_account_id is required when no client default is configured")
            return None
        text = str(value)
        if not text.isascii() or not text.isdigit() or not 0 < int(text) <= 2**64 - 1:
            raise ValueError("sub_account_id must be a positive uint64")
        return str(int(text))

    async def call(
        self,
        rpc: RpcSpec,
        params: Any = None,
        *,
        sign: SignFn | None = None,
        client_order_id: Any = None,
        timeout: float | None = None,
    ) -> Any:
        if self._closed:
            raise GrvtError("client closed")
        total = self.call_timeout if timeout is None else timeout
        if not isfinite(total) or total <= 0:
            raise ValueError("timeout must be finite and positive")
        deadline = CallDeadline(total)
        budget = asyncio.timeout_at(deadline.expires)
        try:
            async with budget:
                return await self._call(rpc, params, sign, client_order_id, deadline)
        except TimeoutError as e:
            cancellation = e.__cause__ or e.__context__
            unknown = cancellation.__cause__ if cancellation is not None else None
            if budget.expired() and isinstance(unknown, OrderStateUnknown):
                # Use a fresh error: cancellation already chains the original uncertainty.
                raise OrderStateUnknown(
                    unknown.rpc, "write response deadline exceeded", unknown.client_order_ids
                ) from e
            raise

    async def _call(
        self, rpc: RpcSpec, params: Any, sign: SignFn | None, client_order_id: Any, deadline: CallDeadline
    ) -> Any:
        deadline.check()
        via = "ws" if rpc.order_entry and self.order_transport == "ws" else "rest"
        if self.hooks.before_send is not None:
            params = (await self.hooks.run_before_send(Call(rpc.method, params, via))).params
        deadline.check()
        if sign is not None:
            params = await sign(self, params)  # nothing may change the request after this line
        deadline.check()
        body = _EMPTY if params is None else codec.encode(params)
        final = codec.decode(body)
        if isinstance(final, dict) and ("order" in final or "orders" in final):
            final = cast(dict[str, Any], final)
            orders: list[dict[str, Any]] = final.get("orders", [final.get("order")])
            client_order_id = [
                o["metadata"]["client_order_id"]
                for o in orders or []
                if o.get("metadata", {}).get("client_order_id") is not None
            ]
        wire = body
        if self._lite:
            if rpc.lite_path is None or rpc.request_type is None:
                if via == "ws":
                    raise GrvtError(f"{rpc.method} has no lite form to send over a lite WebSocket")
            elif params is not None:
                wire = self._lite_codec.encode(rpc.request_type, params)
        try:
            return await self._send(rpc, via, wire, deadline, client_order_id)
        except ApiError as e:
            if e.code in rpc.refresh_metadata_on:
                for name in set(leg_instruments(codec.decode(body))):
                    self.metadata.drop("instrument", name)
            raise

    async def _send(self, rpc: RpcSpec, via: str, body: bytes, deadline: CallDeadline, client_order_id: Any) -> Any:
        deadline.check()
        if rpc.auth:
            await self._ensure_session()
        deadline.check()
        if via == "ws":
            try:
                raw = await self._sockets["td"].request(
                    rpc.method, body, write=rpc.write, client_order_id=client_order_id, deadline=deadline
                )
            except ApiError as e:
                if rpc.write and uncertain_api_error(e):
                    raise OrderStateUnknown(rpc.method, str(e), client_order_id) from e
                raise
            try:
                if self._lite:
                    return self._lite_codec.decode(bytes(raw), rpc.response_type)
                return codec.decode(bytes(raw), rpc.response_type)
            except Exception as e:
                if rpc.write:
                    raise OrderStateUnknown(rpc.method, str(e), client_order_id) from e
                raise DecodeError(rpc.method, str(e)) from e
        headers = {"Content-Type": "application/json"}
        if rpc.auth:
            headers.update(self.session.headers())
        path, use_lite = rpc.path, False
        if self._lite and rpc.lite_path is not None:
            path, use_lite = rpc.lite_path, True
        return await rest_call(
            self.http,
            self.env.rest_url(rpc.gateway, path),
            rpc,
            body,
            headers,
            self.http_attempt_timeout,
            self.hooks,
            client_order_id,
            deadline=deadline,
            lite=self._lite_codec if use_lite else None,
        )

    # -- streams ----------------------------------------------------------------------------
    def stream(self, spec: StreamSpec, selector: str, *, queue_size: int | None = None) -> Subscription[Any]:
        return Subscription(self._sockets[spec.gateway], spec, selector, queue_size or self.queue_size)

    # -- signing ----------------------------------------------------------------------------
    @property
    def domain(self) -> Domain:
        return self._domain

    async def sign(self, struct: StructType, message: dict[str, Any], *, chain_id: int | None = None) -> Signature:
        """Sign with the Grvt domain for `chain_id` (default: this environment's chain id)."""
        if self.signer is None:
            raise GrvtError("this call needs a signer: pass signer=PrivateKeySigner(...)")
        domain = self._domain if chain_id in (None, self.env.chain_id) else grvt_domain(chain_id)
        if type(self.signer) is PrivateKeySigner:  # in-process key: sign our digest directly
            return self.signer.sign_digest(struct.digest(domain, message))
        # External (wallet/KMS) signer: it signs the typed data itself, so check the result against our digest.
        sig = await self.signer.sign_typed(domain, struct, message)
        why = check_signature(struct.digest(domain, message), sig, self.signer.address)
        if why is not None:
            raise GrvtError(f"signer {self.signer.address} returned an invalid signature: {why}")
        return sig

    # -- session ----------------------------------------------------------------------------
    async def _ensure_session(self) -> None:
        if not self.session.enabled:
            raise GrvtError("this call needs a session: pass api_key=...")
        await self.session.ensure()
        self._start_rotation()

    def _start_rotation(self) -> None:
        """Renews the session from its first login on, whether a call or a socket handshake needed it."""
        if self._rotation is None and not self._closed:
            self._rotation = asyncio.create_task(self._rotate_forever(), name="grvt-session-rotation")

    async def _rotate_forever(self) -> None:
        while True:
            await asyncio.sleep(max(1.0, self.session.refresh_due() - time.time()))
            try:
                await self.session.login()
            except GrvtError as e:
                log.warning("grvt: session re-login failed: %s", e)
                if self.hooks.on_event is not None:
                    self.hooks.emit(Event(EventKind.ERROR, detail={"error": str(e), "where": "session"}))
                await asyncio.sleep(self._session_retry_delay)
                continue
            if self.hooks.on_event is not None:
                self.hooks.emit(
                    Event(
                        EventKind.SESSION_ROTATED,
                        websocket_reconnect_required=True,
                        detail={"generation": self.session.generation, "expires_at": self.session.expires_at},
                    )
                )

    # -- clock ------------------------------------------------------------------------------
    async def sync_clock(self) -> None:
        """GET {md}/time ({"server_time": ms}) and feed it to the clock, if it can use it."""
        t0 = time.monotonic()
        status, _, body = await self.http.get(self.env.md_url + "/time", self.http_attempt_timeout)
        rtt_ms = (time.monotonic() - t0) * 1000
        observe = getattr(self.clock, "observe", None)
        if status == 200 and callable(observe):
            observe(int(codec.decode(body)["server_time"]), rtt_ms)
