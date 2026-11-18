"""REST calls: POST {gateway}{path} with a JSON body.

Success: HTTP 200, body is the RPC's response struct.
Failure: body {"code": int, "message": str, "status": int}; lite paths use the lite keys ({"c", "m", "s"}).
Reads retry with full jitter on retryable failures; writes are sent exactly once.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .._generated import behavior
from . import codec
from .deadline import CallDeadline
from .errors import ApiError, DecodeError, OrderStateUnknown, TransportError, response_error, uncertain_api_error
from .hooks import Event, EventKind, Hooks
from .log import debug_enabled, log, redact, redact_headers
from .protocol import Backoff
from .spec import RpcSpec
from .transport import HttpTransport


def read_backoff() -> Backoff:
    """The backoff between read retries."""
    return Backoff(base=behavior.READ_BACKOFF_BASE, cap=behavior.READ_BACKOFF_CAP)


async def rest_call(
    transport: HttpTransport,
    url: str,
    spec: RpcSpec,
    body: bytes,
    headers: dict[str, str],
    timeout: float,
    hooks: Hooks,
    client_order_id: Any = None,
    deadline: CallDeadline | None = None,
    lite: Any = None,
) -> Any:
    """`lite`: the grvt._runtime.lite module when the call goes to a lite path, else None."""
    backoff = read_backoff()
    attempt = 0
    while True:
        if deadline is not None:
            deadline.check()
        attempt += 1
        try:
            return await _once(transport, url, spec, body, headers, timeout, hooks, client_order_id, deadline, lite)
        except TransportError as e:
            if spec.write:
                if e.sent:
                    raise OrderStateUnknown(spec.method, str(e), client_order_id) from e
                raise
            err: Exception = e
            retryable = True
        except DecodeError as e:
            if spec.write:  # the server answered, but we can't tell what it said
                raise OrderStateUnknown(spec.method, str(e), client_order_id) from e
            raise
        except ApiError as e:
            if spec.write:
                if uncertain_api_error(e):
                    raise OrderStateUnknown(spec.method, str(e), client_order_id) from e
                raise
            err, retryable = e, e.retryable
        if not behavior.retry_read(retryable, attempt):
            raise err
        delay = backoff.next_delay()
        if hooks.on_event is not None:
            hooks.emit(
                Event(EventKind.RETRY, rpc=spec.method, detail={"attempt": attempt, "delay": delay, "error": str(err)})
            )
        await asyncio.sleep(delay)


async def _once(
    transport: HttpTransport,
    url: str,
    spec: RpcSpec,
    body: bytes,
    headers: dict[str, str],
    timeout: float,
    hooks: Hooks,
    client_order_id: Any = None,
    deadline: CallDeadline | None = None,
    lite: Any = None,
) -> Any:
    if debug_enabled():
        log.debug("grvt: POST %s headers=%s body=%s", url, redact_headers(headers), redact(body))
    if deadline is not None:
        deadline.check()
    try:
        status, _, resp = await transport.post(url, body, headers, timeout)
    except asyncio.CancelledError as e:
        if spec.write:
            raise e from OrderStateUnknown(spec.method, "write cancelled while awaiting its response", client_order_id)
        raise
    if debug_enabled():
        log.debug("grvt: %s -> %d %s", spec.method, status, redact(resp[:2000]))
    if status == 200:
        try:
            if lite is not None:
                return lite.decode(resp, spec.response_type)
            return codec.decode(resp, spec.response_type)
        except Exception as e:
            raise DecodeError(spec.method, str(e)) from e
    try:
        err = codec.decode(resp)
    except Exception:
        err = None
    if lite is not None:
        err = lite.http_error(err)
    raise response_error(err, status, spec.method)
