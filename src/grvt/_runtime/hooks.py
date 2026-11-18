"""User hooks.

before_send(call)  runs before signing for every RPC (REST or WS). It may change `call.params`
                   or return a new Call. It runs before signing, so nothing can change a request
                   after it is signed.
on_event(event)    read-only listener for connection and stream events. Must not block.

When a hook is not set the runtime skips it with a single `is None` check.
"""

from __future__ import annotations

import enum
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("grvt")


class EventKind(enum.Enum):
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    RECONNECT = "reconnect"
    SUBSCRIBE = "subscribe"
    GAP = "gap"
    RETRY = "retry"
    FRAME = "frame"
    SESSION_ROTATED = "session_rotated"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class GapRange:
    """Potentially missed sequences: exclusive start, inclusive end.

    None means the bound is unknown. An open end includes the re-subscription
    window; a new snapshot cannot recover private events. Sequence numbers may
    be sparse and REST history does not accept this range as a resume cursor.
    """

    after_sequence: int | None
    through_sequence: int | None
    reason: str


@dataclass(slots=True)
class Event:
    kind: EventKind
    gap: GapRange | None = None
    websocket_reconnect_required: bool | None = None  # SESSION_ROTATED, not a continuity guarantee
    socket: str | None = None  # "md" | "td" | None for REST
    stream: str | None = None
    selector: str | None = None
    rpc: str | None = None
    # expected_prev/prev/seq are sequence_mismatch diagnostics only; gap owns recovery bounds.
    detail: dict[str, Any] = field(default_factory=dict[str, Any])
    # FRAME only: raw bytes as sent/received ("in"/"out" in detail["dir"]). NOT redacted.
    raw: bytes | None = None
    ts: float = field(default_factory=time.time)


@dataclass(slots=True)
class Call:
    rpc: str  # e.g. "v1/create_order"
    params: Any  # msgspec Struct (generated model) or dict
    transport: str  # "rest" | "ws"


BeforeSend = Callable[[Call], "Call | None | Awaitable[Call | None]"]
OnEvent = Callable[[Event], None]


class Hooks:
    __slots__ = ("before_send", "on_event")

    def __init__(self, before_send: BeforeSend | None = None, on_event: OnEvent | None = None):
        self.before_send = before_send
        self.on_event = on_event

    def emit(self, event: Event) -> None:
        # Callers check `hooks.on_event is not None` before building the Event.
        try:
            self.on_event(event)  # type: ignore[misc]
        except Exception:  # a listener must never break the runtime
            log.exception("grvt: on_event hook raised")

    async def run_before_send(self, call: Call) -> Call:
        out = self.before_send(call)  # type: ignore[misc]
        if inspect.isawaitable(out):
            out = await out
        return call if out is None else out
