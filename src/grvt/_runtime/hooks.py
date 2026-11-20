"""User hooks.

before_send(call)  runs before signing for every RPC (REST or WS). It may change `call.params`
                   or return a new Call, but must not add, remove or reorder orders, or change their client order IDs (that raises
                   ConfigError and sends nothing). It runs before signing, so nothing can change a
                   request after it is signed.
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


# The kind-specific attributes each `detail` dict holds, as before the attributes existed ("dir" was direction).
_DETAIL: dict[EventKind, tuple[str, ...]] = {
    EventKind.DISCONNECT: ("code", "reason"),
    EventKind.SUBSCRIBE: ("latest_sequence_number", "num_snapshots"),
    EventKind.GAP: ("expected_prev", "prev", "seq"),
    EventKind.RETRY: ("attempt", "delay", "error"),
    EventKind.FRAME: ("direction",),
    EventKind.SESSION_ROTATED: ("generation", "expires_at"),
    EventKind.ERROR: ("fatal", "error"),
}


@dataclass(slots=True)
class Event:
    """What on_event receives. Only the attributes that apply to `kind` are set; the rest stay None.

    The kinds, their attributes and when each is emitted are the `events` table in sdk/contract/behavior.json.
    """

    kind: EventKind
    gap: GapRange | None = None  # GAP: the recovery bounds
    websocket_reconnect_required: bool | None = None  # SESSION_ROTATED, not a continuity guarantee
    socket: str | None = None  # "md" | "td"; None for REST and session events
    stream: str | None = None  # SUBSCRIBE, GAP
    selector: str | None = None  # SUBSCRIBE, GAP
    rpc: str | None = None  # RETRY of a read
    # FRAME only: raw bytes as sent or received. NOT redacted.
    raw: bytes | None = None
    ts: float = field(default_factory=time.time)
    code: int | None = None  # DISCONNECT: the close code, if any
    reason: str | None = None  # DISCONNECT
    latest_sequence_number: int | None = None  # SUBSCRIBE
    num_snapshots: int | None = None  # SUBSCRIBE
    # GAP of a sequence_mismatch only: diagnostics; gap owns the recovery bounds.
    expected_prev: int | None = None
    prev: int | None = None
    seq: int | None = None
    attempt: int | None = None  # RETRY, counting from 1
    delay: float | None = None  # RETRY: seconds before the next try
    error: str | None = None  # RETRY, ERROR
    fatal: bool | None = None  # ERROR: the socket stopped for good
    direction: str | None = None  # FRAME: "in" or "out"
    generation: int | None = None  # SESSION_ROTATED
    expires_at: float | None = None  # SESSION_ROTATED: Unix seconds

    @property
    def detail(self) -> dict[str, Any]:
        """The kind's attributes as a dict, kept for code written before they were attributes ("dir" for a frame's
        direction). Read the attributes instead."""
        out = {name: getattr(self, name) for name in _DETAIL.get(self.kind, ())}
        if "direction" in out:
            out["dir"] = out.pop("direction")
        return out


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
