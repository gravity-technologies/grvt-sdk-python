"""Client settings and per-call transport options."""

from dataclasses import dataclass
from math import isfinite
from typing import Literal, TypedDict

from .clock import ClientOrderIdProvider, Clock, NonceProvider
from .hooks import BeforeSend, OnEvent
from .transport import HttpTransport


class ClientOptions(TypedDict, total=False):
    """Optional client settings, accepted as named constructor arguments."""

    sub_account_id: int | str | None
    encoding: Literal["full", "lite"]
    check_orders: bool
    clock: Clock | None
    nonces: NonceProvider | None
    client_order_ids: ClientOrderIdProvider | None
    transport: HttpTransport | None
    before_send: BeforeSend | None
    on_event: OnEvent | None
    http_attempt_timeout: float
    call_timeout: float
    ws_request_timeout: float
    resync_timeout: float
    queue_size: int


@dataclass(frozen=True, slots=True)
class CallOptions:
    """Bound the complete call in seconds, including authentication, signing and retries.

    The default is the client's call_timeout (30 seconds).
    HTTP attempts have a separate http_attempt_timeout cap (10 seconds by default).
    """

    timeout: float | None = None

    def __post_init__(self) -> None:
        if self.timeout is not None and (not isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("timeout must be finite and positive")
