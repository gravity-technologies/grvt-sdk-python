"""Grvt SDK (draft).

AsyncGrvt / Grvt are the clients (generated from the contract). The other names are the pieces users
plug in: environments, signers, hooks and errors. Wire types are in grvt.models, enums in grvt.enums.
"""

from ._generated import AsyncGrvt, Grvt, enums, models
from ._generated import errors as api_errors
from ._runtime.clock import Clock, SystemClock
from ._runtime.env import PROD, TESTNET, Environment
from ._runtime.errors import (
    ApiError,
    ClosedError,
    ConfigError,
    DecodeError,
    FrameTooLarge,
    GrvtError,
    OrderOutOfBounds,
    OrderStateUnknown,
    RateLimitError,
    SigningError,
    StreamOverflow,
    TransportError,
)
from ._runtime.hooks import Call, Event, EventKind, GapRange
from ._runtime.options import CallOptions, ClientOptions
from ._runtime.scalars import Timestamp
from ._runtime.signing import Domain, PrivateKeySigner, Signature, Signer
from ._runtime.transport import HttpTransport
from .backfill import BackfillPage, backfill_pages, backfill_pages_sync
from .countdown import CountdownReceipt, refresh_cancel_countdown, refresh_cancel_countdown_sync
from .orderbook import BookError, BookSnapshot, OrderBook
from .pagination import PaginationCycle, pages, pages_sync
from .recovery import OrderResolution, resolve_order, resolve_order_sync

__all__ = [
    "CountdownReceipt",
    "refresh_cancel_countdown",
    "refresh_cancel_countdown_sync",
    "BackfillPage",
    "backfill_pages",
    "backfill_pages_sync",
    "OrderBook",
    "BookSnapshot",
    "BookError",
    "PaginationCycle",
    "pages",
    "pages_sync",
    "CallOptions",
    "ClientOptions",
    "OrderResolution",
    "resolve_order",
    "resolve_order_sync",
    "AsyncGrvt",
    "Grvt",
    "enums",
    "models",
    "api_errors",
    "Clock",
    "SystemClock",
    "PROD",
    "TESTNET",
    "Environment",
    "ApiError",
    "ClosedError",
    "ConfigError",
    "DecodeError",
    "FrameTooLarge",
    "GrvtError",
    "OrderOutOfBounds",
    "OrderStateUnknown",
    "RateLimitError",
    "SigningError",
    "StreamOverflow",
    "TransportError",
    "Call",
    "Event",
    "EventKind",
    "GapRange",
    "Domain",
    "PrivateKeySigner",
    "Signature",
    "Signer",
    "HttpTransport",
    "Timestamp",
]
