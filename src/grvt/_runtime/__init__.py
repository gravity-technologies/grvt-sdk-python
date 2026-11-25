"""Hand-written runtime. Fixed size: it never grows with the API. Generated code imports from here."""

from . import codec, protocol
from .auth import Session
from .clock import FixedClock, RandomNonce, SystemClock, TimeCounterClientOrderId
from .env import PRODUCTION, TESTNET, Environment, resolve_env
from .errors import (
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
    register_error_codes,
)
from .hooks import Call, Event, EventKind, Hooks
from .runtime import MetadataCache, Runtime
from .signing import Domain, PrivateKeySigner, Signature, Signer, StructType, grvt_domain, scaled, to_address, to_int
from .spec import RpcSpec, StreamSpec
from .sync import SyncRunner, SyncSubscription
from .transport import AiohttpTransport, HttpTransport
from .ws import Subscription, WsSocket

__all__ = [
    "codec",
    "protocol",
    "Session",
    "FixedClock",
    "RandomNonce",
    "SystemClock",
    "TimeCounterClientOrderId",
    "PRODUCTION",
    "TESTNET",
    "Environment",
    "resolve_env",
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
    "register_error_codes",
    "Call",
    "Event",
    "EventKind",
    "Hooks",
    "MetadataCache",
    "Runtime",
    "Domain",
    "PrivateKeySigner",
    "Signature",
    "Signer",
    "StructType",
    "grvt_domain",
    "scaled",
    "to_address",
    "to_int",
    "RpcSpec",
    "StreamSpec",
    "SyncRunner",
    "SyncSubscription",
    "AiohttpTransport",
    "HttpTransport",
    "Subscription",
    "WsSocket",
]
