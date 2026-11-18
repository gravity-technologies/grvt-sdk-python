"""Errors raised by the SDK. Errors are raised, never returned.

ApiError        the server answered with an error code.
RateLimitError  the server said "slow down" (code 1006).
TransportError  we could not get an answer (connect failure, timeout on a read, socket closed).
DecodeError     a response or stream frame did not match the expected type.
OrderStateUnknown  a write may or may not have reached the exchange. Never resend blindly:
                   look the order up by client_order_id first.
OrderOutOfBounds   an order leg is off its instrument's tick or size step, or under its minimum size or
                   notional. Raised before signing; nothing was sent.
"""

from __future__ import annotations

from typing import Any, cast

from .._generated import behavior

# The codes the contract marks retryable; the generated error table fills it via register_error_codes().
# The retry and write-outcome rules themselves are generated from sdk/contract/behavior.json.
_RETRYABLE_CODES: set[int] = set()

# code -> ApiError subclass; filled by generated code.
_ERROR_CLASSES: dict[int, type[ApiError]] = {}
_ERROR_STATUS: dict[int, int] = {}  # code -> the contract's HTTP status for it


class GrvtError(Exception):
    """Base class for every SDK error."""


class ApiError(GrvtError):
    def __init__(
        self, code: int, message: str, status: int | None = None, rpc: str | None = None, retryable: bool | None = None
    ):
        self.code = code
        self.message = message
        self.status = status
        self.rpc = rpc
        if retryable is None:
            retryable = behavior.read_retryable(code, status, code in _RETRYABLE_CODES)
        self.retryable = retryable
        super().__init__(f"{rpc or '?'}: [{code}] {message}" + (f" (http {status})" if status else ""))


class RateLimitError(ApiError):
    pass


class TransportError(GrvtError):
    """No usable answer. `sent` says whether the request may have left this process."""

    def __init__(self, message: str, *, rpc: str | None = None, sent: bool = False):
        self.rpc = rpc
        self.sent = sent
        super().__init__(f"{rpc + ': ' if rpc else ''}{message}")


class FrameTooLarge(TransportError):
    """The encoded WS frame is larger than the server accepts. Nothing was sent."""


class DecodeError(GrvtError):
    """A response or stream frame did not match the expected type. The original error is its __cause__."""

    def __init__(self, rpc: str, message: str):
        self.rpc = rpc
        super().__init__(f"{rpc}: cannot decode: {message}")


class OrderStateUnknown(GrvtError):
    """A write was in flight when the connection dropped or the call timed out.

    The order may or may not exist. Resolve by looking it up (e.g. by client_order_id;
    client_order_ids lists every order of a bulk call); the SDK never resends a write.
    """

    def __init__(self, rpc: str, reason: str, client_order_id: str | int | list[str | int] | None = None):
        ids = (
            list(client_order_id)
            if isinstance(client_order_id, (list, tuple))
            else ([] if client_order_id is None else [client_order_id])
        )
        self.rpc = rpc
        self.reason = reason
        self.client_order_ids = ids  # every order the write carried (several for bulk_orders)
        self.client_order_id = ids[0] if len(ids) == 1 else None
        super().__init__(
            f"{rpc}: outcome unknown ({reason})"
            + (f", client_order_id={ids[0]}" if len(ids) == 1 else "")
            + (f", client_order_ids={ids}" if len(ids) > 1 else "")
        )


class OrderOutOfBounds(GrvtError, ValueError):
    """An order leg breaks one of its instrument's limits. Raised before signing; nothing was sent.

    The SDK never rounds an order: pick one of `nearest` (decimal strings) and send again. `server_code` is
    the gateway error the order would have got (e.g. 2064 invalid limit price tick).
    """

    def __init__(
        self,
        message: str,
        *,
        instrument: str,
        leg: int,
        field: str,
        check: str,
        limit: str,
        value: str,
        limit_value: str,
        nearest: list[str],
        server_code: int,
    ):
        self.instrument = instrument
        self.leg = leg
        self.field = field
        self.check = check
        self.limit = limit
        self.value = value
        self.limit_value = limit_value
        self.nearest = nearest
        self.server_code = server_code
        super().__init__(message)


class StreamOverflow(GrvtError):
    """The consumer fell behind a stream that may not drop messages.

    The queue was cleared and the subscription re-synced; items in between are lost.
    """

    def __init__(self, stream: str, selector: str, capacity: int):
        self.stream = stream
        self.selector = selector
        self.capacity = capacity
        super().__init__(f"{stream} {selector}: consumer fell behind {capacity} items; re-syncing")


def register_error_codes(classes: dict[int, type[ApiError]], retryable: set[int], statuses: dict[int, int]) -> None:
    """Called once by generated code with the contract's error table."""
    _ERROR_CLASSES.update(classes)
    _ERROR_STATUS.clear()
    _ERROR_STATUS.update(statuses)
    _RETRYABLE_CODES.clear()
    _RETRYABLE_CODES.update(retryable)


def uncertain_api_error(error: ApiError) -> bool:
    """Whether a write that failed with this error may have reached the exchange."""
    code_status = _ERROR_STATUS.get(error.code) if error.code in _ERROR_CLASSES else None
    return behavior.write_outcome_unknown(error.status, code_status)


def response_error(value: object, status: int | None, rpc: str) -> ApiError:
    if isinstance(value, dict):
        body = cast(dict[str, Any], value)
        code, message = body.get("code"), body.get("message")
        if type(code) is int and isinstance(message, str):
            return api_error(code, message, status, rpc)
    return api_error(0, "malformed error reply", status, rpc)


def api_error(code: int, message: str, status: int | None, rpc: str | None) -> ApiError:
    if code == behavior.RATE_LIMIT_CODE:
        return RateLimitError(code, message, status, rpc, retryable=True)
    cls = _ERROR_CLASSES.get(code, ApiError)
    return cls(code, message, status, rpc)
