"""Explicit arm/refresh of the server's account-wide cancellation countdown."""

from dataclasses import dataclass
from typing import Protocol

from ._generated import behavior
from ._generated.models import AckResponse
from ._runtime.options import CallOptions


@dataclass(frozen=True, slots=True)
class CountdownReceipt:
    sub_account_id: str
    countdown_seconds: int
    acknowledged: bool

    @property
    def scope(self) -> str:
        return "sub_account"


class _AsyncCountdown(Protocol):
    async def cancel_on_disconnect(
        self, *, sub_account_id: int | str, countdown_time: int | str, options: CallOptions | None = None
    ) -> AckResponse: ...


class _SyncCountdown(Protocol):
    def cancel_on_disconnect(
        self, *, sub_account_id: int | str, countdown_time: int | str, options: CallOptions | None = None
    ) -> AckResponse: ...


def _validate(account: int | str, seconds: object) -> str:
    low, high = behavior.COUNTDOWN_MIN_SECONDS, behavior.COUNTDOWN_MAX_SECONDS
    if isinstance(seconds, bool) or not isinstance(seconds, int) or not low <= seconds <= high:
        raise ValueError(f"countdown_seconds must be an integer from {low} to {high}")
    if (
        isinstance(account, bool)
        or not str(account).isascii()
        or not str(account).isdigit()
        or not 0 < int(account) < 2**64
    ):
        raise ValueError("sub_account_id must be a positive uint64")
    return str(int(account))


async def refresh_cancel_countdown(
    client: _AsyncCountdown, *, sub_account_id: int | str, countdown_seconds: int, options: CallOptions | None = None
) -> CountdownReceipt:
    """Send once, never retry or stop the timer. An ambiguous failure propagates.

    The receipt records an acknowledgment, not proof the timer remains active:
    another caller can replace it. Reconnect alone never invokes this helper.
    """
    account = _validate(sub_account_id, countdown_seconds)
    response = await client.cancel_on_disconnect(
        sub_account_id=account, countdown_time=countdown_seconds * 1000, options=options
    )
    return CountdownReceipt(account, countdown_seconds, response.result.ack)


def refresh_cancel_countdown_sync(
    client: _SyncCountdown, *, sub_account_id: int | str, countdown_seconds: int, options: CallOptions | None = None
) -> CountdownReceipt:
    """Blocking counterpart with the same explicit, single-write behavior."""
    account = _validate(sub_account_id, countdown_seconds)
    response = client.cancel_on_disconnect(
        sub_account_id=account, countdown_time=countdown_seconds * 1000, options=options
    )
    return CountdownReceipt(account, countdown_seconds, response.result.ack)
