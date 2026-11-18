"""Look up an uncertain order without placing or resending anything."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from ._generated import models
from ._generated.errors import NotFoundError


@dataclass(frozen=True, slots=True)
class OrderResolution:
    """A matching observed order, or an outcome that is still unknown.

    Not found can mean the order is not visible yet. Neither outcome permits an
    automatic resend. If IDs were reused, a match may belong to another request.
    """

    status: Literal["found", "still_unknown"]
    order: models.Order | None = None


class _AsyncLookup(Protocol):
    async def get_order(
        self, *, sub_account_id: int | str, client_order_id: int | str
    ) -> models.ApiGetOrderResponse: ...


class _SyncLookup(Protocol):
    def get_order(self, *, sub_account_id: int | str, client_order_id: int | str) -> models.ApiGetOrderResponse: ...


def _result(response: models.ApiGetOrderResponse, account: int | str, order_id: int | str) -> OrderResolution:
    order = response.result
    if order.sub_account_id == str(account) and order.metadata.client_order_id == str(order_id):
        return OrderResolution("found", order)
    return OrderResolution("still_unknown")


def _validate(account: int | str, order_id: int | str) -> tuple[str, str]:
    for value in (account, order_id):
        if (
            isinstance(value, bool)
            or not str(value).isascii()
            or not str(value).isdigit()
            or not 0 < int(value) < 1 << 64
        ):
            raise ValueError("sub_account_id and client_order_id must be positive uint64 values")
    return str(int(account)), str(int(order_id))


async def resolve_order(
    client: _AsyncLookup, *, sub_account_id: int | str, client_order_id: int | str
) -> OrderResolution:
    """Look up by client order ID once. Not found remains unknown; never resend."""
    sub_account_id, client_order_id = _validate(sub_account_id, client_order_id)
    try:
        response = await client.get_order(sub_account_id=sub_account_id, client_order_id=client_order_id)
    except NotFoundError:
        return OrderResolution("still_unknown")
    return _result(response, sub_account_id, client_order_id)


def resolve_order_sync(
    client: _SyncLookup, *, sub_account_id: int | str, client_order_id: int | str
) -> OrderResolution:
    """Blocking counterpart of resolve_order, with the same lookup-only guarantee."""
    sub_account_id, client_order_id = _validate(sub_account_id, client_order_id)
    try:
        response = client.get_order(sub_account_id=sub_account_id, client_order_id=client_order_id)
    except NotFoundError:
        return OrderResolution("still_unknown")
    return _result(response, sub_account_id, client_order_id)
