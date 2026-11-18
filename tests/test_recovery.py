import pytest

from grvt import Timestamp, enums, models, resolve_order, resolve_order_sync
from grvt._generated.errors import NotFoundError
from grvt._runtime.errors import ApiError


class Lookup:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def get_order(self, **request):
        self.calls.append(request)
        if isinstance(self.result, Exception):
            raise self.result
        return models.ApiGetOrderResponse(result=self.result)


class AsyncLookup:
    def __init__(self, result):
        self.lookup = Lookup(result)
        self.calls = self.lookup.calls

    async def get_order(self, **request):
        return self.lookup.get_order(**request)


def order_for(account):
    return models.Order(
        sub_account_id=account,
        time_in_force=enums.TimeInForce.GOOD_TILL_TIME,
        legs=[],
        signature=models.Signature(
            signer="0x0", r="0x0", s="0x0", v=27, nonce=1, expiration=Timestamp(0), chain_id="326"
        ),
        metadata=models.OrderMetadata(client_order_id="2"),
    )


@pytest.mark.parametrize("value", [NotFoundError(1004, "not visible"), order_for("99")])
async def test_missing_or_mismatched_stays_unknown(value):
    client = AsyncLookup(value)
    result = await resolve_order(client, sub_account_id="1", client_order_id="2")
    assert result.status == "still_unknown" and result.order is None
    assert client.calls == [{"sub_account_id": "1", "client_order_id": "2"}]


async def test_matching_order_and_error_propagation():
    order = order_for("1")
    client = AsyncLookup(order)
    result = await resolve_order(client, sub_account_id="1", client_order_id="2")
    assert result.status == "found" and result.order is order
    with pytest.raises(ApiError):
        await resolve_order(AsyncLookup(ApiError(1001, "forbidden")), sub_account_id="1", client_order_id="2")


def test_sync_lookup_and_invalid_id():
    client = Lookup(NotFoundError(1004, "not visible"))
    assert resolve_order_sync(client, sub_account_id="1", client_order_id="2").status == "still_unknown"
    with pytest.raises(ValueError):
        resolve_order_sync(client, sub_account_id="1", client_order_id="0")
    assert len(client.calls) == 1
