import pytest

from grvt import AsyncGrvt, enums, models


@pytest.mark.asyncio
async def test_default_account_and_explicit_overrides_do_not_mutate_orders():
    captured = []

    def capture(call):
        captured.append(call.params)
        raise RuntimeError("captured")

    async with AsyncGrvt(sub_account_id="123", before_send=capture) as client:
        with pytest.raises(RuntimeError, match="captured"):
            await client.get_order(client_order_id="1")
        assert captured[-1].sub_account_id == "123"
        with pytest.raises(RuntimeError, match="captured"):
            await client.get_order(sub_account_id="456", client_order_id="1")
        assert captured[-1].sub_account_id == "456"
        order = models.OrderInput(time_in_force=enums.TimeInForce.GOOD_TILL_TIME, legs=[])
        with pytest.raises(RuntimeError, match="captured"):
            await client.create_order(order=order)
        assert captured[-1].order.sub_account_id == "123"
        assert order.sub_account_id is None
        with pytest.raises(RuntimeError, match="captured"):
            await client.bulk_orders(sub_account_id="456", orders=[order])
        assert captured[-1].orders[0].sub_account_id == "456"
        assert order.sub_account_id is None
        assert client.order_stream().selector == "123"
        assert client.order_stream(sub_account_id="456").selector == "456"


@pytest.mark.asyncio
async def test_missing_account_fails_before_sending():
    async with AsyncGrvt() as client:
        with pytest.raises(ValueError, match="sub_account_id is required"):
            await client.get_order(client_order_id="1")
    for value in [0, -1, "abc", str(2**64)]:
        with pytest.raises(ValueError, match="positive uint64"):
            AsyncGrvt(sub_account_id=value)


@pytest.mark.asyncio
@pytest.mark.parametrize("default", [None, "123"])
async def test_transfer_selector_keeps_optional_account_omitted(default):
    async with AsyncGrvt(sub_account_id=default) as client:
        assert client.transfer_stream(main_account_id="main").selector == "main"
        assert client.transfer_stream(main_account_id="main", sub_account_id="456").selector == "main-456"
