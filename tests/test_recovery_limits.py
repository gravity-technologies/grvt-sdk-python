import pytest

from grvt import (
    CallOptions,
    OrderStateUnknown,
    models,
    refresh_cancel_countdown,
    refresh_cancel_countdown_sync,
)


class Countdown:
    def __init__(self):
        self.calls = []
        self.error: Exception | None = None

    def cancel_on_disconnect(self, **request):
        self.calls.append(request)
        if self.error:
            raise self.error
        return models.AckResponse(result=models.Ack(ack=True))


class AsyncCountdown:
    def __init__(self, sync):
        self.sync = sync

    async def cancel_on_disconnect(self, **request):
        return self.sync.cancel_on_disconnect(**request)


async def test_countdown_is_explicit_single_write_and_never_disarms():
    sync = Countdown()
    client = AsyncCountdown(sync)
    options = CallOptions(timeout=2)
    receipt = await refresh_cancel_countdown(client, sub_account_id="001", countdown_seconds=120, options=options)
    assert receipt.acknowledged and receipt.scope == "sub_account"
    assert sync.calls == [{"sub_account_id": "1", "countdown_time": 120000, "options": options}]
    for seconds in [0, 301, True, 1.5]:
        with pytest.raises(ValueError):
            await refresh_cancel_countdown(client, sub_account_id=1, countdown_seconds=seconds)
    assert len(sync.calls) == 1
    sync.error = OrderStateUnknown("cancel_on_disconnect", "timeout")
    with pytest.raises(OrderStateUnknown):
        await refresh_cancel_countdown(client, sub_account_id=1, countdown_seconds=120)
    assert len(sync.calls) == 2
    with pytest.raises(OrderStateUnknown):
        refresh_cancel_countdown_sync(sync, sub_account_id=1, countdown_seconds=120)
    assert len(sync.calls) == 3
