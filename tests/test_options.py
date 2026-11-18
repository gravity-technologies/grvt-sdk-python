"""Call options reach the transport without becoming request fields."""

import inspect
import json
from unittest.mock import AsyncMock

import pytest
from conftest import instrument_payload

from grvt import AsyncGrvt, CallOptions, Grvt
from grvt._runtime import Runtime
from grvt._runtime.options import ClientOptions
from grvt._runtime.transport import AiohttpTransport


class RecordingTransport(AiohttpTransport):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def post(self, url, body, headers, timeout):
        self.calls.append((json.loads(body), timeout))
        return 200, {}, json.dumps({"result": instrument_payload()}).encode()


async def test_async_call_options_apply_to_one_request():
    transport = RecordingTransport()
    async with AsyncGrvt(transport=transport, encoding="full", http_attempt_timeout=7) as client:
        await client.get_instrument(instrument="BTC_USDT_Perp", options=CallOptions(timeout=0.25))
        await client.get_instrument(instrument="BTC_USDT_Perp")
    assert transport.calls == [({"instrument": "BTC_USDT_Perp"}, 7), ({"instrument": "BTC_USDT_Perp"}, 7)]


def test_sync_call_options_reach_async_transport():
    transport = RecordingTransport()
    with Grvt(transport=transport, encoding="full") as client:
        client.get_instrument(instrument="BTC_USDT_Perp", options=CallOptions(timeout=0.5))
    assert transport.calls == [({"instrument": "BTC_USDT_Perp"}, 10)]


async def test_ws_call_options_reach_socket(gateway):
    async with AsyncGrvt(gateway.env, encoding="full", api_key="test-key", order_transport="ws") as client:
        socket = client.rt.socket("td")
        socket.request = AsyncMock(return_value=b'{"result":{"ack":true}}')
        await client.cancel_order(sub_account_id=1, client_order_id=2, options=CallOptions(timeout=0.25))
        assert socket.request.call_args.kwargs["deadline"].expires > 0
        assert socket.request.call_args.kwargs["write"] is True


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_timeout_must_be_positive_and_finite(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        CallOptions(timeout=timeout)


def test_client_options_match_runtime_keywords():
    keywords = {
        name
        for name, parameter in inspect.signature(Runtime.__init__).parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    assert set(ClientOptions.__annotations__) == keywords - {"api_key", "signer", "order_transport"}
