"""A call has one budget, including preparation and shared waits."""

import asyncio
import json
import time
from dataclasses import replace

import pytest
from conftest import CREATE_ORDER, RAW_CREATE_ORDER, RAW_INSTRUMENT, order, wait_until

from grvt._runtime import OrderStateUnknown, Runtime

WRITE = replace(RAW_CREATE_ORDER, auth=False)


class Transport:
    def __init__(self):
        self.calls = []
        self.sent = asyncio.Event()
        self.block_write = False
        self.login_delay = 0.0
        self.status = 200

    async def post(self, url, body, headers, timeout):
        self.calls.append((url, json.loads(body), timeout))
        if url.endswith("login"):
            await asyncio.sleep(self.login_delay)
            return 200, {"Set-Cookie": "gravity=shared; Path=/", "X-Grvt-Account-Id": "1"}, b"{}"
        self.sent.set()
        if self.block_write:
            await asyncio.sleep(10)
        if self.status != 200:
            return self.status, {}, b'{"code":1002,"message":"busy"}'
        return 200, {}, b'{"result":{}}'

    async def get(self, url, timeout):
        return 200, {}, b"{}"

    async def close(self):
        pass


async def test_signing_timeout_and_blocking_hook_never_dispatch():
    transport = Transport()

    async def sign(rt, params):
        await asyncio.sleep(1)
        return params

    async with Runtime(transport=transport, encoding="full", call_timeout=0.01) as rt:
        with pytest.raises(TimeoutError):
            await rt.call(WRITE, {}, sign=sign)
    assert not transport.calls

    def blocking_hook(call):
        time.sleep(0.02)

    async with Runtime(transport=transport, encoding="full", call_timeout=0.005, before_send=blocking_hook) as rt:
        with pytest.raises(TimeoutError):
            await rt.call(WRITE, {})
    assert not transport.calls


async def test_cancelled_metadata_waiter_does_not_cancel_the_shared_load():
    transport = Transport()
    loads = 0

    async def load():
        nonlocal loads
        loads += 1
        await asyncio.sleep(0.05)
        return "metadata"

    async def sign(rt, params):
        await rt.metadata.get("instrument", "shared", load)
        return params

    async with Runtime(transport=transport, encoding="full") as rt:
        first = asyncio.create_task(rt.call(WRITE, {}, sign=sign, timeout=0.01))
        second = asyncio.create_task(rt.call(WRITE, {}, sign=sign, timeout=0.3))
        with pytest.raises(TimeoutError):
            await first
        await second
        assert loads == 1 and len(transport.calls) == 1


async def test_authentication_wait_uses_the_call_budget_without_poisoning_shared_login():
    transport = Transport()
    transport.login_delay = 0.05
    spec = replace(RAW_INSTRUMENT, auth=True)
    async with Runtime(transport=transport, encoding="full", api_key="k") as rt:
        first = asyncio.create_task(rt.call(spec, {}, timeout=0.01))
        second = asyncio.create_task(rt.call(spec, {}, timeout=0.3))
        with pytest.raises(TimeoutError):
            await first
        await second
        assert sum(url.endswith("login") for url, _, _ in transport.calls) == 1
        assert len(transport.calls) == 2


async def test_retry_backoff_shares_the_total_budget(monkeypatch):
    transport = Transport()
    transport.status = 503
    monkeypatch.setattr("grvt._runtime.rest.Backoff.next_delay", lambda _: 0.1)
    async with Runtime(transport=transport, encoding="full", call_timeout=0.02) as rt:
        with pytest.raises(TimeoutError):
            await rt.call(RAW_INSTRUMENT, {})
    assert len(transport.calls) == 1


@pytest.fixture(params=[False, True], ids=["explicit-cause", "context-only"])
def timeout_chaining(request, monkeypatch):
    if request.param:
        original = asyncio.Timeout.__aexit__

        async def context_only(self, *args):
            try:
                return await original(self, *args)
            except TimeoutError as error:
                # Python 3.11.0-3.11.2 preserves cancellation in context, without an explicit cause.
                assert isinstance(error.__context__, asyncio.CancelledError)
                error.__cause__ = None
                raise

        monkeypatch.setattr(asyncio.Timeout, "__aexit__", context_only)


@pytest.mark.parametrize("cancel", [False, True])
async def test_dispatched_http_write_keeps_ids_and_native_cancellation_cause(cancel, timeout_chaining):
    transport = Transport()
    transport.block_write = True
    async with Runtime(transport=transport, encoding="full") as rt:
        task = asyncio.create_task(
            rt.call(WRITE, {"order": {"metadata": {"client_order_id": "42"}}}, client_order_id="42", timeout=0.02)
        )
        await transport.sent.wait()
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else OrderStateUnknown) as ei:
            await task
        unknown = ei.value.__cause__ if cancel else ei.value
        assert isinstance(unknown, OrderStateUnknown)
        assert unknown.client_order_ids == ["42"]
        assert task.cancelled() == cancel
        cause = ei.value
        seen = set()
        while cause is not None:
            assert id(cause) not in seen
            seen.add(id(cause))
            cause = cause.__cause__
        assert len(transport.calls) == 1


async def test_dispatched_ws_write_deadline_removes_pending_entry(gateway, timeout_chaining):
    gateway.ignore_rpcs.add(CREATE_ORDER.method)
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key", order_transport="ws") as rt:
        await rt.session.ensure()
        await rt.socket("td").connected()
        task = asyncio.create_task(rt.call(CREATE_ORDER, order("42"), client_order_id="42", timeout=0.05))
        await wait_until(lambda: any(r["method"] == CREATE_ORDER.method for r in gateway.received))
        with pytest.raises(OrderStateUnknown) as ei:
            await task
        assert ei.value.client_order_ids == ["42"]
        assert len(rt.socket("td").requests) == 0


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
async def test_invalid_total_and_attempt_timeouts_are_rejected(value):
    with pytest.raises(ValueError):
        Runtime(call_timeout=value)
    with pytest.raises(ValueError):
        Runtime(http_attempt_timeout=value)
    async with Runtime() as rt:
        with pytest.raises(ValueError):
            await rt.call(WRITE, {}, timeout=value)


async def test_blocking_rest_logging_cannot_dispatch_after_deadline(monkeypatch):
    transport = Transport()
    monkeypatch.setattr("grvt._runtime.rest.debug_enabled", lambda: True)
    monkeypatch.setattr("grvt._runtime.rest.log.debug", lambda *args, **kwargs: time.sleep(0.02))
    async with Runtime(transport=transport, encoding="full", call_timeout=0.005) as rt:
        with pytest.raises(TimeoutError):
            await rt.call(WRITE, {})
    assert not transport.calls


async def test_hook_cannot_dispatch_after_swallowing_caller_cancellation():
    transport = Transport()
    ready = asyncio.Event()

    async def hook(call):
        ready.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async with Runtime(transport=transport, encoding="full", before_send=hook) as rt:
        task = asyncio.create_task(rt.call(WRITE, {}))
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not transport.calls


async def test_taskgroup_cancellation_keeps_write_uncertainty():
    transport = Transport()
    transport.block_write = True
    async with Runtime(transport=transport, encoding="full") as rt:
        async with asyncio.TaskGroup() as group:
            task = group.create_task(
                rt.call(WRITE, {"order": {"metadata": {"client_order_id": "42"}}}, client_order_id="42")
            )
            await transport.sent.wait()
            task.cancel()
        assert task.cancelled()
        with pytest.raises(asyncio.CancelledError) as ei:
            task.result()
        unknown = ei.value.__cause__
        assert isinstance(unknown, OrderStateUnknown)
        assert unknown.client_order_ids == ["42"]


async def test_ws_external_cancellation_stays_native(gateway):
    gateway.ignore_rpcs.add(CREATE_ORDER.method)
    async with Runtime(gateway.env, **gateway.client_options, api_key="test-key", order_transport="ws") as rt:
        task = asyncio.create_task(rt.call(CREATE_ORDER, order("42"), client_order_id="42"))
        await wait_until(lambda: any(r["method"] == CREATE_ORDER.method for r in gateway.received))
        task.cancel()
        with pytest.raises(asyncio.CancelledError) as ei:
            await task
        assert task.cancelled()
        unknown = ei.value.__cause__
        assert isinstance(unknown, OrderStateUnknown)
        assert unknown.client_order_ids == ["42"]
        assert len(rt.socket("td").requests) == 0
