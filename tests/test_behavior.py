"""The runtime's shared defaults and rules equal sdk/contract/behavior.json (grvt._generated.behavior).

A failure here means the runtime holds its own copy of a shared constant: use the generated one instead.
"""

import pytest
from conftest import INSTRUMENT

from grvt import AsyncGrvt
from grvt._generated import behavior
from grvt._runtime import ApiError, RateLimitError, Runtime, auth, errors
from grvt._runtime.clock import FixedClock, TimeCounterClientOrderId
from grvt._runtime.protocol import Backoff
from grvt._runtime.rest import read_backoff
from grvt.countdown import _validate


async def test_client_defaults():
    async with AsyncGrvt() as client:
        rt = client.rt
        assert rt.call_timeout == behavior.CALL_TIMEOUT
        assert rt.http_attempt_timeout == behavior.HTTP_ATTEMPT_TIMEOUT
        assert rt.session._timeout == behavior.HTTP_ATTEMPT_TIMEOUT
        assert rt.queue_size == behavior.QUEUE_SIZE
        assert rt._session_retry_delay == behavior.SESSION_RETRY_DELAY
        for gateway in ("md", "td"):
            socket = rt.socket(gateway)
            assert socket.request_timeout == behavior.WS_REQUEST_TIMEOUT
            assert socket.resync_timeout == behavior.RESYNC_TIMEOUT
            assert (socket.backoff.base, socket.backoff.cap) == (
                behavior.RECONNECT_BACKOFF_BASE,
                behavior.RECONNECT_BACKOFF_CAP,
            )


async def test_reads_try_read_attempts_times(gateway):
    gateway.rest_handler = lambda r, b: (503, {"code": 1015, "message": "unavailable", "status": 503})
    async with Runtime(gateway.env, encoding="full") as rt:
        with pytest.raises(ApiError):
            await rt.call(INSTRUMENT, {})
    assert len(gateway.rest_calls) == behavior.READ_ATTEMPTS


def test_backoff_and_session_constants():
    assert (Backoff().base, Backoff().cap) == (behavior.RECONNECT_BACKOFF_BASE, behavior.RECONNECT_BACKOFF_CAP)
    assert (read_backoff().base, read_backoff().cap) == (behavior.READ_BACKOFF_BASE, behavior.READ_BACKOFF_CAP)
    assert auth.REFRESH_AT == behavior.SESSION_RENEW_AT
    assert auth.DEFAULT_TTL == behavior.SESSION_DEFAULT_LIFETIME


def test_client_order_id_layout():
    seconds = 1_700_000_000
    ids = TimeCounterClientOrderId(FixedClock(seconds * 1_000_000_000))
    a, b = ids.next(), ids.next()
    for n in (a, b):
        assert n >> 63 == 1
        assert (n >> behavior.CLIENT_ORDER_ID_COUNTER_BITS) & behavior.CLIENT_ORDER_ID_SECONDS_MASK == seconds
    mask = behavior.CLIENT_ORDER_ID_COUNTER_MASK
    assert b & mask == (a + 1) & mask


def test_countdown_bounds():
    _validate(1, behavior.COUNTDOWN_MIN_SECONDS)
    _validate(1, behavior.COUNTDOWN_MAX_SECONDS)
    for bad in (behavior.COUNTDOWN_MIN_SECONDS - 1, behavior.COUNTDOWN_MAX_SECONDS + 1):
        with pytest.raises(ValueError):
            _validate(1, bad)


def test_error_rules():
    assert isinstance(errors.api_error(behavior.RATE_LIMIT_CODE, "slow", 429, "x"), RateLimitError)
    for status in behavior.READ_RETRY_STATUSES:
        assert errors.api_error(99999, "x", status, "x").retryable
    assert not errors.api_error(99999, "x", 500, "x").retryable
    known = 1003  # a contract code with HTTP status 400
    low, high = behavior.WRITE_REJECTED_STATUS_MIN, behavior.WRITE_REJECTED_STATUS_MAX
    assert not errors.uncertain_api_error(errors.api_error(known, "x", low, "x"))
    assert not errors.uncertain_api_error(errors.api_error(known, "x", high, "x"))
    assert errors.uncertain_api_error(errors.api_error(known, "x", high + 1, "x"))
    assert errors.uncertain_api_error(errors.api_error(known, "x", low - 1, "x"))
    server = 1002  # a contract code with HTTP status 500
    assert errors.uncertain_api_error(errors.api_error(server, "x", None, "x"))
