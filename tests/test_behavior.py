"""The runtime's shared defaults and rules equal sdk/contract/behavior.json (grvt._generated.behavior).

A failure here means the runtime holds its own copy of a shared constant: use the generated one instead.
"""

import asyncio
import dataclasses
import inspect
from collections.abc import Callable
from typing import Any

import pytest
from conftest import INSTRUMENT
from shared_data import load_shared

import grvt
from grvt import AsyncGrvt, ClientOptions, ConfigError
from grvt._generated import behavior
from grvt._runtime import ApiError, Event, EventKind, RateLimitError, Runtime, TransportError, auth, errors
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
            assert socket.idle_timeout == behavior.WS_IDLE_TIMEOUT
            assert (socket.backoff.base, socket.backoff.cap) == (
                behavior.RECONNECT_BACKOFF_BASE,
                behavior.RECONNECT_BACKOFF_CAP,
            )


async def test_reads_try_read_attempts_times(gateway):
    gateway.rest_handler = lambda r, b: (503, {"code": 1015, "message": "unavailable", "status": 503})
    async with Runtime(gateway.env, **gateway.client_options) as rt:
        with pytest.raises(ApiError):
            await rt.call(INSTRUMENT, {"instrument": "BTC_USDT_Perp"})
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


# -- options and errors: the "options" and "errors" tables of behavior.json --------------------------------

SHARED = load_shared("contract/behavior.json")


@pytest.mark.parametrize("option", SHARED["options"], ids=lambda o: o["name"])
async def test_option(option):
    """Each shared option is a client keyword with the shared default, and a value below its minimum (or a
    duration that isn't positive) is a ConfigError when the client is built."""
    name = option["name"]
    assert name in ClientOptions.__annotations__
    assert inspect.signature(Runtime).parameters[name].default == getattr(behavior, option["default"].upper())
    lowest: dict[str, Any] = {name: option["min"] if "min" in option else 1e-3}
    below: dict[str, Any] = {name: option["min"] - 1 if "min" in option else 0}
    with pytest.raises(ConfigError):
        AsyncGrvt(**below)
    async with AsyncGrvt(**lowest):
        pass


# One instance of each kind, to check the fields it carries.
ERROR_SAMPLES: dict[str, Callable[[], BaseException]] = {
    "api": lambda: ApiError(1003, "bad", 400, "v1/x"),
    "rate_limit": lambda: RateLimitError(1006, "slow", 429, "v1/x"),
    "transport": lambda: TransportError("down", rpc="v1/x", sent=True),
    "order_state_unknown": lambda: grvt.OrderStateUnknown("v1/x", "timeout", 1),
    "order_out_of_bounds": lambda: grvt.OrderOutOfBounds(
        "m",
        instrument="I",
        leg=0,
        field="size",
        check="at_least",
        limit="min_size",
        value="0",
        limit_value="1",
        nearest=["1"],
        server_code=2000,
    ),
    "stream_overflow": lambda: grvt.StreamOverflow("v1.ticker.d", "I@500", 1),
    "frame_too_large": lambda: grvt.FrameTooLarge(2, 1),
    "decode": lambda: grvt.DecodeError("v1/x", "bad"),
    "signing": lambda: grvt.SigningError("bad"),
    "config": lambda: ConfigError("bad"),
    "closed": lambda: grvt.ClosedError(),
    "pagination_cycle": lambda: grvt.PaginationCycle("c"),
    "book": lambda: grvt.BookError("not_ready"),
}


def test_exported_errors_are_the_shared_kinds():
    exported = {n for n, v in vars(grvt).items() if inspect.isclass(v) and issubclass(v, BaseException)}
    assert exported == {e["names"]["python"] for e in SHARED["errors"]} | {"GrvtError"}
    assert ERROR_SAMPLES.keys() == {e["kind"] for e in SHARED["errors"]}


@pytest.mark.parametrize("kind", SHARED["errors"], ids=lambda e: e["kind"])
def test_error_kind(kind):
    names = {e["kind"]: e["names"]["python"] for e in SHARED["errors"]}
    cls = getattr(grvt, kind["names"]["python"])
    parent = getattr(grvt, names[kind["parent"]]) if "parent" in kind else grvt.GrvtError
    assert parent in cls.__bases__, f"{cls.__name__} extends {cls.__bases__}, want {parent.__name__}"
    err = ERROR_SAMPLES[kind["kind"]]()
    assert type(err) is cls
    renamed = kind.get("renamed", {}).get("python", {})
    for field in kind["fields"]:
        assert hasattr(err, renamed.get(field, field)), f"{cls.__name__} has no {field}"


def test_errors_that_were_value_errors_still_are():
    for cls in (grvt.SigningError, ConfigError, grvt.OrderOutOfBounds):
        assert issubclass(cls, ValueError)


# -- events: the "events" table of behavior.json ------------------------------------------------------------


def test_event_kinds_are_the_shared_kinds():
    assert {k.value for k in EventKind} == {e["kind"] for e in SHARED["events"]}


def test_event_attributes_are_the_shared_fields():
    # One flat Event, as in Go: every kind's fields are attributes, set only for the kinds they belong to.
    # kind and ts belong to the container, not to any one kind.
    want = {"kind", "ts"}
    for e in SHARED["events"]:
        renamed = e.get("renamed", {}).get("python", {})
        want |= {renamed.get(f, f) for f in e["fields"]}
    assert {f.name for f in dataclasses.fields(Event)} == want


async def test_failed_session_renewal_is_retried_after_the_retry_delay(gateway, monkeypatch):
    """After a failed re-login the next try waits session_retry_delay (or until the renewal is due, if later),
    not the retry delay and then the minimum wait again."""
    events: list[Event] = []
    waits: list[tuple[float, int]] = []  # each wait, with the logins tried before it
    logins = 0

    async def login(self) -> None:
        nonlocal logins
        logins += 1
        if logins == 1:
            raise TransportError("login failed", sent=False)

    async def sleep(seconds: float) -> None:
        waits.append((seconds, logins))
        if len(waits) == 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(auth.Session, "login", login)
    monkeypatch.setattr(auth.Session, "refresh_due", lambda self: 0.0)  # always due
    monkeypatch.setattr("grvt._runtime.runtime.asyncio.sleep", sleep)
    rt = Runtime(gateway.env, **gateway.client_options, api_key="k", on_event=events.append)
    with pytest.raises(asyncio.CancelledError):
        await rt._rotate_forever()
    # Due at once, fails, retried after the retry delay alone, succeeds, then waits for the next renewal.
    assert waits == [
        (behavior.SESSION_RENEW_MIN_WAIT, 0),
        (behavior.SESSION_RETRY_DELAY, 1),
        (behavior.SESSION_RENEW_MIN_WAIT, 2),
    ]
    error = next(e for e in events if e.kind is EventKind.ERROR)
    assert (error.fatal, error.socket) == (False, None)
    assert error.error is not None and error.error.startswith("session: ")
