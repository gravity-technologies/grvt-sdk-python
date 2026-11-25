"""Time, nonces and client order ids. All three are constructor args so tests can pin them."""

from __future__ import annotations

import itertools
import os
import secrets
import time
from typing import Protocol

from .._generated import behavior


class Clock(Protocol):
    def now_ns(self) -> int: ...


class SystemClock:
    """Local wall clock plus an optional offset to the server clock.

    `observe(server_ms, rtt_ms)` (e.g. from GET /time) moves the offset toward the server with
    simple exponential smoothing, so one slow sample cannot jump the clock.
    """

    def __init__(self) -> None:
        self.offset_ns = 0
        self._samples = 0

    def now_ns(self) -> int:
        return time.time_ns() + self.offset_ns

    def observe(self, server_ms: int, rtt_ms: float = 0.0) -> None:
        local_mid = time.time_ns() - int(rtt_ms * 1e6 / 2)
        sample = server_ms * 1_000_000 - local_mid
        if self._samples == 0:
            self.offset_ns = sample
        else:
            self.offset_ns = int(self.offset_ns * 0.8 + sample * 0.2)
        self._samples += 1


class FixedClock:
    def __init__(self, ns: int) -> None:
        self.ns = ns

    def now_ns(self) -> int:
        return self.ns


class NonceProvider(Protocol):
    def next(self) -> int: ...


class RandomNonce:
    """uint32 signature nonce (the EIP-712 Order.nonce is uint32)."""

    def next(self) -> int:
        return secrets.randbits(32)


class ClientOrderIdProvider(Protocol):
    def next(self) -> int: ...


def _new_counter() -> itertools.count[int]:
    return itertools.count(secrets.randbits(behavior.CLIENT_ORDER_ID_COUNTER_BITS))


_process_counter = _new_counter()


def _reseed_after_fork() -> None:
    global _process_counter
    _process_counter = _new_counter()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reseed_after_fork)


class TimeCounterClientOrderId:
    """An ID from this clock's Unix second and the process-wide 31-bit counter."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()

    def next(self) -> int:
        seconds = self._clock.now_ns() // 1_000_000_000
        return (
            (1 << 63)
            | ((seconds & behavior.CLIENT_ORDER_ID_SECONDS_MASK) << behavior.CLIENT_ORDER_ID_COUNTER_BITS)
            | (next(_process_counter) & behavior.CLIENT_ORDER_ID_COUNTER_MASK)
        )
