"""Blocking facade: the same async runtime, driven from a background event-loop thread.

Generated sync clients wrap each async method as `runner.run(async_client.ns.method(...))` and each
stream as `runner.iterate(async_client.ns.stream(...))`. There is only one runtime.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class SyncRunner:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, name="grvt-loop", daemon=True)
        self._thread.start()

    def run(self, aw: Awaitable[T], timeout: float | None = None) -> T:
        if threading.current_thread() is self._thread:
            raise RuntimeError("SyncRunner.run called from its own loop thread (use the async client there)")
        return asyncio.run_coroutine_threadsafe(_as_coro(aw), self.loop).result(timeout)

    def build(self, factory: Callable[[], T]) -> T:
        """Construct an object on the loop thread (for objects that bind to the running loop)."""

        async def make() -> T:
            return factory()

        return self.run(make())

    def iterate(self, it: AsyncIterator[T]) -> Iterator[T]:
        while True:
            try:
                yield self.run(it.__anext__())
            except StopAsyncIteration:
                return

    def close(self) -> None:
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout=5)
        self.loop.close()


class SyncSubscription(Generic[T]):
    """Blocking view of a Subscription: iterate it, then close() (or use `with`)."""

    def __init__(self, runner: SyncRunner, sub: Any) -> None:
        self._runner = runner
        self._sub = sub

    def __iter__(self) -> Iterator[T]:
        return self._runner.iterate(self._sub)

    def start(self) -> SyncSubscription[T]:
        self._runner.run(self._sub.start())
        return self

    def close(self) -> None:
        self._runner.run(self._sub.close())

    def __enter__(self) -> SyncSubscription[T]:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.close()


async def _as_coro(aw: Awaitable[Any]) -> Any:
    return await aw
