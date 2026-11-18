"""Fetch cursor pages lazily. The callback owns the call's filters and timeout."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Protocol, TypeVar

from ._runtime.errors import GrvtError


class Page(Protocol):
    @property
    def next(self) -> str | None: ...


P = TypeVar("P", bound=Page)


class PaginationCycle(GrvtError):
    """The server returned a cursor already used in this traversal."""

    def __init__(self, cursor: str):
        self.cursor = cursor
        super().__init__(f"pagination cursor repeated: {cursor!r}")


async def pages(fetch: Callable[[str | None], Awaitable[P]], *, cursor: str | None = None) -> AsyncIterator[P]:
    """Yield complete pages, including empty pages with a continuation cursor.

    No prefetch occurs. Breaking iteration sends no further calls. This follows
    server cursors; it cannot guarantee a stable snapshot while records change.
    """
    seen: set[str] = set()
    while True:
        if cursor:
            if cursor in seen:
                raise PaginationCycle(cursor)
            seen.add(cursor)
        page = await fetch(cursor)
        yield page
        cursor = page.next
        if not cursor:
            return


def pages_sync(fetch: Callable[[str | None], P], *, cursor: str | None = None) -> Iterator[P]:
    """Synchronous version of pages, with the same cursor and stopping rules."""
    seen: set[str] = set()
    while True:
        if cursor:
            if cursor in seen:
                raise PaginationCycle(cursor)
            seen.add(cursor)
        page = fetch(cursor)
        yield page
        cursor = page.next
        if not cursor:
            return
