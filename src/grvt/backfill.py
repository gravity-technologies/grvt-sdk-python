"""Read available history after a gap without claiming complete event recovery."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Generic, Literal

from ._runtime.hooks import GapRange
from .pagination import P, pages, pages_sync


@dataclass(frozen=True, slots=True)
class BackfillPage(Generic[P]):
    page: P
    gap: GapRange
    history_exhausted: bool
    continuity: Literal["unproven"] = field(default="unproven", init=False)


async def backfill_pages(
    fetch: Callable[[str | None], Awaitable[P]], *, gap: GapRange
) -> AsyncIterator[BackfillPage[P]]:
    """The callback chooses the REST endpoint and time window. Sequence bounds are
    not REST cursors. Empty history does not prove that missed events are visible.
    """
    async for page in pages(fetch):
        yield BackfillPage(page, gap, not page.next)


def backfill_pages_sync(fetch: Callable[[str | None], P], *, gap: GapRange) -> Iterator[BackfillPage[P]]:
    """Blocking counterpart, with the same unproven-continuity result."""
    for page in pages_sync(fetch):
        yield BackfillPage(page, gap, not page.next)
