"""Descriptors the generator emits, one per RPC and one per stream. The runtime reads only these."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RpcSpec:
    method: str  # JSON-RPC method and REST route tail, e.g. "v1/create_order"
    gateway: str  # "md" | "td" | "edge"
    path: str  # REST path, e.g. "/full/v1/create_order" (edge routes differ)
    response_type: Any  # msgspec type to decode the REST body / WS `result` into
    auth: bool = False
    write: bool = False  # changes state: never retried; ambiguous failure -> OrderStateUnknown
    order_entry: bool = False  # goes over the trading WS when order_transport="ws"
    # Error codes that mean the cached instrument limits may be stale: the runtime drops the request's leg
    # instruments from the metadata cache (the order itself is never resent).
    refresh_metadata_on: frozenset[int] = frozenset()
    # The request model, and the lite-encoding path; both None for edge routes, which stay full.
    request_type: Any = None
    lite_path: str | None = None


@dataclass(frozen=True, slots=True)
class StreamSpec:
    stream: str  # e.g. "v1.book.d"
    gateway: str  # "md" | "td"
    feed_type: Any  # msgspec type of one feed frame (stream, selector, sequence_number, feed, ...)
    auth: bool = False
    # Every frame is a full snapshot (e.g. "v1.ticker.s"): a gap is reported but needs no resync.
    snapshot_feed: bool = False
    # Old items may be dropped when the consumer is slow: snapshot streams only. Every other stream
    # (deltas, events, private) raises StreamOverflow and re-syncs instead of silently dropping.
    conflate: bool = False
