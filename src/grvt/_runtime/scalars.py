"""Exact timestamps. Convert to datetime only when microsecond precision is sufficient."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


class Timestamp:
    """Signed Unix nanoseconds, with no floating-point conversion."""

    __slots__ = ("_nanoseconds",)

    def __init__(self, nanoseconds: int) -> None:
        if type(nanoseconds) is not int:
            raise TypeError("nanoseconds must be an integer")
        if not -(1 << 63) <= nanoseconds < (1 << 63):
            raise ValueError("timestamp is outside the signed 64-bit range")
        self._nanoseconds = nanoseconds

    @property
    def nanoseconds(self) -> int:
        return self._nanoseconds

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.nanoseconds == other.nanoseconds

    def __repr__(self) -> str:
        return f"Timestamp({self.nanoseconds})"

    def __int__(self) -> int:
        return self.nanoseconds

    def __str__(self) -> str:
        return str(self.nanoseconds)

    def to_datetime(self) -> datetime:
        """Return UTC, rounding down to the preceding microsecond."""
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=self.nanoseconds // 1000)

    @classmethod
    def from_datetime(cls, value: datetime) -> Timestamp:
        """Convert an aware datetime exactly; reject ambiguous local times."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must have a timezone")
        delta = value - datetime(1970, 1, 1, tzinfo=UTC)
        return cls(((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000)
