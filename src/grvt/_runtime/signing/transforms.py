"""The closed set of wire-value -> EIP-712-value transforms the generated sign functions use.

Matches the contract's `xf` names: copy (no helper), enumCode (generated dict lookup), toAddress,
assetKey (instrument hash from metadata), scaled. Over-precision is rejected, never rounded.
"""

from __future__ import annotations

from decimal import Context, Decimal, Inexact, InvalidOperation, Overflow
from functools import lru_cache
from typing import Any

from ..errors import SigningError


def json_object(value: Any) -> dict[str, Any] | None:
    """value as a JSON object, or None when it is not one."""
    return value if isinstance(value, dict) else None  # pyright: ignore[reportUnknownVariableType]


def json_list(value: Any) -> list[Any]:
    """value as a JSON array; [] when it is absent or not an array."""
    return value if isinstance(value, list) else []  # pyright: ignore[reportUnknownVariableType]


def scaled(value: Any, decimals: int) -> int:
    """Decimal string/Decimal/int -> integer units. "65000.1", 9 -> 65000100000000."""
    if type(value) is str:
        return _scaled_text(value, decimals)
    return _scaled(value, decimals)


# Exact arithmetic whatever the caller's decimal context says: a signed price is never rounded to the thread's
# precision, and every path (fast or Decimal) gives the same answer.
_EXACT = Context(prec=200, Emax=10_000, Emin=-10_000, traps=[InvalidOperation, Inexact, Overflow])


def _scaled(value: Any, decimals: int) -> int:
    if type(value) is str and 0 <= decimals <= 77:
        # Fast path for plain "123" / "-123.45": the same result as the exact Decimal path below, without
        # building Decimals. Anything else ("+1", "1e3", spaces, more than 28 digits, a huge decimals) takes
        # the Decimal path.
        whole, _, frac = value.partition(".")
        digits = whole[1:] if whole.startswith("-") else whole
        if (
            digits
            and len(digits) + len(frac) <= 28
            and digits.isascii()
            and digits.isdigit()
            and (not frac or (frac.isascii() and frac.isdigit()))
        ):
            if len(frac) > decimals:
                if frac[decimals:].strip("0"):
                    raise SigningError(f"{value} has more than {decimals} decimal places")
                frac = frac[:decimals]
            return int(whole + frac + "0" * (decimals - len(frac)))
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation:
        raise SigningError(f"not a decimal: {value!r}") from None
    try:
        n = d.scaleb(decimals, context=_EXACT)
    except (Inexact, Overflow):
        raise SigningError(f"cannot scale {value!r} by 10^{decimals} exactly") from None
    if n != n.to_integral_value(context=_EXACT):
        raise SigningError(f"{value} has more than {decimals} decimal places")
    return int(n)


# Signing scales the same few strings again and again (instrument limits, the prices and sizes being quoted).
# Only str inputs are cached: equal strings always give the same result, and errors are never cached.
_scaled_text = lru_cache(maxsize=4096)(_scaled)


def to_int(value: Any) -> int:
    """Quoted 64-bit wire ints ("123") and hex ("0x030501") -> int."""
    if isinstance(value, int):
        return value
    s = str(value)
    return int(s, 16) if s.startswith(("0x", "0X")) else int(s)


def to_address(value: Any) -> str:
    s = str(value).lower()
    if not s.startswith("0x"):
        s = "0x" + s
    if len(s) != 42:
        raise SigningError(f"not an address: {value!r}")
    return s
