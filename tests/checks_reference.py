"""The order checks and decimal scaling as they were before the hot-path rewrite, kept only as a test reference.

tests/test_hot_path.py runs random orders and values through both and expects the same result or error.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from grvt._runtime.errors import OrderOutOfBounds
from grvt._runtime.signing.checks import OrderChecks, OrderRule
from grvt._runtime.signing.transforms import json_list, json_object
from grvt._runtime.signing.wire import Meta


def scaled(value: Any, decimals: int) -> int:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"not a decimal: {value!r}") from None
    n = d.scaleb(decimals)
    if n != n.to_integral_value():
        raise ValueError(f"{value} has more than {decimals} decimal places")
    return int(n)


_U64_MAX = 2**64 - 1


def _dig(obj: Any, path: str) -> Any:
    for key in path.split("."):
        d = json_object(obj)
        if d is None:
            return None
        obj = d.get(key)
    return obj


def _fmt(n: int, d: int) -> str:
    """n / 10^d without trailing zeros: (60000100000000, 9) -> "60000.1"."""
    if d == 0:
        return str(n)
    s = str(n).rjust(d + 1, "0")
    whole, frac = s[:-d], s[-d:].rstrip("0")
    return f"{whole}.{frac}" if frac else whole


def _ceil_div(a: int, b: int) -> int:
    return -(-a // b)


def check_order(checks: OrderChecks, order: Any, meta: Meta) -> None:
    """Raise OrderOutOfBounds for the first leg value outside its instrument's limits (ValueError when malformed)."""
    order_obj = json_object(order)
    if order_obj is None:
        return
    for index, leg in enumerate(json_list(order_obj.get(checks["legs"]))):
        _check_leg(checks, order_obj, index, leg, meta)


def _check_leg(checks: OrderChecks, order: dict[str, Any], index: int, leg: Any, meta: Meta) -> None:
    """Raise OrderOutOfBounds for the first value of this leg outside its instrument's limits."""
    rules = checks["rules"]
    name = leg.get(checks["instrument"])
    inst = meta.instrument(name)

    def decimals(field: str) -> int:
        s = checks["scales"][field]
        return s if isinstance(s, int) else int(inst[s])

    def value(field: str) -> int | None:
        raw = leg.get(field)
        if raw in (None, ""):
            return None
        try:
            n = scaled(raw, decimals(field))
        except ValueError as e:
            raise ValueError(f"OrderLeg.{field}: {e}") from None
        if n < 0 or n > _U64_MAX:
            raise ValueError(f"OrderLeg.{field}: {raw} is out of range")
        return n

    def limit(field: str, d: int) -> int:
        raw = inst.get(field)
        if raw in (None, ""):
            return 0
        try:
            return scaled(raw, d)
        except ValueError as e:
            raise ValueError(f"instrument {name} {field}: {e}") from None

    def skipped(rule: OrderRule) -> bool:
        return any(_dig(order, p) is True for p in rule.get("skip_if", ()))

    for field in checks["scales"]:  # a malformed value is reported as such, whatever the rules
        value(field)
    for rule in rules:
        if skipped(rule):
            continue
        v = value(rule["field"])
        if v is None:
            continue
        d, label, times = decimals(rule["field"]), rule["field"], rule.get("times")
        if times:
            t = value(times)
            if t is None or v == 0 or t == 0:
                continue
            v, d, label = v * t, d + decimals(times), f"{rule['field']} * {times}"
        lim = limit(rule["limit"], d)
        if lim == 0:
            continue
        nearest: list[int]
        if rule["check"] == "at_least":
            if v >= lim:
                continue
            if not times:
                nearest, nd = [lim], d
                msg = f"{label} {_fmt(v, d)} is below {rule['limit']} {_fmt(lim, d)}"
            else:
                nd = decimals(times)
                x = _ceil_div(lim, value(rule["field"]))  # type: ignore[arg-type]
                for r2 in rules:
                    if r2["field"] != times or r2.get("times") or skipped(r2):
                        continue
                    l2 = limit(r2["limit"], nd)
                    if l2 == 0:
                        continue
                    if r2["check"] == "at_least":
                        x = max(x, l2)
                    else:
                        x = _ceil_div(x, l2) * l2
                nearest = [x]
                msg = (
                    f"{label} = {_fmt(v, d)} is below {rule['limit']} {_fmt(lim, d)} "
                    f"(smallest {times} at this {rule['field']}: {_fmt(x, nd)})"
                )
        else:
            if v % lim == 0:
                continue
            lo = v - v % lim
            nearest, nd = [n for n in (lo, lo + lim) if n > 0], d
            msg = (
                f"{label} {_fmt(v, d)} isn't a multiple of {rule['limit']} {_fmt(lim, d)} "
                f"(nearest: {' or '.join(_fmt(n, d) for n in nearest)})"
            )
        raise OrderOutOfBounds(
            f"{name}: {msg}",
            instrument=name,
            leg=index,
            field=label,
            check=rule["check"],
            limit=rule["limit"],
            value=_fmt(v, d),
            limit_value=_fmt(lim, d),
            nearest=[_fmt(n, nd) for n in nearest],
            server_code=rule["server_code"],
        )
