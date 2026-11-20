"""Order checks: instrument limits (tick, minimum size, size step, minimum notional) checked before signing.

The rules come from the contract (`order_checks`) through the generated ORDER_CHECKS table; this module
only applies them. Values are compared as exact integers, scaled like the signed fields. Nothing is ever
rounded or changed: a failing order raises OrderOutOfBounds, with the nearest valid values.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from ..errors import OrderOutOfBounds, SigningError
from .transforms import json_list, json_object, scaled

if TYPE_CHECKING:
    from .wire import Meta

_U64_MAX = 2**64 - 1


class OrderRule(TypedDict):
    """One limit check. `times` multiplies the field by another leg field (notional = price * size)."""

    field: str
    times: NotRequired[str]
    check: str  # "at_least" | "multiple_of"
    limit: str  # instrument field holding the limit
    skip_if: tuple[str, ...]  # order fields (dotted) that switch the rule off when true
    server_code: int


class OrderChecks(TypedDict):
    """The generated ORDER_CHECKS table (contract order_checks)."""

    legs: str
    instrument: str
    scales: dict[str, int | str]  # leg field -> fixed decimals, or the instrument field holding them
    rules: list[OrderRule]


def _dig(obj: Any, path: str) -> Any:
    for key in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    return obj  # pyright: ignore[reportUnknownVariableType]


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
    """Raise OrderOutOfBounds for the first leg value outside its instrument's limits (SigningError when malformed)."""
    order_obj = json_object(order)
    if order_obj is None:
        return
    legs = json_list(order_obj.get(checks["legs"]))
    if not legs:
        return
    # skip_if reads order-level fields, so which rules apply is the same for every leg.
    is_true: dict[str, bool] = {}  # skip_if path -> its value is True
    rules: list[OrderRule] = []
    for rule in checks["rules"]:
        for path in rule.get("skip_if", ()):
            hit = is_true.get(path)
            if hit is None:
                hit = is_true[path] = _dig(order_obj, path) is True
            if hit:
                break
        else:
            rules.append(rule)
    for index, leg in enumerate(legs):
        _check_leg(checks, rules, index, leg, meta)


def _check_leg(checks: OrderChecks, rules: list[OrderRule], index: int, leg: Any, meta: Meta) -> None:
    """Raise OrderOutOfBounds for the first value of this leg outside its instrument's limits (rules: the ones
    the order does not skip). Each leg value is scaled once, up front."""
    scales = checks["scales"]
    name = leg.get(checks["instrument"])
    inst = meta.instrument(name)
    values: dict[str, int | None] = {}
    decs: dict[str, int] = {}

    def decimals(field: str) -> int:
        d = decs.get(field)
        if d is None:
            s = scales[field]
            d = decs[field] = s if isinstance(s, int) else int(inst[s])
        return d

    def value(field: str) -> int | None:
        if field in values:
            return values[field]
        raw = leg.get(field)
        if raw in (None, ""):
            values[field] = None
            return None
        try:
            n = scaled(raw, decimals(field))
        except ValueError as e:
            raise SigningError(f"OrderLeg.{field}: {e}") from None
        if n < 0 or n > _U64_MAX:
            raise SigningError(f"OrderLeg.{field}: {raw} is out of range")
        values[field] = n
        return n

    def limit(field: str, d: int) -> int:
        raw = inst.get(field)
        if raw in (None, ""):
            return 0
        try:
            return scaled(raw, d)
        except ValueError as e:
            raise SigningError(f"instrument {name} {field}: {e}") from None

    for field in scales:  # a malformed value is reported as such, whatever the rules
        value(field)
    for rule in rules:
        field = rule["field"]
        v = values[field] if field in values else value(field)
        if v is None:
            continue
        d, label, times = decimals(field), field, rule.get("times")
        if times:
            t = value(times)
            if t is None or v == 0 or t == 0:
                continue
            v, d, label = v * t, d + decimals(times), f"{field} * {times}"
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
                    if r2["field"] != times or r2.get("times"):
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


def leg_instruments(obj: Any, legs: str = "legs", instrument: str = "instrument") -> Iterator[str]:
    """Every instrument named by a leg anywhere in a request (JSON data)."""
    d = json_object(obj)
    if d is not None:
        for k, v in d.items():
            if k == legs and isinstance(v, list):
                for x in json_list(v):
                    leg = json_object(x)
                    if leg is not None and leg.get(instrument):
                        yield str(leg[instrument])
            else:
                yield from leg_instruments(v, legs, instrument)
    else:
        for v in json_list(obj):
            yield from leg_instruments(v, legs, instrument)
