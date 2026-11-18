"""The faster signing helpers give exactly what the plain versions gave: same keccak bytes, same scaled integers,
same order-check verdicts and errors (checks_reference.py holds the plain versions)."""

import os
import random
from decimal import Decimal
from typing import Any

import checks_reference as ref
import pytest

from grvt._generated.signing import ORDER_CHECKS
from grvt._runtime import Runtime
from grvt._runtime.signing import checks, encode, transforms
from grvt._runtime.signing.wire import Meta


@pytest.mark.parametrize("backend", ["openssl", "ctypes"])
def test_keccak_backends_match_pycryptodome(backend):
    make = {"openssl": encode._openssl_keccak256, "ctypes": encode._raw_keccak256}[backend]  # pyright: ignore[reportPrivateUsage]
    k = make()
    if k is None:
        pytest.skip(f"{backend} keccak not available here")
    rng = random.Random(1)
    for n in [0, 1, 31, 32, 33, 135, 136, 137, 271, 272, 1000, *(rng.randrange(2000) for _ in range(200))]:
        data = os.urandom(n)
        assert k(data) == encode._public_keccak256(data)  # pyright: ignore[reportPrivateUsage]


def _outcome(fn: Any, *args: Any) -> Any:
    try:
        return ("ok", fn(*args))
    except Exception as e:  # noqa: BLE001 - the error itself is what is compared
        return (type(e), str(e), getattr(e, "__dict__", {}))


SCALED_CASES = [
    "0", "-0", "0.0", "1", "-1", "1.", ".5", "-.5", "0.1", "65000.1", "65000.10000", "1.2000000000000",
    "0.000000001", "0.0000000001", "0.00000000010", "-0.0000000001", "007.50", "1e3", "1E-9", "1.5e-10",
    "+1", " 1", "1 ", "1_000", "١٢", "²", "", "-", ".", "abc", "NaN", "-nan", "sNaN", "1.2.3", "--1", "0x10",
    "9" * 20, "9" * 28, "9" * 29, "1" + "0" * 27 + ".5", "0." + "0" * 30 + "1", "1234567890123456789.123456789",
    "18446744073709551615", "18446744073709551616", "123456789012345678901234567.8",
    1, 0, -5, True, None, 1.5, 0.1, Decimal("1.25"), Decimal("1E+3"), Decimal("NaN"),
]  # fmt: skip


def _exact_ref(value: Any, decimals: int) -> Any:
    # The old Decimal code, run with enough precision that it never rounds: the new code is exact on purpose
    # (the old one rounded values longer than the caller's precision, 28 digits by default).
    import decimal

    with decimal.localcontext() as ctx:
        ctx.prec = 200
        return _outcome(ref.scaled, value, decimals)


@pytest.mark.parametrize("decimals", [0, 1, 2, 6, 9, 18, 30])
def test_scaled_matches_decimal(decimals):
    rng = random.Random(decimals)
    cases: list[Any] = list(SCALED_CASES)
    for _ in range(500):
        whole = str(rng.randrange(10 ** rng.randrange(1, 22)))
        frac = "".join(rng.choice("0123456789") for _ in range(rng.randrange(0, 22)))
        sign = rng.choice(["", "", "-"])
        cases.append(sign + whole + ("." + frac if frac or rng.random() < 0.1 else ""))
    for value in cases:
        for _ in range(2):  # the second call is served from the cache
            assert _outcome(transforms.scaled, value, decimals) == _exact_ref(value, decimals), value


def _instrument(name: str, rng: random.Random) -> dict[str, Any]:
    return {
        "instrument": name,
        "base_decimals": rng.choice([3, 6, 9, "9", "x"] if rng.random() < 0.05 else [3, 6, 9]),
        "tick_size": rng.choice(
            ["0.1", "0.5", "0.01", "1", "", None, "0.0000000001", "abc"]
            if rng.random() < 0.1
            else ["0.1", "0.5", "0.01", "1"]
        ),
        "min_size": rng.choice(["0.001", "0.01", "1", "0", "", None]),
        "min_notional": rng.choice(["0", "10", "100", "1000", "0.5", "", None]),
    }


def _value(rng: random.Random) -> Any:
    if rng.random() < 0.1:  # malformed, missing or out of range
        return rng.choice(["-1", "", None, "1e2", "x", "18446744073709551616", f"{rng.random():.12f}"])
    return rng.choice(
        [
            f"{rng.randrange(0, 200000) / 10:.1f}",
            f"{rng.randrange(0, 200000) / 100:.2f}",
            f"{rng.randrange(0, 5000) / 1000:.3f}",
            f"{rng.randrange(0, 50) / 1000:.3f}",
            str(rng.randrange(0, 1000)),
        ]
    )


def _order(rng: random.Random, names: list[str]) -> dict[str, Any]:
    order: dict[str, Any] = {
        "legs": [
            {
                "instrument": rng.choice(names),
                "size": _value(rng),
                "limit_price": _value(rng),
                "is_buying_asset": rng.random() < 0.5,
            }
            for _ in range(rng.randrange(0, 4))
        ]
    }
    for flag in ("is_market", "reduce_only"):
        if rng.random() < 0.3:
            order[flag] = rng.choice([True, False, "true", None])
    if rng.random() < 0.2:
        order["metadata"] = {"trigger": {"tpsl": {"close_position": rng.choice([True, False])}}}
    return order


async def test_check_order_matches_reference():
    rng = random.Random(7)
    rt = Runtime("testnet")
    try:
        names = ["A", "B", "C"]
        for _ in range(3000):
            for name in names:
                rt.metadata.put("instrument", name, _instrument(name, rng))
            order = _order(rng, names)
            meta = Meta(rt.metadata)
            got = _outcome(checks.check_order, ORDER_CHECKS, order, meta)
            want = _outcome(ref.check_order, ORDER_CHECKS, order, meta)
            assert got == want, order
    finally:
        await rt.close()


def test_scaled_ignores_the_callers_decimal_context():
    # A signed price is never rounded to the thread's precision, and the fast and Decimal paths agree.
    import decimal

    long_value = "1" + "0" * 40 + ".123456789"
    with decimal.localcontext() as ctx:
        ctx.prec = 10
        ctx.traps[decimal.Inexact] = True
        assert transforms.scaled("65000.123456789", 9) == 65000123456789
        assert transforms.scaled(Decimal("65000.123456789"), 9) == 65000123456789
        assert transforms.scaled(long_value, 9) == int("1" + "0" * 40 + "123456789")
    with pytest.raises(ValueError):
        transforms.scaled("1.5", 0)
