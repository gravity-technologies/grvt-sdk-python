"""Generated EIP-712 hashers == the generic reference encoder, on random and edge-case messages."""

import random
from typing import Any

import pytest
from eip712_reference import Reference

from grvt._generated import signing
from grvt._runtime.signing import StructType, grvt_domain

TYPES = {name: st for name, st in vars(signing).items() if name.startswith("EIP712_") and isinstance(st, StructType)}
DOMAINS = [grvt_domain(1), grvt_domain(325), grvt_domain(326)]


def edges(typ: str) -> list[Any]:
    if typ == "bool":
        return [False, True]
    if typ == "address":
        return ["0x" + "00" * 20, "0x" + "ff" * 20]
    if typ.startswith("uint"):
        bits = int(typ[4:] or 256)
        return [0, 1, (1 << bits) - 1]
    if typ.startswith("int"):
        bits = int(typ[3:] or 256)
        return [0, -1, 1, -(1 << (bits - 1)), (1 << (bits - 1)) - 1]
    raise AssertionError(typ)


def value(rng: random.Random, typ: str, types: dict[str, list[dict[str, str]]]) -> Any:
    if typ.endswith("[]"):
        return [value(rng, typ[:-2], types) for _ in range(rng.choice([0, 1, 2, 2, 5]))]
    if typ in types:
        return {f["name"]: value(rng, f["type"], types) for f in types[typ]}
    if rng.random() < 0.3:
        return rng.choice(edges(typ))
    if typ == "bool":
        return rng.random() < 0.5
    if typ == "address":
        return "0x" + rng.randbytes(20).hex()
    if typ.startswith("uint"):
        return rng.getrandbits(int(typ[4:] or 256))
    bits = int(typ[3:] or 256)
    return rng.getrandbits(bits) - (1 << (bits - 1))


def outcome(fn: Any, *args: Any) -> Any:
    try:
        return fn(*args)
    except Exception as e:  # noqa: BLE001 - comparing the exact error both encoders raise
        return (type(e), str(e))


@pytest.mark.parametrize("name", sorted(TYPES))
def test_random_messages_match_reference(name: str) -> None:
    st = TYPES[name]
    ref = Reference.of(st)
    rng = random.Random(name)
    for _ in range(500):
        msg = value(rng, st.name, st.types)
        domain = rng.choice(DOMAINS)
        assert st.hash_struct(msg) == ref.hash_struct(msg)
        assert st.digest(domain, msg) == ref.digest(domain, msg)


def bad_values(typ: str) -> list[Any]:
    if typ.startswith("uint"):
        bits = int(typ[4:] or 256)
        return [-1, 1 << bits, "x", None]
    if typ.startswith("int"):
        bits = int(typ[3:] or 256)
        return [-(1 << (bits - 1)) - 1, 1 << (bits - 1), "x", None]
    if typ == "address":
        return ["0x1234", "0x" + "zz" * 20, 1 << 160, None]
    return []


def fields(st: StructType) -> list[tuple[list[str | int], str]]:
    """(path into the message, type) for every leaf field; arrays point at item 0."""
    out: list[tuple[list[str | int], str]] = []

    def walk(struct: str, prefix: list[str | int]) -> None:
        for f in st.types[struct]:
            base = f["type"].removesuffix("[]")
            path: list[str | int] = [*prefix, f["name"]] + ([0] if f["type"].endswith("[]") else [])
            if base in st.types:
                walk(base, path)
            else:
                out.append((path, f["type"]))

    walk(st.name, [])
    return out


@pytest.mark.parametrize("name", sorted(TYPES))
def test_bad_values_raise_like_reference(name: str) -> None:
    st = TYPES[name]
    ref = Reference.of(st)
    rng = random.Random(name)
    checked = 0
    for path, typ in fields(st):
        for bad in bad_values(typ):
            msg = value(rng, st.name, st.types)
            obj: Any = msg
            try:
                for key in path[:-1]:
                    obj = obj[key]
            except IndexError:  # the random array came out empty
                continue
            obj[path[-1]] = bad
            got, want = outcome(st.hash_struct, msg), outcome(ref.hash_struct, msg)
            assert isinstance(want, tuple), (path, bad)
            assert got == want, (path, bad)
            checked += 1
    assert checked > 0


def test_wire_style_values_match_reference() -> None:
    """Values the reference accepted through int(): bools for ints, numeric strings."""
    st = signing.EIP712_ORDER
    ref = Reference.of(st)
    msg = {
        "subAccountID": "8289849667772468",
        "isMarket": 0,
        "timeInForce": True,
        "postOnly": 1,
        "reduceOnly": "",
        "legs": [{"assetID": "197889", "contractSize": 1.0, "limitPrice": True, "isBuyingContract": "yes"}],
        "nonce": "7",
        "expiration": "-5",
    }
    assert st.hash_struct(msg) == ref.hash_struct(msg)
