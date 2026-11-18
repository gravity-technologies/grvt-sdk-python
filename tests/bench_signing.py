"""Order (2 legs) hash and hash+sign benchmark. Run: cd sdk/languages/python && uv run python tests/bench_signing.py"""

import timeit
import tracemalloc
from typing import Any

import eip712_reference
from eip712_reference import Reference

from grvt._generated.signing import EIP712_ORDER
from grvt._runtime.signing import PrivateKeySigner, encode, grvt_domain

KEY = "0x" + "11" * 32
SIGNER = PrivateKeySigner(KEY)
DOMAIN = grvt_domain(326)
MSG: dict[str, Any] = {
    "subAccountID": 8289849667772468,
    "isMarket": False,
    "timeInForce": 1,
    "postOnly": False,
    "reduceOnly": False,
    "legs": [
        {
            "assetID": 0x030501,
            "contractSize": 1_200_000_000,
            "limitPrice": 65_000_100_000_000,
            "isBuyingContract": True,
        },
        {
            "assetID": 0x030502,
            "contractSize": 3_000_000_000,
            "limitPrice": 3_100_000_000_000,
            "isBuyingContract": False,
        },
    ],
    "nonce": 1234567,
    "expiration": 1_730_800_000_000_000_000,
}


REF = Reference.of(EIP712_ORDER)


def ref_hash() -> bytes:
    return REF.digest(DOMAIN, MSG)


def old_keccak() -> None:
    """The SDK before generated hashers: generic encoder + keccak.new per hash."""
    eip712_reference.keccak256 = encode._public_keccak256  # pyright: ignore[reportPrivateUsage]


def ref_hash_and_sign() -> object:
    return SIGNER.sign_digest(REF.digest(DOMAIN, MSG))


def hash_only() -> bytes:
    return EIP712_ORDER.digest(DOMAIN, MSG)


def hash_and_sign() -> object:
    return SIGNER.sign_digest(EIP712_ORDER.digest(DOMAIN, MSG))


def sign_typed() -> object:
    coro = SIGNER.sign_typed(DOMAIN, EIP712_ORDER, MSG)
    try:
        coro.send(None)
    except StopIteration as e:
        return e.value
    raise AssertionError("sign_typed awaited something")


def measure(name: str, fn: Any, n: int = 20000) -> None:
    fn()
    best = min(timeit.repeat(fn, number=n, repeat=7)) / n * 1e6
    tracemalloc.start()
    fn()
    tracemalloc.reset_peak()
    base = tracemalloc.get_traced_memory()[0]
    fn()
    peak = tracemalloc.get_traced_memory()[1] - base
    tracemalloc.stop()
    print(f"{name:12s} {best:7.2f} us/op   peak {peak} B/op (tracemalloc)")


if __name__ == "__main__":
    fast = eip712_reference.keccak256
    old_keccak()
    measure("old hash", ref_hash)
    measure("old h+sign", ref_hash_and_sign)
    eip712_reference.keccak256 = fast
    measure("hash", hash_only)
    measure("hash+sign", hash_and_sign)
    measure("sign_typed", sign_typed)
