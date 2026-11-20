"""Helpers the generated sign functions call. They work on the request as plain JSON data (dicts).

Each helper is one contract transform (see sdk/contract/contract.json `signed.transforms`).
Errors are SigningError (a ValueError) and always happen before anything is signed.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, overload

from ..errors import ConfigError, SigningError
from .checks import OrderChecks, OrderRule, check_order, leg_instruments
from .eip712 import StructType, grvt_domain
from .transforms import json_list, to_address, to_int
from .transforms import scaled as _scaled

if TYPE_CHECKING:
    from ..runtime import MetadataCache, Runtime

ZERO_ADDRESS = "0x" + "00" * 20
_MISSING = object()


def get(obj: Any, *path: str) -> Any:
    """Value at a dotted path, or None when any step is absent."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    return obj  # pyright: ignore[reportUnknownVariableType]


def items(obj: Any, *path: str) -> list[Any]:
    """The JSON array at a dotted path; [] when it is absent."""
    return json_list(get(obj, *path))


def copy(value: Any, eip_type: str, where: str, default: Any = _MISSING) -> Any:
    if value is None or value == "":
        if default is _MISSING:
            raise SigningError(f"{where} is required for signing")
        value = default
    if eip_type == "bool":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    if eip_type == "address":
        return to_address(value)
    if eip_type.startswith(("uint", "int")):
        return to_int(value)
    return value


def enum_code(value: Any, codes: dict[str, int], where: str) -> int:
    if value is None or value == "":
        return 0
    try:
        return codes[str(value)]
    except KeyError:
        raise SigningError(f"{where}: unknown enum value {value!r}") from None


def address(value: Any) -> str:
    return ZERO_ADDRESS if value in (None, "") else to_address(value)


def address_set(value: Any) -> bool:
    return value not in (None, "") and int(str(value), 16) != 0


def scaled(value: Any, decimals: int, where: str, default: Any = _MISSING) -> int:
    if value is None or value == "":
        if default is _MISSING:
            raise SigningError(f"{where} is required for signing")
        value = default
    try:
        return _scaled(value, decimals)
    except ValueError as e:
        raise SigningError(f"{where}: {e}") from None


class Meta:
    """Instrument, currency and account metadata, already loaded into the runtime's cache."""

    def __init__(self, cache: MetadataCache) -> None:
        self._cache = cache

    def instrument(self, name: Any) -> dict[str, Any]:
        v = self._cache.peek("instrument", str(name))
        if v is None:
            raise SigningError(f"no metadata for instrument {name!r}")
        return v

    def currency(self, symbol: Any) -> dict[str, Any]:
        v = self._cache.peek("currency", str(symbol))
        if v is None:
            raise SigningError(f"no metadata for currency {symbol!r}")
        return v

    def main_account(self) -> str:
        """The session's main account address (contract signed.main_account)."""
        v = self._cache.peek("main_account", "")
        if v is None:
            raise SigningError("no metadata for the session's main account")
        return to_address(v)


def present(values: Iterable[Any]) -> list[str]:
    return [str(v) for v in values if v not in (None, "")]


def prepare_signature(rt: Runtime, request: dict[str, Any], path: tuple[str, ...], ttl_ns: int) -> dict[str, Any]:
    """Find or create the signature object in the request and fill signer, nonce, expiration, chain_id."""
    parent = request
    for key in path[:-1]:
        parent = parent.setdefault(key, {})
    sig: dict[str, Any] = parent.get(path[-1]) or {}
    parent[path[-1]] = sig
    if rt.signer is None:
        raise ConfigError("this call needs a signer: pass signer=PrivateKeySigner(...)")
    sig.setdefault("signer", rt.signer.address)
    if sig.get("nonce") is None:
        sig["nonce"] = rt.nonces.next()
    if sig.get("expiration") in (None, "", "0"):
        sig["expiration"] = str(rt.clock.now_ns() + ttl_ns)
    if sig.get("chain_id") in (None, ""):
        sig["chain_id"] = str(rt.env.chain_id)
    return sig


async def sign_into(rt: Runtime, sig: dict[str, Any], struct: StructType, message: dict[str, Any]) -> None:
    """Sign with the domain chain id from the signature object (0 = the environment's) and write r/s/v."""
    chain_id = to_int(sig.get("chain_id") or 0) or rt.env.chain_id
    s = await rt.sign(struct, message, chain_id=chain_id)
    sig["signer"], sig["r"], sig["s"], sig["v"] = s.signer, s.r, s.s, s.v


@overload
def as_str(value: int | str) -> str: ...


@overload
def as_str(value: None) -> None: ...


def as_str(value: Any) -> str | None:
    """64-bit ints travel as JSON strings; accept an int from the caller."""
    return None if value is None else str(value)


def selector(primary: list[Any], secondary: list[Any]) -> str:
    """`<primary joined by ->[@<secondary joined by ->]`, skipping fields left as None."""
    p = "-".join(present(primary))
    s = "-".join(present(secondary))
    return f"{p}@{s}" if s else p


__all__ = [
    "get",
    "items",
    "copy",
    "enum_code",
    "address",
    "address_set",
    "scaled",
    "Meta",
    "present",
    "prepare_signature",
    "sign_into",
    "as_str",
    "selector",
    "grvt_domain",
    "ZERO_ADDRESS",
    "check_order",
    "leg_instruments",
    "OrderChecks",
    "OrderRule",
]
