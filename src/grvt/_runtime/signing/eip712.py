"""EIP-712 hashing.

The generator emits one StructType per signed message: the type definitions (eth_signTypedData format, for
signers that need the full typed data) plus a generated hasher that packs the message into one fixed-size
buffer and hashes it (see encode.py). Type hashes are checked at import; domain separators are computed once
per Domain.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from .encode import address, keccak256, uint

Hasher = Callable[[Mapping[str, Any]], bytes]

_DOMAIN_FIELDS = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
]
_DOMAIN_TYPE = keccak256(b"EIP712Domain(string name,string version,uint256 chainId)")
_DOMAIN_TYPE_VC = keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")


@dataclass(frozen=True, slots=True)
class Domain:
    name: str
    version: str
    chain_id: int
    verifying_contract: str | None = None
    _separator: bytes = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_separator", _domain_separator(self))

    def to_json(self) -> dict[str, Any]:
        d: dict[str, Any] = {"name": self.name, "version": self.version, "chainId": self.chain_id}
        if self.verifying_contract is not None:
            d["verifyingContract"] = self.verifying_contract
        return d

    def types(self) -> list[dict[str, str]]:
        t = [dict(f) for f in _DOMAIN_FIELDS]
        if self.verifying_contract is not None:
            t.append({"name": "verifyingContract", "type": "address"})
        return t

    def separator(self) -> bytes:
        return self._separator


def _domain_separator(d: Domain) -> bytes:
    chain = uint(d.chain_id, 256).to_bytes(32, "big")
    head = _DOMAIN_TYPE if d.verifying_contract is None else _DOMAIN_TYPE_VC
    data = head + keccak256(d.name.encode()) + keccak256(d.version.encode()) + chain
    if d.verifying_contract is not None:
        data += bytes(12) + address(d.verifying_contract)
    return keccak256(data)


# Grvt's domain; chain id comes from the Environment.
@lru_cache(maxsize=8)
def grvt_domain(chain_id: int) -> Domain:
    return Domain("GRVT Exchange", "0", chain_id)


class StructType:
    """One generated EIP-712 primary type with all the struct types it references. Internal: the SDK signs only
    Grvt's own messages, so every StructType comes from grvt._generated.signing with its generated hasher.

    types: {"Order": [{"name": "subAccountID", "type": "uint64"}, ...], "OrderLeg": [...]}
    type_hash: the generator's expected keccak(encodeType); checked at import so a template bug
    fails loudly instead of producing signatures the server rejects.
    hasher: the generated hashStruct for this type (see grvt._generated.signing).
    """

    __slots__ = ("name", "types", "type_hash", "_hasher")

    def __init__(
        self,
        name: str,
        types: Mapping[str, Sequence[Mapping[str, str]]],
        type_hash: str,
        *,
        hasher: Hasher,
    ):
        self.name = name
        self.types = {k: [dict(f) for f in v] for k, v in types.items()}
        computed = "0x" + keccak256(self.encode_type(name).encode()).hex()
        if type_hash.lower() != computed:
            raise ValueError(f"{name}: type hash mismatch, generated {type_hash} vs computed {computed}")
        self.type_hash = computed
        self._hasher = hasher

    def encode_type(self, struct: str) -> str:
        deps = sorted(self._deps(struct, set()) - {struct})
        return "".join(self._one_type(t) for t in [struct, *deps])

    def _one_type(self, struct: str) -> str:
        return struct + "(" + ",".join(f"{f['type']} {f['name']}" for f in self.types[struct]) + ")"

    def _deps(self, struct: str, seen: set[str]) -> set[str]:
        if struct in seen:
            return seen
        seen.add(struct)
        for f in self.types[struct]:
            base = f["type"].split("[", 1)[0]
            if base in self.types:
                self._deps(base, seen)
        return seen

    def hash_struct(self, message: Mapping[str, Any]) -> bytes:
        return self._hasher(message)

    def digest(self, domain: Domain, message: Mapping[str, Any]) -> bytes:
        return keccak256(b"\x19\x01" + domain.separator() + self._hasher(message))

    def to_typed_data(self, domain: Domain, message: Mapping[str, Any]) -> dict[str, Any]:
        """Standard eth_signTypedData_v4 JSON, for signers that need the full struct (KMS, wallets)."""
        return {
            "types": {"EIP712Domain": domain.types(), **self.types},
            "primaryType": self.name,
            "domain": domain.to_json(),
            "message": dict(message),
        }
