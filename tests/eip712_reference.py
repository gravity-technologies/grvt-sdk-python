"""The old generic EIP-712 encoder, kept only as a test reference for the generated hashers.

It interprets the type definitions at runtime (one encoder closure per field), exactly as the SDK did before
the hashers were generated.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from grvt._runtime.signing import Domain, StructType, keccak256

Encoder = Callable[[Any], bytes]
_ARRAY = re.compile(r"^(.*)\[(\d*)\]$")


def _uint(bits: int) -> Encoder:
    hi = 1 << bits

    def enc(v: Any) -> bytes:
        n = v if type(v) is int else int(v)
        if n < 0 or n >= hi:
            raise ValueError(f"value {n} out of range for uint{bits}")
        return n.to_bytes(32, "big")

    return enc


def _int(bits: int) -> Encoder:
    lo, hi = -(1 << (bits - 1)), 1 << (bits - 1)

    def enc(v: Any) -> bytes:
        n = v if type(v) is int else int(v)
        if n < lo or n >= hi:
            raise ValueError(f"value {n} out of range for int{bits}")
        return (n % (1 << 256)).to_bytes(32, "big")

    return enc


_TRUE = (1).to_bytes(32, "big")
_FALSE = bytes(32)


def _bool(v: Any) -> bytes:
    return _TRUE if v else _FALSE


def _to_bytes(v: Any) -> bytes:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return bytes.fromhex(v[2:] if v.startswith("0x") else v)
    raise TypeError(f"expected bytes or hex string, got {type(v).__name__}")


def _address(v: Any) -> bytes:
    b = v.to_bytes(20, "big") if isinstance(v, int) else _to_bytes(v)
    if len(b) != 20:
        raise ValueError("address must be 20 bytes")
    return bytes(12) + b


def _fixed_bytes(size: int) -> Encoder:
    def enc(v: Any) -> bytes:
        b = _to_bytes(v)
        if len(b) != size:
            raise ValueError(f"expected {size} bytes, got {len(b)}")
        return b + bytes(32 - size)

    return enc


def _string(v: Any) -> bytes:
    return keccak256(v.encode())


def _dyn_bytes(v: Any) -> bytes:
    return keccak256(_to_bytes(v))


class Reference:
    """Generic hashStruct for a set of EIP-712 struct types."""

    def __init__(self, name: str, types: Mapping[str, Sequence[Mapping[str, str]]]):
        self.name = name
        self.types = {k: [dict(f) for f in v] for k, v in types.items()}
        self._hashers: dict[str, Callable[[Mapping[str, Any]], bytes]] = {}
        for struct in self.types:
            self._compile(struct)

    @classmethod
    def of(cls, st: StructType) -> Reference:
        return cls(st.name, st.types)

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
            base = _ARRAY.sub(r"\1", f["type"])
            while _ARRAY.match(base):
                base = _ARRAY.sub(r"\1", base)
            if base in self.types:
                self._deps(base, seen)
        return seen

    def _encoder(self, typ: str) -> Encoder:
        m = _ARRAY.match(typ)
        if m:
            inner = self._encoder(m.group(1))
            return lambda vs: keccak256(b"".join(inner(v) for v in vs))
        if typ in self.types:
            return lambda v, s=typ: self._hashers[s](v)
        if typ == "bool":
            return _bool
        if typ == "address":
            return _address
        if typ == "string":
            return _string
        if typ == "bytes":
            return _dyn_bytes
        if typ.startswith("uint"):
            return _uint(int(typ[4:] or 256))
        if typ.startswith("int"):
            return _int(int(typ[3:] or 256))
        if typ.startswith("bytes"):
            return _fixed_bytes(int(typ[5:]))
        raise ValueError(f"unsupported EIP-712 type {typ!r}")

    def _compile(self, struct: str) -> None:
        th = keccak256(self.encode_type(struct).encode())
        fields = [(f["name"], self._encoder(f["type"])) for f in self.types[struct]]

        def hash_struct(msg: Mapping[str, Any]) -> bytes:
            return keccak256(th + b"".join(enc(msg[name]) for name, enc in fields))

        self._hashers[struct] = hash_struct

    def hash_struct(self, message: Mapping[str, Any]) -> bytes:
        return self._hashers[self.name](message)

    def digest(self, domain: Domain, message: Mapping[str, Any]) -> bytes:
        return keccak256(b"\x19\x01" + domain.separator() + self.hash_struct(message))

    def as_struct_type(self) -> StructType:
        """A StructType hashed by this reference (for tests of hand-written types)."""
        type_hash = "0x" + keccak256(self.encode_type(self.name).encode()).hex()
        return StructType(self.name, self.types, type_hash, hasher=self.hash_struct)
