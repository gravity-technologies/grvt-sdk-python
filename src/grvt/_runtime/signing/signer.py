"""Signer plug-in.

A signer gets the exact EIP-712 input (domain, primary type with its type definitions, message) and
returns r/s/v. It needs no Grvt knowledge, so a KMS/HSM/wallet signer can call
`primary_type.to_typed_data(domain, message)` and hand that to its backend.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import coincurve

from .eip712 import Domain, StructType
from .encode import keccak256


def recovered_address(digest: bytes, sig: Signature) -> str | None:
    """The address that produced sig over digest, or None when sig is malformed."""
    try:
        rs = bytes.fromhex(sig.r.removeprefix("0x")) + bytes.fromhex(sig.s.removeprefix("0x"))
        if len(rs) != 64 or sig.v not in (27, 28):
            return None
        pub = coincurve.PublicKey.from_signature_and_message(rs + bytes([sig.v - 27]), digest, hasher=None)
    except (ValueError, TypeError, AttributeError):
        return None
    return "0x" + keccak256(pub.format(compressed=False)[1:])[-20:].hex()


_HALF_ORDER = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0


def check_signature(digest: bytes, sig: Signature, address: str) -> str | None:
    """Why a wallet/KMS signature is not valid for digest and address, or None when it is.

    The same rule in every SDK: 32-byte r and s, v 27 or 28, low-s, recovers to address, and
    sig.signer (when set) is address.
    """
    try:
        r, s = bytes.fromhex(sig.r.removeprefix("0x")), bytes.fromhex(sig.s.removeprefix("0x"))
    except (ValueError, TypeError, AttributeError):
        return "r and s must be 32 bytes of hex"
    if len(r) != 32 or len(s) != 32:
        return "r and s must be 32 bytes of hex"
    if sig.v not in (27, 28):
        return f"v must be 27 or 28, got {sig.v}"
    if int.from_bytes(s, "big") > _HALF_ORDER:
        return "s is not low-s"
    got = recovered_address(digest, sig)
    if got is None or got != address.lower():
        return f"it recovers to {got}, not to the signer address (wrong key, or a different message was signed)"
    if sig.signer and sig.signer.lower() != address.lower():
        return f"its signer field {sig.signer} is not the signer address"
    return None


@dataclass(frozen=True, slots=True)
class Signature:
    signer: str  # 0x-prefixed lowercase address
    r: str  # 0x-prefixed 32-byte hex
    s: str  # 0x-prefixed 32-byte hex, low-s (the server rejects high s, code 2007)
    v: int  # 27 or 28


class Signer(Protocol):
    @property
    def address(self) -> str: ...

    async def sign_typed(self, domain: Domain, primary_type: StructType, message: Mapping[str, Any]) -> Signature: ...


class PrivateKeySigner:
    """In-process secp256k1 key (coincurve). Also fits session keys: pass the session private key."""

    __slots__ = ("_key", "_address")

    def __init__(self, private_key: str | bytes):
        raw = bytes.fromhex(private_key.removeprefix("0x")) if isinstance(private_key, str) else private_key
        self._key = coincurve.PrivateKey(raw)
        pub = self._key.public_key.format(compressed=False)[1:]
        self._address = "0x" + keccak256(pub)[-20:].hex()

    @property
    def address(self) -> str:
        return self._address

    def __repr__(self) -> str:  # never print the key
        return f"PrivateKeySigner({self._address})"

    def sign_digest(self, digest: bytes) -> Signature:
        # r(32) | s(32) | recovery id(1), low-s. coincurve leaves custom_nonce's tuple untyped.
        sig = self._key.sign_recoverable(digest, hasher=None)  # pyright: ignore[reportUnknownMemberType]
        return Signature(self._address, "0x" + sig[:32].hex(), "0x" + sig[32:64].hex(), 27 + sig[64])

    async def sign_typed(self, domain: Domain, primary_type: StructType, message: Mapping[str, Any]) -> Signature:
        return self.sign_digest(primary_type.digest(domain, message))
