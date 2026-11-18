"""Helpers for the generated EIP-712 hashers (grvt._generated.signing).

A generated hasher packs a whole struct with one precompiled struct.Struct and hashes it once. When the fast
pack fails (a value out of range or not an int), it repacks through the checked helpers below, which convert
the value like int(value) and raise the documented error ("value N out of range for uintB").
"""

from __future__ import annotations

import ctypes
import hashlib
import importlib.util
import struct
import threading
from collections.abc import Callable
from typing import Any

from Crypto.Hash import keccak as _keccak

# What a fast pack raises on a bad value; the hasher then retries with the checked helpers.
PACK_ERRORS = (struct.error, TypeError, ValueError, OverflowError, AttributeError)


def _public_keccak256(data: bytes) -> bytes:
    return _keccak.new(digest_bits=256, data=data).digest()


# Inputs a faster keccak must hash exactly like keccak.new before it is used.
_SELF_TEST = (b"", b"abc", bytes(range(256)) * 3)


def _openssl_keccak256() -> Callable[[bytes], bytes] | None:
    """keccak256 from the OpenSSL that hashlib links (KECCAK-256, OpenSSL 3.2 and later).

    Copying one prepared hash object is about twice as fast as the ctypes path below. Returns None when
    this Python's OpenSSL has no KECCAK-256 or it does not reproduce keccak.new.
    """
    try:
        proto = hashlib.new("keccak-256")
    except (ValueError, TypeError):
        return None
    copy = proto.copy

    def keccak256(data: bytes) -> bytes:
        h = copy()
        h.update(data)
        return h.digest()

    try:
        ok = all(keccak256(d) == _public_keccak256(d) for d in _SELF_TEST)
    except (ValueError, TypeError):
        return None
    return keccak256 if ok else None


def _raw_keccak256() -> Callable[[bytes], bytes] | None:
    """keccak256 on one reused state per thread, calling pycryptodome's C library directly.

    keccak.new builds a Python hash object per call, which costs more than hashing a few hundred bytes; reusing
    the C state makes each hash about 2.5x faster. Returns None (the caller then uses keccak.new) when the
    library cannot be loaded or does not reproduce keccak.new.
    """
    spec = importlib.util.find_spec("Crypto.Hash._keccak")
    if spec is None or spec.origin is None:
        return None
    try:
        lib = ctypes.CDLL(spec.origin)
        init, reset, absorb, digest = lib.keccak_init, lib.keccak_reset, lib.keccak_absorb, lib.keccak_digest
        destroy = lib.keccak_destroy
    except (OSError, AttributeError):
        return None
    destroy.argtypes = [ctypes.c_void_p]
    init.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.c_uint8]
    reset.argtypes = [ctypes.c_void_p]
    absorb.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    digest.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint8]
    for f in (init, destroy, reset, absorb, digest):
        f.restype = ctypes.c_int

    class State:
        """One thread's keccak state and output buffer; freed when the thread's locals go away."""

        __slots__ = ("st", "out")

        def __init__(self) -> None:
            self.st = ctypes.c_void_p()
            if init(ctypes.byref(self.st), 64, 24):  # capacity 2 x 32 bytes, 24 rounds: Keccak-256
                raise MemoryError("keccak_init failed")
            self.out = ctypes.create_string_buffer(32)

        def __del__(self) -> None:
            if self.st:
                destroy(self.st)

    local = threading.local()

    def keccak256(data: bytes) -> bytes:
        try:
            s: State = local.s
        except AttributeError:
            s = local.s = State()
        st, out = s.st, s.out
        # padding 0x01: original Keccak, not SHA3. These are pycryptodome internals: fail loudly if one errors.
        if reset(st) or absorb(st, data, len(data)) or digest(st, out, 32, 1):
            raise RuntimeError("pycryptodome keccak call failed")
        return out.raw

    try:
        ok = all(keccak256(d) == _public_keccak256(d) for d in _SELF_TEST)
    except (OSError, MemoryError, RuntimeError, ctypes.ArgumentError):
        return None
    return keccak256 if ok else None


keccak256: Callable[[bytes], bytes] = _openssl_keccak256() or _raw_keccak256() or _public_keccak256


def uint(value: Any, bits: int) -> int:
    n = value if type(value) is int else int(value)
    if n < 0 or n >= 1 << bits:
        raise ValueError(f"value {n} out of range for uint{bits}")
    return n


def sint(value: Any, bits: int) -> int:
    n = value if type(value) is int else int(value)
    if n < -(1 << (bits - 1)) or n >= 1 << (bits - 1):
        raise ValueError(f"value {n} out of range for int{bits}")
    return n


def uint256(value: Any) -> bytes:
    return uint(value, 256).to_bytes(32, "big")


def address(value: Any) -> bytes:
    """20 address bytes from a 0x hex string, raw bytes or an int."""
    if isinstance(value, int):
        b = value.to_bytes(20, "big")
    elif isinstance(value, str):
        b = bytes.fromhex(value[2:] if value.startswith("0x") else value)
    elif isinstance(value, (bytes, bytearray)):
        b = bytes(value)
    else:
        raise TypeError(f"expected bytes or hex string, got {type(value).__name__}")
    if len(b) != 20:
        raise ValueError("address must be 20 bytes")
    return b
