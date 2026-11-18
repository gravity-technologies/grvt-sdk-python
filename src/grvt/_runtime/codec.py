"""Wire conversions for exact Decimal, Timestamp and generated enum/model types.

The gateway carries decimals, timestamps and wide integers as JSON strings.
"""

from __future__ import annotations

from typing import Any

import msgspec

from .scalars import Timestamp


def _encode_scalar(value: Any) -> Any:
    if isinstance(value, Timestamp):
        return str(value)
    raise TypeError(f"unsupported value type: {type(value).__name__}")


def _decode_scalar(type: Any, value: Any) -> Any:
    if type is Timestamp:
        if not isinstance(value, str):
            raise TypeError("timestamp must be a string of Unix nanoseconds")
        if not value or not value.removeprefix("-").isascii() or not value.removeprefix("-").isdigit():
            raise ValueError("timestamp must be a string of Unix nanoseconds")
        return Timestamp(int(value))
    raise TypeError(f"unsupported value type: {type}")


_encoder = msgspec.json.Encoder(decimal_format="string", enc_hook=_encode_scalar)
_decoders: dict[Any, msgspec.json.Decoder[Any]] = {}


def to_builtins(obj: Any) -> Any:
    return msgspec.to_builtins(obj, enc_hook=_encode_scalar)


def convert(obj: Any, type: Any) -> Any:
    return msgspec.convert(obj, type, strict=False, dec_hook=_decode_scalar)


def encode(obj: Any) -> bytes:
    return _encoder.encode(obj)


def decode(data: bytes, type: Any = Any) -> Any:
    dec = _decoders.get(type)
    if dec is None:
        # strict=False: accept "123" for int fields, the gateway quotes some ints.
        dec = _decoders[type] = msgspec.json.Decoder(type, strict=False, dec_hook=_decode_scalar)
    return dec.decode(data)


class Frame(msgspec.Struct, omit_defaults=True):
    """The parts of an incoming WS frame the read loop needs. `result`/`feed` stay raw."""

    id: int | None = None
    method: str | None = None
    result: msgspec.Raw = None  # type: ignore[assignment]  (Raw cannot be in a union)
    error: dict[str, Any] | None = None
    stream: str | None = None
    selector: str | None = None
    sequence_number: int | None = None
    prev_sequence_number: int | None = None
    # REST-style error body ({"code","message","status"}) sometimes seen on WS too
    code: int | None = None
    message: str | None = None


_frame_decoder = msgspec.json.Decoder(Frame, strict=False)


def decode_frame(data: bytes) -> Frame:
    return _frame_decoder.decode(data)


class SubscribeResult(msgspec.Struct):
    stream: str = ""
    subs: list[str] = []
    unsubs: list[str] = []
    num_snapshots: list[int] = []
    first_sequence_number: list[int] = []
    latest_sequence_number: list[int] = []
