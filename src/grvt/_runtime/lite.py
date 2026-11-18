"""The lite wire encoding: the same gateway API with short JSON field names.

Lite names are unique only within one type ("s" is the stream of a frame, the size of a book level, the status of
an instrument), so each gateway type has a generated twin struct (grvt._generated.lite): the model's attributes
and types under lite JSON names. Nothing else in the SDK sees lite names: models, signing, hooks and errors stay
full, and the runtime converts only here, at the wire.

decode   lite JSON -> twin in one msgspec pass -> to_full_<Type> makes each twin its model in place (the twin has
         the model's base and slots, so setting __class__ is allowed and cheap). Unknown keys are skipped, as the
         full decoder skips them.
encode   model -> to_lite_<Type> builds the twin -> JSON. A twin has exactly the type's fields, so no other key can
         reach the wire; a dict request is checked first, since the lite server would ignore a key it does not know
         and read the field as empty.
Frame    the read loop's envelope struct with lite keys; it decodes in one msgspec pass, like codec.Frame.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import msgspec

from .._generated import lite as gen
from . import codec

_Decoder = Callable[[bytes], Any]
_scalar = codec._decode_scalar  # pyright: ignore[reportPrivateUsage]  (the same scalar rules as full)


_decoders: dict[Any, tuple[_Decoder, Callable[[Any], Any]]] = {}


def _entry(type: Any) -> tuple[_Decoder, Callable[[Any], Any]]:
    entry = _decoders.get(type)
    if entry is None:
        try:
            twin, to_full, _ = gen.TYPES[type]
        except KeyError:
            raise TypeError(f"lite encoding: {getattr(type, '__name__', type)} is not a gateway type") from None
        decoder = msgspec.json.Decoder(twin, strict=False, dec_hook=_scalar)
        entry = _decoders[type] = (decoder.decode, to_full)
    return entry


def decode(data: bytes, type: Any) -> Any:
    """Decode lite JSON into `type`, a generated gateway model."""
    dec, to_full = _entry(type)
    obj = dec(data)
    to_full(obj)  # twin -> model in place (grvt._generated.lite)
    return obj


def encode(type: Any, obj: Any) -> bytes:
    """Lite JSON for `obj`, a model of `type` (or any object with its attributes) or a full-name dict."""
    try:
        _, _, to_lite = gen.TYPES[type]
    except KeyError:
        raise TypeError(f"lite encoding: {getattr(type, '__name__', type)} is not a gateway type") from None
    if isinstance(obj, dict):
        obj = _from_dict(type, cast(dict[str, Any], obj))
    try:
        return codec.encode(to_lite(obj))
    except AttributeError as e:
        raise ValueError(f"lite encoding: {type.__name__} request: {e}") from e


def _from_dict(type: Any, obj: dict[str, Any]) -> Any:
    """The model of a full-name dict, refusing any key the model would drop (a lite server would ignore it)."""
    model = codec.convert(obj, type)
    unknown = _dropped(obj, codec.to_builtins(model), "")
    if unknown:
        raise ValueError(f"lite encoding: {type.__name__} has no field {unknown}, so the server would ignore it")
    return model


def _dropped(sent: Any, kept: Any, path: str) -> str | None:
    """The first key of `sent` that converting to the model dropped; None-valued keys may be omitted."""
    if isinstance(sent, dict) and isinstance(kept, dict):
        s, k = cast(dict[str, Any], sent), cast(dict[str, Any], kept)
        for key, value in s.items():
            if key not in k:
                if value is not None:
                    return repr(path + key)
                continue
            found = _dropped(value, k[key], f"{path}{key}.")
            if found:
                return found
    elif isinstance(sent, list) and isinstance(kept, list):
        for a, b in zip(cast(list[Any], sent), cast(list[Any], kept), strict=False):
            found = _dropped(a, b, path)
            if found:
                return found
    return None


def _lite_names(twin: Any) -> dict[str, str]:
    """Attribute -> lite JSON name of a twin struct."""
    return {f.name: f.encode_name for f in msgspec.structs.fields(twin)}


_ERROR_TO_FULL = {lite: full for full, lite in _lite_names(gen.ERROR_TYPE).items()}
_HTTP_ERROR_TO_FULL = {lite: full for full, lite in gen.HTTP_ERROR_BODY.items()}


def _rename(obj: Any, names: dict[str, str]) -> Any:
    if not isinstance(obj, dict):
        return obj
    return {names.get(k, k): v for k, v in cast(dict[str, Any], obj).items()}


def error(obj: Any) -> Any:
    """A WS reply's lite `error` object ({c, m}) with full keys."""
    return _rename(obj, _ERROR_TO_FULL)


def http_error(obj: Any) -> Any:
    """A lite HTTP error body ({c, m, s, ri}) with full keys. Keys it does not know pass through unchanged."""
    return _rename(obj, _HTTP_ERROR_TO_FULL)


def stream_params(subscribe: bool, stream: str, selectors: list[str], use_global_sequence_number: bool) -> bytes:
    """Lite params of a subscribe or unsubscribe request."""
    twin = gen.SUBSCRIBE_PARAMS if subscribe else gen.UNSUBSCRIBE_PARAMS
    return codec.encode(twin(stream=stream, selectors=selectors, use_global_sequence_number=use_global_sequence_number))


_REQ = _lite_names(gen.REQUEST_TYPE)
_FRAME_HEAD = b'{"' + _REQ["jsonrpc"].encode() + b'":"2.0","' + _REQ["method"].encode() + b'":"'
_FRAME_PARAMS = b'","' + _REQ["params"].encode() + b'":'
_FRAME_ID = b',"' + _REQ["id"].encode() + b'":'


def jsonrpc_frame(method: str, params_json: bytes, rid: int) -> bytes:
    """protocol.jsonrpc_frame with lite envelope keys; params_json is already lite."""
    return _FRAME_HEAD + method.encode() + _FRAME_PARAMS + params_json + _FRAME_ID + str(rid).encode() + b"}"


def _frame_names() -> dict[str, str]:
    response = _lite_names(gen.RESPONSE_TYPE)
    rename = {k: response[k] for k in ("id", "method", "result", "error")}
    rename.update(gen.FEED_FRAME)
    rename["code"] = gen.HTTP_ERROR_BODY["code"]
    if len(set(rename.values())) != len(rename):
        raise RuntimeError(f"lite frame keys collide: {rename}")
    return rename


class Frame(msgspec.Struct, omit_defaults=True, rename=_frame_names()):
    """codec.Frame for lite frames. `m` is the method; a REST-style error body ({c, m, s}) puts its message there
    and its status in `s`, so `message` reads `method` and `stream` accepts an int."""

    id: int | None = None
    method: str | None = None
    result: msgspec.Raw = None  # type: ignore[assignment]  (Raw cannot be in a union)
    error: dict[str, Any] | None = None
    stream: str | int | None = None
    selector: str | None = None
    sequence_number: int | None = None
    prev_sequence_number: int | None = None
    code: int | None = None

    @property
    def message(self) -> str | None:
        return self.method


_frame_decoder = msgspec.json.Decoder(Frame, strict=False)


def decode_frame(data: bytes) -> Frame:
    return _frame_decoder.decode(data)


class SubscribeResult(msgspec.Struct, rename=_lite_names(gen.SUBSCRIBE_RESULT)):
    """codec.SubscribeResult with lite keys."""

    stream: str = ""
    subs: list[str] = []
    unsubs: list[str] = []
    num_snapshots: list[int] = []
    first_sequence_number: list[int] = []
    latest_sequence_number: list[int] = []


_subscribe_decoder = msgspec.json.Decoder(SubscribeResult, strict=False)


def subscribe_result(data: bytes) -> SubscribeResult:
    return _subscribe_decoder.decode(data)
