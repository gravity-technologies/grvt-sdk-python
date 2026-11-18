from datetime import UTC, datetime
from decimal import Decimal

import msgspec
import pytest

from grvt import Timestamp
from grvt._runtime.codec import decode, encode


def test_exact_wire_roundtrip() -> None:
    value = Timestamp(1793172225200080949)
    assert encode(value) == b'"1793172225200080949"'
    assert decode(encode(value), Timestamp) == value
    assert decode(encode(Decimal("12345678901234567890.123456789")), Decimal) == Decimal(
        "12345678901234567890.123456789"
    )


def test_datetime_conversion_is_explicit_and_floors() -> None:
    assert Timestamp(-1).to_datetime() == datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    value = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    assert Timestamp.from_datetime(value).to_datetime() == value
    with pytest.raises(ValueError):
        Timestamp.from_datetime(datetime(2026, 1, 1))


@pytest.mark.parametrize("wire", [b'"9223372036854775808"', b'"1.5"', b'"1e9"', b'"--1"', b'""', b"1", b"null"])
def test_invalid_timestamp(wire: bytes) -> None:
    with pytest.raises((msgspec.ValidationError, ValueError, TypeError)):
        decode(wire, Timestamp)


def test_generated_models_decode_exact_scalars_and_future_enums() -> None:
    from grvt import enums, models

    order = decode(
        b'{"sub_account_id":"1","time_in_force":"FUTURE_VALUE","legs":[{"instrument":"X","is_buying_asset":true,"size":"0.123456789012345678901"}],"signature":{"expiration":"1793172225200080949"}}',
        models.OrderInput,
    )
    assert isinstance(order.time_in_force, enums.TimeInForce)
    assert str(order.time_in_force) == "FUTURE_VALUE"
    assert order.legs[0].size == Decimal("0.123456789012345678901")
    assert order.signature.expiration == Timestamp(1793172225200080949)


def test_history_filter_keeps_nanoseconds_on_wire():
    from grvt import models

    value = Timestamp(1793172225200080949)
    request = models.ApiFillHistoryRequest(sub_account_id="123", start_time=value)
    wire = encode(request)
    assert b'"start_time":"1793172225200080949"' in wire
    assert decode(wire, models.ApiFillHistoryRequest).start_time == value
