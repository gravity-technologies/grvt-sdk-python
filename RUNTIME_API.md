# Python runtime API (generator target)

Generated code in `src/grvt/_generated/` talks to the hand-written runtime only through the names on
this page, all importable from `grvt._runtime`. If the generator needs something not listed here, add
it to the runtime and to this page; never reach into private attributes.

The generator is `sdk/generator/python` (`go run ./sdk/generator python`, or `make sdk-gen` for everything). Its
output in `src/grvt/_generated/` (not committed) is the reference for everything below.

## 1. Models (one per contract type)

```python
class OrderLeg(GrvtModel):  # GrvtModel = msgspec.Struct, kw_only=True, omit_defaults=True
    """One-line description."""

    instrument: str
    size: Decimal
    is_buying_asset: bool
```

- Public field names follow contract aliases. Required fields have no default. Optional fields default
  to `None`; nullable required fields accept `None` but must be supplied. Signed calls use `Input`
  models for SDK-filled fields. Only declared partial streams use `Partial` payload models.
- Wire type -> Python type (from the contract's `json_type`):

  | wire | Python | notes |
  |---|---|---|
  | decimal string (`"65000.1"`) | `decimal.Decimal` | exact value; encoded as a string |
  | 64-bit int as string (`"1793172225200080949"`) | `str` | the gateway rejects these as JSON numbers (checked on testnet) |
  | uint8/16/32 number | `int` | |
  | enum string | generated `StrEnum` | unknown values remain typed enum members |
  | timestamp string | `Timestamp` | signed integer nanoseconds; `to_datetime()` rounds down to microseconds |
  | bool / string / list / nested type | `bool` / `str` / `list[T]` / `T` | |

- Optional fields default to `None` (or the zero value the server treats as absent).
- Decoding uses `codec.decode(bytes, Type)` (non-strict: also accepts `"123"` for `int` fields).
  Unknown fields are ignored.

## 2. RPCs (one `RpcSpec` constant + one method per RPC)

```python
RPC_CREATE_ORDER = RpcSpec(
    method="v1/create_order",  # JSON-RPC method == REST route tail
    gateway="td",  # "md" | "td" | "edge"
    path="/full/v1/create_order",  # REST path (edge routes use their own path)
    response_type=ApiCreateOrderResponse,
    auth=True,  # needs the session cookie
    write=True,  # never retried; ambiguous failure -> OrderStateUnknown
    order_entry=True,  # goes over the trading WS when order_transport="ws"
    refresh_metadata_on=frozenset({2062, 2064, 2065, 2066, 2067}),  # order calls only: on these codes,
    # drop the request's legs[].instrument from the metadata cache
    request_type=ApiCreateOrderRequestInput,  # request model: the lite encoder builds its twin
    lite_path="/lite/v1/create_order",  # REST path in the lite encoding; None for edge routes
)


async def create_order(self, *, order: m.OrderInput) -> m.ApiCreateOrderResponse:
    req = m.ApiCreateOrderRequest(order=order)  # keyword args = request fields, required first
    return await self.rt.call(
        specs.ORDER_CREATE_ORDER,
        req,
        sign=signing.sign_order_create_order,  # only for signed RPCs
        client_order_id=order.metadata.client_order_id,
    )  # order writes only
```

Top-level 64-bit int arguments are typed `int | str` and passed through `w.as_str(...)`.

`Runtime.call(rpc, params, *, sign=None, client_order_id=None, timeout=None)` does, in order:
`before_send` hook -> `sign(rt, params)` -> encode -> REST or WS -> decode `response_type`.
For REST the whole body is the response struct; over WS the JSON-RPC `result` is that same struct.
Errors come back as exceptions: `ApiError` (or a generated subclass), `RateLimitError`,
`TransportError`, `OrderStateUnknown`, `DecodeError` (a body that does not match `response_type`; on a write it
is the cause of `OrderStateUnknown`). `client_order_id` may be a list (bulk orders: one id per order);
`OrderStateUnknown.client_order_ids` then carries all of them.

With `encoding="lite"` (the default) the runtime sends gateway calls to `lite_path` and renames at the wire only (section 7);
the request, the hooks, signing and the decoded model keep full names.

## 3. Streams (one `StreamSpec` constant + one method per stream)

```python
STREAM_BOOK_DELTA = StreamSpec(
    stream="v1.book.d",
    gateway="md",
    feed_type=WSOrderbookLevelsFeedDataV1,  # the whole frame: stream, selector, sequence_number,
    # prev_sequence_number, feed
    auth=False,
    snapshot_feed=False,  # True when every frame is complete (e.g. v1.ticker.s): gaps need no resync
    conflate=False,  # True for snapshot streams only; others raise StreamOverflow and re-sync
)


def orderbook_delta_stream(
    self, *, instrument: str, rate: int | None = None, depth: int | None = None
) -> Subscription[m.WSOrderbookLevelsFeedDataV1]:
    return self.rt.stream(specs.STREAM_ORDERBOOK_ORDERBOOK_DELTA, w.selector([instrument], [rate, depth]))
```

`w.selector(primary, secondary)` joins the non-None fields as `p1-p2@s1-s2`. Every stream method gets a
`_stream` suffix (`naming.StreamSuffix`), so it never shares a name with an RPC. `Subscription` is an
async iterator (iterating subscribes) and an async context manager (`close()` unsubscribes). After a
gap or a reconnect the next item is a fresh snapshot (`sequence_number == "0"`).

## 4. Signed types (one `StructType` constant + one sign function per signed RPC)

```python
EIP712_ORDER = StructType(
    "Order",
    {
        "Order": [{"name": "subAccountID", "type": "uint64"}, ..., {"name": "legs", "type": "OrderLeg[]"}, ...],
        "OrderLeg": [{"name": "assetID", "type": "uint256"}, ...],
    },
    type_hash="0xb4b857ba...",
    hasher=_hash_order,
)  # from the contract; checked at import
```

`_hash_order` is generated per struct: one precompiled `struct.Struct` laid out as the EIP-712 encoding
(type hash + one 32-byte word per field), one pack, one keccak. Values that do not pack (out of range, not an
int) are retried through the checked helpers in `_runtime/signing/encode.py`, which raise the precise range
error. A generic encoder exists only as the test reference (`tests/eip712_reference.py`).

`StructType` is internal (not exported from `grvt`) and always takes the generated `hasher`: the SDK signs only
Grvt's own messages. `Runtime.sign` signs with a `PrivateKeySigner` directly from the generated digest. Any other
`Signer` gets the typed data through `sign_typed`, and its signature is checked against the generated digest
before anything is sent (the rules and the KMS high-s note are in [README.md](README.md#quickstart)).

The sign function works on the request as JSON data (`d = codec.to_builtins(req)`), so contract
paths apply directly:

```python
async def sign_order_create_order(rt: Runtime, req: m.ApiCreateOrderRequest) -> m.ApiCreateOrderRequest:
    d = codec.to_builtins(req)
    sig = w.prepare_signature(rt, d, ("order", "signature"), SIGNATURE_TTL_NS)  # fills signer/nonce/expiration/chain_id
    meta = await _metadata(rt, instruments, currencies)  # cached RPC lookups
    w.check_order(ORDER_CHECKS, w.get(d, "order"), meta)  # calls that carry an Order
    if w.address_set(w.get(d, "order", "builder")):  # variant rule
        struct, message = EIP712_ORDER_WITH_BUILDER_FEE, _message_order_with_builder_fee(d, meta)
    else:
        struct, message = EIP712_ORDER, _message_order(d, meta)
    await w.sign_into(rt, sig, struct, message)  # domain chainId = signature.chain_id or env's; writes r/s/v
    return codec.convert(d, m.ApiCreateOrderRequest)
```

Contract transform -> generated expression (helpers in `grvt._runtime.signing.wire`, imported as `w`):

| transform | code |
|---|---|
| `copy`, `sig.nonce`, `sig.expiration` | `w.copy(w.get(d, *path), "<eip type>", "<where>"[, default])` |
| `address` | `w.address(value)` (absent = zero address) |
| `enumCode` | `w.enum_code(value, ENUM_CODES["TimeInForce"], where)` (absent = 0) |
| `scaled` fixed | `w.scaled(value, 9, where[, default])` (raises on over-precision, never rounds) |
| `scaled` instrument_base_decimals | `w.scaled(value, meta.instrument(name)["base_decimals"], where)` |
| `scaled` instrument_quote_decimals | `w.scaled(value, meta.instrument(name)["quote_decimals"], where)` |
| `scaled` currency_balance_decimals | `w.scaled(value, meta.currency(symbol)["balance_decimals"], where)` |
| `assetKey` | `to_int(meta.instrument(name)["instrument_hash"])` |
| `currencyId` | `meta.currency(symbol)["id"]` |
| `mainAccount` | `meta.main_account()` (`_metadata(..., main_account=True)` loads it once per client) |
| `each` | list comprehension over a nested `_message_<type>_<struct>` builder |

`ORDER_CHECKS` is the contract's `order_checks` as a dict (`legs`, `instrument`, `scales` as an int or
`"base_decimals"`, `rules`). `w.check_order` raises `OrderOutOfBounds` (a `ValueError`) for the first leg
outside its instrument's limits and `ValueError` for a malformed value; it never changes the order.

Variant rule kinds the generator knows: `builder`, `wallet_types`, `currency_width`, and a single type.
An unknown kind fails generation.

## 5. Errors

```python
register_error_codes({2001: SignatureExpiredError, ...}, retryable={1014, 1015, ...}, statuses={2001: 400, ...})
```

Called once at import of the generated package. Each class subclasses `ApiError`. The runtime uses
`retryable` for the read retry rule (`rate_limit_code`, a retryable code, or a status in `read_retry_statuses`; both keys live in the generated behavior module).

## 6. Client shell

```python
class AsyncGrvt(api.AsyncApi):  # AsyncApi: every RPC and stream as a method (client.create_order)
    def __init__(self, env="testnet", *, api_key=None, signer=None, order_transport="rest", **options):
        self.rt = Runtime(env, api_key=api_key, signer=signer, order_transport=order_transport, **options)

    # close / __aenter__ / __aexit__


class Grvt(api.SyncApi):  # blocking; SyncApi: the same methods
    def __init__(self, env="testnet", **kw):
        self._runner = SyncRunner()
        self.aio = self._runner.build(lambda: AsyncGrvt(env, **kw))
        self._async = self.aio


# each sync RPC:     return self._runner.run(self._async.create_order(order=order))
# each sync stream:  return SyncSubscription(self._runner, self._async.orderbook_delta_stream(...))
```

`Runtime(...)` keyword args the shell may pass through: `api_key`, `signer`, `order_transport`
(`"rest"|"ws"`), `encoding` (`"lite"` default `|"full"`), `check_orders`, `clock`, `nonces`, `client_order_ids`, `transport` (HTTP plug-in), `before_send`,
`on_event`, `http_attempt_timeout`, `call_timeout`, `ws_request_timeout`, `resync_timeout`, `queue_size`. These keywords
are described by
`ClientOptions`; generated constructors use `Unpack[ClientOptions]` so type checkers validate them.

Every generated RPC accepts `options: CallOptions | None`.
Its optional `timeout` bounds the complete call in seconds, including hooks, signing, metadata, authentication, retries and the response;
it overrides the client's `call_timeout`. `http_attempt_timeout` caps each HTTP attempt and `ws_request_timeout` a WebSocket reply or
subscription acknowledgement. Defaults come from `grvt/_generated/behavior.py` (built from `contract/behavior.json`). All timeout values must be finite and positive.

A timeout before dispatch raises `TimeoutError` and sends no write.
Cancellation uses normal `asyncio` cancellation before dispatch.
After a write starts, the SDK deadline raises `OrderStateUnknown` with the final submitted IDs. External cancellation keeps the native `asyncio.CancelledError`, so task groups and `task.cancelled()` work normally. Its `__cause__` holds an `OrderStateUnknown` with the RPC name and final submitted IDs; catch cancellation to save these IDs for reconciliation, then re-raise it. The cause is absent if nothing was dispatched. Reconciliation belongs to the caller that catches cancellation, outside the canceled task or task group. Request bodies and credentials are never attached.
Shared metadata and login tasks can finish after a caller stops waiting.
Closing the client cancels those owned tasks.
Synchronous hooks and noncooperative plug-ins cannot be interrupted, but the runtime checks the absolute deadline again before dispatch.

## HTTP redirects and retries

The SDK does not follow HTTP redirects for API requests or WebSocket handshakes.
A redirected WebSocket handshake fails before a second request, so session cookies and account headers stay at the configured endpoint.
A redirect on a write returns an uncertain outcome instead of sending the signed request to another endpoint.
The default transports also avoid retries after an uncertain write failure.
Custom HTTP transports must preserve these rules.

## 7. Lite encoding

`src/grvt/_generated/lite.py` has a twin struct per gateway type: the model's base (`GrvtModel`), attributes and
types, with lite JSON names (`msgspec.field(name="p")`). Lite names repeat across types (`s` is a stream, a size and
a status), so each type has its own twin, and nested fields point to nested twins. Per type it also has
`to_full_<Type>` and `to_lite_<Type>`, collected in `TYPES` (model -> twin, to_full, to_lite), plus the envelope
types, the stream frame keys (`FEED_FRAME`) and the HTTP error body keys (`HTTP_ERROR_BODY`).
`grvt._runtime.lite` uses them only at the wire:

- **Responses and stream items:** `lite.decode(bytes, Model)` decodes into the twin in one msgspec pass (unknown
  keys skipped), then `to_full_<Type>` turns each twin into its model in place by setting `__class__`. Twin and
  model have the same base and the same slots, so Python allows it (and raises `TypeError` if they ever differ);
  the result is an ordinary model instance. This costs about the same CPU as a full decode.
- **Requests:** `lite.encode(rpc.request_type, params)` builds new twins with `to_lite_<Type>` (the caller's objects
  are never changed) and encodes them. A twin has exactly the type's fields, so no other key can reach the wire.
  A dict request (for example from `before_send`) is converted to the model first, and a key the model would
  drop raises `ValueError` before anything is sent: the lite server would ignore it and read the field as empty.
- **WebSocket envelope:** the read loop decodes frames with `lite.Frame` (the same fields as `codec.Frame`, lite
  keys) in one msgspec pass; `lite.jsonrpc_frame` builds requests. Error objects, subscribe params and subscribe
  results use the names of their contract types. Error replies may carry id 0 and an empty method.
- **Full stays as it was:** a full client binds the same `codec`/`protocol` functions as before at construction,
  so its bytes and its hot path do not change.
