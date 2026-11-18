# grvt (Python SDK, preview)

> **Preview.** This SDK is still being stabilised. Its API can change in any 0.x release, so pin an exact
> version. It becomes stable at 1.0.0.

The Python SDK for Grvt: REST, WebSocket streams, WebSocket order entry, signing and login in one
package. Everything that grows with the API (models, enums, errors, RPC/stream methods, sign functions)
is generated from `sdk/contract/contract.json` by `sdk/generator/python`; the small hand-written runtime
in `src/grvt/_runtime` stays the same size however many APIs exist.

## Quickstart

Use the [canonical quickstart](https://developers.grvt.io/guides/quickstart) and
[download its Python example](https://developers.grvt.io/downloads/examples/python/examples/quickstart.py).
The example reads public testnet data by default. Add `--trade` to place, observe, and cancel one testnet order.
Trading requires `GRVT_API_KEY`, `GRVT_PRIVATE_KEY`, and `GRVT_SUB_ACCOUNT_ID`. An order can fill before cancellation.
The example uses current instrument limits and stops after bounded observation attempts.

From this SDK's directory, run:

```sh
uv sync --locked
uv run --locked python examples/quickstart.py
```

The wheel contains the library, not the example files. Download the example or use a source checkout.

Signing covers Grvt's own messages only (the generated signed types); there is no API for custom EIP-712 types.
`PrivateKeySigner` signs in process. A wallet or KMS signer implements `Signer.sign_typed(domain, primary_type,
message)` and can hand `primary_type.to_typed_data(domain, message)` to its backend. The SDK then checks the
returned signature against its own digest: it must be low-s, recover to `signer.address`, and have a signer field
that is empty or equal to that address, or `GrvtError` is raised. KMS backends that return a high s (AWS KMS, for
one) need it normalized (s = n - s, flip v).

`Grvt(...)` is the blocking client with the same methods. Every RPC is a method directly on the client,
`client.<method_name>(...)` (`client.create_order(...)`), and every stream is `client.<method_name>_stream(...)`
(`client.trade_stream(...)`). The contract's namespace is only a docs category, so it never appears in your
code. RPC methods take the request fields as keyword arguments and return the response model; stream methods
take the selector fields.

## Generated source

The public source repo includes the generated code and shared test fixtures. For development in the platform
monorepo, run `make sdk-gen-python` from its root before building this SDK. Generated source comes from the
Grvt API contract. Send proposed changes to the Grvt maintainers so they can update the monorepo.

## Layout

```
src/grvt/__init__.py        public names: AsyncGrvt, Grvt, models, enums, signers, hooks, errors
src/grvt/_generated/        GENERATED (do not edit): models, enums, errors, specs, signing, api, client
src/grvt/_runtime/          hand-written runtime, same size however many APIs exist
  runtime.py                Runtime: call(), stream(), sign(), session rotation, metadata cache
  ws.py                     one managed socket per gateway; Subscription
  protocol.py               sans-IO state: request table, sequencer, backoff
  rest.py, transport.py     REST calls, HTTP transport plug-in (aiohttp)
  auth.py                   api-key login, cookie + X-Grvt-Account-Id
  signing/                  EIP-712 domain + generated-hasher plumbing, Signer, PrivateKeySigner, transforms, wire helpers
  codec.py, spec.py         msgspec helpers; RpcSpec / StreamSpec descriptors
  hooks.py, log.py          before_send / on_event; GRVT_LOG=debug with redaction
  env.py, clock.py, errors.py, sync.py
RUNTIME_API.md              the contract between generator (sdk/generator/python) and runtime
tests/                      unit, fake-gateway, signing-case and generated-API tests; one testnet smoke test
benchmarks/hot_path.py      signing, encode and decode timings (uv run --locked python benchmarks/hot_path.py)
```

## Run the tests

```
uv sync
uv run pytest                       # everything offline
GRVT_SMOKE=1 uv run pytest -s tests/test_smoke_testnet.py   # read-only, unauthenticated testnet check
```

## How the main pieces behave

- **Order transport** is chosen per client: `order_transport="rest"` (default) or `"ws"`. Writes are
  never retried. If a write may have reached the exchange but no answer came back (timeout, socket
  drop), the call raises `OrderStateUnknown` with the client order id (`client_order_ids` lists every
  order of a bulk call); look the order up, don't resend.
- **Reads** retry up to 3 times with full-jitter backoff on retryable errors.
- **Encoding** is chosen per client: `encoding="lite"` (default) or `"full"`. Lite is the same API with short
  JSON field names, so order book frames are about a third smaller. It switches every gateway call
  (`/lite/v1/...`) and both WebSockets (`/ws/lite`); edge login and `GET /time` stay full. Your code doesn't
  change: models, errors, hooks and signing keep the full names, and the SDK renames per type only on the wire.
  Decoding costs about the same CPU as full; encoding a request adds about half a microsecond
  (`benchmarks/hot_path.py`). Lite WebSockets use your `md_ws_url` / `td_ws_url` with `/ws/full` swapped for `/ws/lite`; set
  `md_ws_lite_url` / `td_ws_lite_url` on a custom `Environment` only when its URLs don't end in `/ws/full`.
- **Order checks**: before signing, each leg is checked against its instrument's tick, size step,
  `min_size` and `min_notional`. A miss raises `OrderOutOfBounds` with the nearest valid values
  (`limit_price 60000.03 isn't a multiple of tick_size 0.1 (nearest: 60000 or 60000.1)`); nothing is sent
  and nothing is rounded. If the exchange still rejects an order for these limits, the instrument's cached
  metadata is dropped so the next order refetches it. `check_orders=False` turns the check off and leaves
  the limits to the exchange.
- **Streams** use global sequence numbers. The snapshot frame has `sequence_number` 0, and the
  first update's `prev_sequence_number` is at or below the subscribe reply's `latest_sequence_number`. The
  first frame after a (re)subscribe reply is accepted
  when its `prev_sequence_number` is 0 or not greater than the latest sequence number known then
  (testnet sends the old sequence number there). After that, `prev_sequence_number` must equal the
  last sequence number seen; otherwise it is a gap: the runtime re-subscribes the same selector and
  drops frames until the new snapshot. No snapshot within 5s -> reconnect. On reconnect every
  subscription is re-sent.
- **Backpressure**: the read loop never waits for a consumer; each subscription has a bounded queue.
  Only snapshot streams (every frame complete, `*.s`) drop the oldest frame when the consumer falls
  behind. Every other stream (deltas, events, private) raises `StreamOverflow` and re-syncs.
- **Session**: api-key login on edge gives the `gravity` cookie and `X-Grvt-Account-Id`; both go on REST
  calls and the WS handshake. The runtime re-logs in at 80% of the cookie lifetime.

## Draft notes (shortcuts and TODOs)

- **Session rotation on WS is simplified.** After a re-login the sockets wait until no write is in
  flight, then reconnect with the new cookie (break-before-make). The design calls for make-before-break:
  open the new socket, subscribe, move order entry, then drain the old one.
- **No re-login on a WS 1000 (unauthorized) reply.** A subscribe that fails auth raises to the consumer.
  Handshake 401/403 and a failed api-key login stop reconnecting for good.
- **Frame-size check exists but is off by default.** The gateway library default `ReadLimitSize` is
  1024 bytes, but the deployed value is not published, so `Environment.ws_max_frame_bytes` is `None` for
  testnet/production. When set, an oversized frame raises `FrameTooLarge` (nothing sent). Automatic fallback to
  REST is a TODO.
- **Book deltas are never conflated.** The local `OrderBook` helper merges levels.
  Queue overflow invalidates continuity. See [rebuild a book after a gap](https://developers.grvt.io/guides/recovery#rebuild-a-book-after-a-gap).
- **Request ordering on one WS is not guaranteed by the server today** (several consumer goroutines read
  one channel), so a cancel can overtake its create. The SDK does not work around this.
- **Cancellation countdowns require explicit refresh.** Use `refresh_cancel_countdown`; reconnect does not extend the account-wide timer.
- **Private-stream resume** (`from_sequence_number`) is not possible until the server supports it; after
  a gap or reconnect, private streams re-subscribe and callers should backfill via REST.
- **Unknown fields** in responses are dropped by msgspec, not kept.
- **Only the HTTP side is pluggable** (`HttpTransport`). The WS side is aiohttp only; a picows/uvloop
  transport is a later option.
- **No WireRecorder / ReplayTransport / OpenTelemetry** yet; `on_event` with `FRAME` events gives raw
  bytes (not redacted) for anyone who wants to build them.
- **Signing speed:** signing a 2-leg `create_order` from its model takes about 27 µs: secp256k1 (coincurve)
  about 12 µs, the EIP-712 hash about 2.5 µs (keccak from OpenSSL when it has KECCAK-256, else
  pycryptodome), and the order checks and message building most of the rest. Hash + sign alone is about
  15 µs, about 5x faster than eth-account. Byte-equal signatures to eth-account are checked in
  `tests/test_signing.py`. All 12 server signing cases in `sdk/scenarios/signing.json` pass through the generated
  sign functions.
- **Decode speed:** a 1.2 KB `v1.book.d` frame (10+10 levels) decodes into its model in about 4 µs, and the
  whole stream path (envelope, route, sequence, decode) takes about 5.2 µs. Untyped msgspec reads the same
  frame in about 1.8 µs; most of the difference is building the 40 exact `Decimal` values. The read loop
  reads only the envelope (about 0.65 µs, skipping the payload) and the consumer decodes the whole frame.
  Numbers from `uv run --locked python benchmarks/hot_path.py` on one Apple M-series core.
- **64-bit ints are `str` in models** because the gateway rejects them as JSON numbers. Top-level method
  arguments accept `int | str`; fields inside models (e.g. `Order.sub_account_id`) must be strings.
- **Exact values:** decimal fields use `decimal.Decimal`. Timestamps use `grvt.Timestamp` with integer
  nanoseconds. `to_datetime()` explicitly rounds down to microseconds; `from_datetime()` requires a timezone.
  Enum fields use the generated enums and retain unknown values from newer servers.
- **Required fields follow the contract.** Constructors and decoding reject missing required values.
  Signed calls use `Input` models where the SDK fills signatures and metadata. Only contract-declared
  partial streams use `Partial` payload models. Required arrays can be `None` when the wire value is null.
- **Read/write access is explicit in the contract.** New RPC handlers need a reviewed classification before
  contract generation succeeds. SDK generation rejects missing or invalid access. Only explicit reads can retry.
- **Flat names must be unique.** Methods are not grouped by namespace, so two calls (or two streams) with
  the same `method_name` in different namespaces would clash; the generator fails when that happens.
- **Authentication belongs to the session runtime.** Set `api_key=` on the client to log in.
  Raw edge login routes are not generated client methods; wallet session login is not supported.
- **Not signed yet** (no contract mapping): algo orders, vault, session keys and account management.
  `bulk_orders` signs every order on its own (own nonce and expiration, same Order / OrderWithBuilderFee
  rule as `create_order`); both fill a missing `client_order_id` on each order.
- No Bazel BUILD files for the SDK or the generator; this is a uv project (Python pinned to 3.12 in `.python-version`; coincurve has
  no 3.14 wheel yet).

## Uncertain orders and stream gaps

Use the [recovery helpers and gap ranges](https://developers.grvt.io/guides/recovery) to look up uncertain orders and detect lost stream continuity.
A missing order stays unknown. The helpers never resend an order.

## Per-call options

Use `options=CallOptions(timeout=2.0)` to set a two-second total limit for one RPC:

```python
from grvt import AsyncGrvt, CallOptions

async with AsyncGrvt(call_timeout=30.0, http_attempt_timeout=10.0) as client:
    instrument = await client.get_instrument(instrument="BTC_USDT_Perp", options=CallOptions(timeout=2.0))
```

The blocking client accepts the same option. Timeouts use seconds.
`call_timeout` defaults to 30 seconds for the whole RPC, including authentication, metadata, signing, retries, and the response.
`http_attempt_timeout` caps each HTTP attempt at 10 seconds by default, within the remaining total limit.
Replace the old `request_timeout` setting with `http_attempt_timeout`.
`CallOptions.timeout` now limits the whole call instead of each transport attempt.
Client keyword settings are typed through `ClientOptions`, so type checkers catch unknown names.

Bulk order calls and models use `order_ids` and `client_order_ids`. The SDK translates them to the
server's `order_i_ds` and `client_order_i_ds` JSON keys; raw HTTP callers must use the wire keys.

## Pagination

Use `pages` to follow cursors without fetching ahead. Keep filters fixed in the callback.
Use `pages_sync` with the synchronous client.

```python
from grvt import pages

async for page in pages(lambda cursor: client.trade_history(instrument="BTC_USDT_Perp", cursor=cursor)):
    for trade in page.result or []:
        print(trade)
```

An empty page can still have a next cursor. Breaking the loop stops further calls.
A repeated cursor raises `PaginationCycle`; fetch errors pass through without extra retries.
Cursors do not guarantee a stable snapshot while records change.

## Default sub-account

Set `sub_account_id="123"` on `AsyncGrvt` or `Grvt` to fill omitted request and stream account IDs.
Explicit IDs take precedence. An order in `bulk_orders` first inherits the request account, then the client default.
Transfer source and destination IDs stay explicit. Defaults are applied to copies before signing.
Without a default, calls that need an account still require one.

## Recovery helpers

The SDK exposes lookup-only order resolution, history backfill with unproven continuity,
and explicit cancellation-countdown refresh.
Session rotation reports that authenticated WebSockets must reconnect.
See the [recovery recipes](https://developers.grvt.io/guides/recovery), including [rebuilding a local order book](https://developers.grvt.io/guides/recovery#rebuild-a-book-after-a-gap), before restoring trading after a gap.
