# Contributing to the Python SDK

This page is for people changing the SDK itself. To use the SDK, start with [README.md](README.md).

## How the code is made

Most of the SDK is generated. Everything that grows with the API (models, enums, errors, RPC and stream
methods, sign functions) is generated from the Grvt API contract (`sdk/contract/contract.json` in the platform
repository) by the generator in `sdk/generator/python`. The small hand-written runtime in `src/grvt/_runtime`
stays the same size however many APIs exist. [RUNTIME_API.md](RUNTIME_API.md) is the contract between the
two: generated code may only use the names it lists.

The generated code (`src/grvt/_generated/`) and `CHANGELOG.md` are git-ignored build output. The public source
repository includes them, along with the shared test fixtures. In the platform repository, generate them first:

```sh
make sdk-gen-python    # from the repository root; needs the repository's Go toolchain
```

Don't edit generated files. Send proposed changes to the Grvt maintainers so they can update the generator or
the contract in the platform repository.

Two generator rules worth knowing:

- **Read/write access is explicit in the contract.** A new RPC handler needs a reviewed read/write
  classification before contract generation succeeds, and SDK generation rejects missing or invalid access.
  Only explicit reads are retried.
- **Method names must be unique across namespaces.** Methods are flat on the client (the namespace is only a
  docs category), so two calls or two streams with the same `method_name` would clash. The generator fails
  when that happens.

## Build from source

To install the SDK from a platform-repository checkout (Python 3.11 or newer, plus the Go toolchain):

```sh
make sdk-gen-python
pip install -e sdk/languages/python
```

## Layout

```
src/grvt/__init__.py        public names: AsyncGrvt, Grvt, models, enums, signers, hooks, errors
src/grvt/_generated/        GENERATED (do not edit): models, enums, errors, specs, signing, api, client
src/grvt/_runtime/          hand-written runtime
  runtime.py                Runtime: call(), stream(), sign(), session rotation, metadata cache
  ws.py                     one managed socket per gateway; Subscription
  protocol.py               sans-IO state: request table, sequencer, backoff
  rest.py, transport.py     REST calls, HTTP transport plug-in (aiohttp)
  auth.py                   api-key login, cookie + X-Grvt-Account-Id
  signing/                  EIP-712 domain + generated-hasher plumbing, Signer, PrivateKeySigner, transforms, wire helpers
  codec.py, spec.py         msgspec helpers; RpcSpec / StreamSpec descriptors
  hooks.py, log.py          before_send / on_event; GRVT_LOG=debug with redaction
  env.py, clock.py, errors.py, sync.py
src/grvt/*.py               recovery helpers: pagination, backfill, order book, countdown, order lookup
RUNTIME_API.md              the contract between the generator and the runtime
examples/                   quickstart.py and recovery.py (not shipped in the wheel)
tests/                      unit, fake-gateway, signing-case and generated-API tests; one testnet smoke test
benchmarks/hot_path.py      signing, encode and decode timings
```

## Tools

This is a uv project. There are no Bazel BUILD files for the SDK or its generator. Development uses Python 3.12
(`.python-version`) because coincurve has no 3.14 wheel yet; the package itself supports 3.11 and newer.

Library choices:

- **msgspec** for models and JSON. It drops unknown response fields rather than keeping them.
- **aiohttp** for HTTP and WebSocket. Only the HTTP side is pluggable (`HttpTransport`); the WebSocket side is
  aiohttp only.
- **coincurve** for secp256k1 signing. Keccak comes from OpenSSL when it has KECCAK-256, else pycryptodome.

## Run the tests

```sh
uv sync --locked
uv run --locked pytest                                                  # everything, offline
GRVT_SMOKE=1 uv run --locked pytest -s tests/test_smoke_testnet.py     # read-only, unauthenticated testnet check
uv run --locked python examples/quickstart.py                           # the quickstart example
```

`tests/test_signing.py` checks that signatures are byte-equal to eth-account's. All 12 server signing cases in
`sdk/scenarios/signing.json` pass through the generated sign functions.

## Performance

Run `uv run --locked python benchmarks/hot_path.py`. Numbers below are from one Apple M-series core.

- **Signing** a 2-leg `create_order` from its model takes about 27 µs: secp256k1 about 12 µs, the EIP-712
  hash about 2.5 µs, and the order checks and message building most of the rest. Hash + sign alone is about
  15 µs, about 5x faster than eth-account.
- **Decoding** a 1.2 KB `v1.book.d` frame (10+10 levels) into its model takes about 4 µs, and the whole stream
  path (envelope, route, sequence, decode) about 5.2 µs. Untyped msgspec reads the same frame in about 1.8 µs;
  most of the difference is building the 40 exact `Decimal` values. The read loop reads only the envelope
  (about 0.65 µs, skipping the payload) and the consumer decodes the whole frame.
- **Lite encoding** decodes at about the same CPU cost as full; encoding a request adds about half a
  microsecond.

## Known shortcuts and TODOs

What users can't do yet (re-login after a WebSocket auth failure, request ordering on one WebSocket,
private-stream resume, ...) is listed in [Not supported yet](https://developers.grvt.io/sdks/python#not-supported-yet); keep it there. Below are the
maintainers' notes.

- **Session rotation on WebSocket is break-before-make.** After a re-login each socket waits until no write is
  in flight, then reconnects with the new cookie. The design calls for make-before-break: open the new socket,
  subscribe, move order entry, then drain the old one.
- **The frame-size check is off by default.** The gateway library's default read limit is 1024 bytes, but the
  deployed value isn't published, so `Environment.ws_max_frame_bytes` is `None` for testnet and production.
  When set, an oversized frame raises `FrameTooLarge` and nothing is sent. Automatic fallback to REST is a TODO.
- **Book deltas are never conflated.** The `OrderBook` helper merges levels; queue overflow breaks continuity.
- **WebSocket transport:** a picows/uvloop transport is a possible later option.
- **No WireRecorder, ReplayTransport or OpenTelemetry yet.** `on_event` `FRAME` events carry raw bytes (not
  redacted) for anyone who wants to build them.
