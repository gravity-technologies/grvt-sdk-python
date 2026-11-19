# grvt: Python SDK for Grvt

The Python client for the Grvt exchange: REST calls, WebSocket streams, order entry, signing and login in one
package.

> **Preview:** any 0.x release can change the API, so pin the exact version you install (for example in
> requirements.txt).

## Install

```sh
pip install grvt
```

Needs Python 3.11 or newer.

## Quickstart

Place a post-only limit order on testnet. Set `GRVT_API_KEY`, `GRVT_PRIVATE_KEY` and `GRVT_SUB_ACCOUNT_ID` first.

```python
import asyncio
import os
from decimal import Decimal

from grvt import AsyncGrvt, PrivateKeySigner, enums, models as m


async def main():
    async with AsyncGrvt(
        env="testnet",
        api_key=os.environ["GRVT_API_KEY"],
        signer=PrivateKeySigner(os.environ["GRVT_PRIVATE_KEY"]),
    ) as client:
        order = m.OrderInput(
            sub_account_id=os.environ["GRVT_SUB_ACCOUNT_ID"],
            time_in_force=enums.TimeInForce.GOOD_TILL_TIME,
            post_only=True,
            legs=[
                m.OrderLeg(
                    instrument="BTC_USDT_Perp", size=Decimal("0.01"), limit_price=Decimal("60000"), is_buying_asset=True
                )
            ],
        )
        resp = await client.create_order(order=order)
        print(resp.result.metadata.client_order_id)


asyncio.run(main())
```

For a runnable script that reads market data and can place, watch and cancel one order, download the
[Python quickstart example](https://developers.grvt.io/downloads/examples/python/examples/quickstart.py) and
follow the [Quickstart guide](https://developers.grvt.io/guides/quickstart). The package itself doesn't include
the examples.

## Documentation

- [Python SDK](https://developers.grvt.io/sdks/python): client options, streams, orders, errors, helpers
- [Quickstart](https://developers.grvt.io/guides/quickstart)
- [Signing](https://developers.grvt.io/guides/signing)
- [Errors and retries](https://developers.grvt.io/guides/errors-and-retries)
- [Recovery recipes](https://developers.grvt.io/guides/recovery): lost order replies, stream gaps
- [WebSocket](https://developers.grvt.io/guides/websocket)
- [API reference](https://developers.grvt.io/reference/index)

## Python notes

- `AsyncGrvt` is the asyncio client. `Grvt` is the blocking one with the same methods; it runs its own event
  loop on a background thread, so it also works in Jupyter.
- Every call is a method on the client, `client.create_order(...)`, and every stream ends in `_stream`,
  `client.trade_stream(...)`. Pass request fields as keyword arguments; you get back a typed model.
- Prices, sizes and other decimals are `decimal.Decimal`, never `float`.
- 64-bit integers (IDs, sequence numbers) are `str` inside models, because the exchange rejects them as JSON
  numbers. Top-level method arguments accept `int | str`.
- Timestamps are `grvt.Timestamp`, in integer nanoseconds.

## Known limitations

- Over WebSocket order entry, the server may process requests out of order, so a cancel can overtake its
  create. Wait for the create's reply before you cancel.
- Private streams can't resume from a sequence number. After a gap or reconnect, fill missed events from REST
  history.
- No re-login after a WebSocket auth failure: a subscribe rejected for auth raises to your code, and a rejected
  handshake (401/403) or failed API-key login stops reconnecting for good.
- Session renewal closes and reopens every socket of the client, so every stream gets a gap.
- Wallet login isn't supported (API-key sessions only), and signing covers Grvt's own message types only.
- Response fields the SDK doesn't know are dropped, not kept.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [RUNTIME_API.md](RUNTIME_API.md).

## License

MIT. See [LICENSE](LICENSE).
