"""HTTP transport plug-in. The default is one aiohttp session with keep-alive.

A custom transport (recording, replay, test double) implements `HttpTransport`.
Errors must be raised as TransportError with `sent=False` only when the request provably did not
leave the process (e.g. connect refused); the runtime uses that to decide whether a write is unknown.
A transport must not replay POSTs or follow their redirects.
Cancellation uses normal asyncio cancellation. A transport must cancel its I/O when its await is
cancelled and must not leave a queued write that can be sent later. It must propagate CancelledError
instead of suppressing cancellation or starting detached write tasks. After dispatch starts, the runtime chains an OrderStateUnknown as
CancelledError.__cause__ and propagates cancellation. SDK deadlines raise OrderStateUnknown.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

import aiohttp

from .errors import TransportError

# HttpTransport.post as a plain callable: async (url, body, headers, timeout) -> (status, headers, body).
PostFn = Callable[[str, bytes, Mapping[str, str], float], Awaitable[tuple[int, Any, bytes]]]


class HttpTransport(Protocol):
    async def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Any, bytes]: ...

    async def get(self, url: str, timeout: float) -> tuple[int, Any, bytes]: ...

    async def close(self) -> None: ...


class AiohttpTransport:
    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session
        self._owned = session is None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=64, keepalive_timeout=60, ttl_dns_cache=300),
                # The runtime sets the cookie header itself; never let aiohttp's jar mix sessions.
                cookie_jar=aiohttp.DummyCookieJar(),
            )
        return self._session

    async def post(self, url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, Any, bytes]:
        return await self._request("POST", url, body, headers, timeout)

    async def get(self, url: str, timeout: float) -> tuple[int, Any, bytes]:
        return await self._request("GET", url, None, {}, timeout)

    async def _request(
        self, method: str, url: str, body: bytes | None, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Any, bytes]:
        try:
            async with self.session.request(
                method,
                url,
                data=body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=False,
            ) as r:
                return r.status, r.headers, await r.read()
        except aiohttp.ClientConnectorError as e:
            raise TransportError(f"connect failed: {e}", sent=False) from e
        except (TimeoutError, aiohttp.ClientError) as e:
            raise TransportError(f"{type(e).__name__}: {e}", sent=True) from e

    async def close(self) -> None:
        if self._owned and self._session is not None:
            await self._session.close()
            self._session = None
