"""API-key session.

Login: POST {edge}/auth/api_key/login {"api_key": ...}
  -> Set-Cookie: gravity=...; Expires=...
  -> X-Grvt-Account-Id header, which gateways check as a sticky-session token
Both go on every authenticated REST call and on the WS handshake. A WS connection keeps the session it
was opened with; the gateway drops it when that session disappears, so the runtime re-logs in before
expiry and moves sockets to the new session.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from http.cookies import SimpleCookie
from typing import Any

from .._generated import behavior
from . import codec
from .errors import ApiError
from .log import log
from .transport import PostFn

COOKIE_NAME = "gravity"
ACCOUNT_HEADER = "X-Grvt-Account-Id"
LOGIN_PATH = "/auth/api_key/login"
DEFAULT_TTL = behavior.SESSION_DEFAULT_LIFETIME  # used when the cookie carries no expiry
REFRESH_AT = behavior.SESSION_RENEW_AT  # re-login at this share of the cookie lifetime


class Session:
    def __init__(
        self,
        api_key: str | None,
        login_url: str,
        post: PostFn,
        *,
        wall: Callable[[], float] = time.time,
        timeout: float = behavior.HTTP_ATTEMPT_TIMEOUT,
    ):
        self._api_key = api_key
        self._login_url = login_url
        self._post = post  # HttpTransport.post: async (url, body, headers, timeout) -> (status, headers, body)
        self._wall = wall
        self._inflight: asyncio.Task[None] | None = None
        self._timeout = timeout
        self.cookie: str | None = None
        self.account_id: str | None = None
        self.issued_at = 0.0
        self.expires_at = 0.0
        self.generation = 0  # bumps on every successful login
        self.funding_account_address: str | None = None

    def __repr__(self) -> str:
        cookie = "<redacted>" if self.cookie else None
        return (
            f"Session(enabled={self.enabled}, cookie={cookie!r}, account_id={self.account_id!r}, "
            f"generation={self.generation}, expires_at={self.expires_at})"
        )

    @property
    def enabled(self) -> bool:
        return self._api_key is not None

    def headers(self) -> dict[str, str]:
        if not self.cookie:
            return {}
        h = {"Cookie": f"{COOKIE_NAME}={self.cookie}"}
        if self.account_id:
            h[ACCOUNT_HEADER] = self.account_id
        return h

    def refresh_due(self) -> float:
        """Wall-clock time at which a re-login should happen."""
        return self.issued_at + (self.expires_at - self.issued_at) * REFRESH_AT

    def fresh(self) -> bool:
        return self.cookie is not None and self._wall() < self.refresh_due()

    async def ensure(self) -> None:
        if not self.enabled or self.fresh():
            return
        await self.login()

    async def login(self) -> None:
        task = self._inflight
        if task is None:
            task = asyncio.create_task(self._login())
            self._inflight = task

            def done(result: asyncio.Task[None]) -> None:
                if self._inflight is result:
                    self._inflight = None
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
        await asyncio.shield(task)

    async def close(self) -> None:
        task = self._inflight
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _login(self) -> None:
        body = codec.encode({"api_key": self._api_key})
        status, headers, resp = await self._post(
            self._login_url, body, {"Content-Type": "application/json"}, self._timeout
        )
        if status != 200:
            msg = "login failed"
            try:
                msg = codec.decode(resp).get("error", msg)
            except Exception:
                pass
            # 401/403 are permanent until the user fixes the key; reconnect loops stop on these.
            raise ApiError(1000, msg, status, "auth/api_key/login", retryable=status >= 500)
        cookie, expires = _parse_cookie(headers)
        if not cookie:
            raise ApiError(1000, "login response had no gravity cookie", status, "auth/api_key/login", retryable=False)
        now = self._wall()
        self.cookie = cookie
        self.account_id = _header(headers, ACCOUNT_HEADER)
        self.issued_at = now
        self.expires_at = expires if expires and expires > now else now + DEFAULT_TTL
        self.generation += 1
        try:
            self.funding_account_address = codec.decode(resp).get("funding_account_address")
        except Exception:
            pass
        log.debug("grvt: logged in, session generation %d valid for %.0fs", self.generation, self.expires_at - now)


def _header(headers: Any, name: str) -> str | None:
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def _parse_cookie(headers: Any) -> tuple[str | None, float | None]:
    raw = (
        headers.getall("Set-Cookie", [])
        if hasattr(headers, "getall")
        else [v for k, v in headers.items() if k.lower() == "set-cookie"]
    )
    for line in raw:
        c = SimpleCookie()
        c.load(line)
        if COOKIE_NAME in c:
            m = c[COOKIE_NAME]
            exp = None
            if m["max-age"]:
                exp = time.time() + float(m["max-age"])
            elif m["expires"]:
                exp = parsedate_to_datetime(m["expires"]).timestamp()
            return m.value, exp
    return None, None
