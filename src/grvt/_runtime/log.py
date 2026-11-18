"""`GRVT_LOG=debug` turns on wire logging to stderr. Secrets are always redacted.

Redacted: the `gravity` cookie, api keys, private keys, and signature r/s.
"""

from __future__ import annotations

import logging
import os
import re

log = logging.getLogger("grvt")

_JSON_SECRET = re.compile(rb'("(?:api_key|private_key|r|s|gravity|cookie)"\s*:\s*)"[^"]*"', re.IGNORECASE)
_COOKIE = re.compile(rb"(gravity=)[^;\s\"]+")
_HEX_KEY = re.compile(rb"\b0x[0-9a-fA-F]{64}\b")


def redact(data: bytes | str) -> str:
    b = data.encode() if isinstance(data, str) else data
    b = _JSON_SECRET.sub(rb'\1"***"', b)
    b = _COOKIE.sub(rb"\1***", b)
    b = _HEX_KEY.sub(b"0x***", b)
    return b.decode("utf-8", "replace")


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in headers.items():
        lk = k.lower()
        out[k] = "***" if lk in ("cookie", "set-cookie", "authorization") else v
    return out


def configure_from_env() -> None:
    level = os.environ.get("GRVT_LOG", "").lower()
    if not level:
        return
    log.setLevel(logging.DEBUG if level == "debug" else getattr(logging, level.upper(), logging.INFO))
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s grvt %(levelname)s %(message)s"))
        log.addHandler(h)


def debug_enabled() -> bool:
    return log.isEnabledFor(logging.DEBUG)
