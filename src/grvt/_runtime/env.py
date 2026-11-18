"""Where the SDK connects and which chain id it signs for.

Pass a named env ("testnet", "prod") or your own Environment (e.g. a local gateway or a test server).
"""

from __future__ import annotations

from dataclasses import dataclass

_FULL_WS_PATH = "/ws/full"
_LITE_WS_PATH = "/ws/lite"


@dataclass(frozen=True, slots=True)
class Environment:
    name: str
    md_url: str  # market data gateway, REST
    td_url: str  # trade data gateway, REST
    edge_url: str  # edge: login, builder, deposit/withdrawal
    md_ws_url: str
    td_ws_url: str
    chain_id: int  # EIP-712 domain chainId
    # Largest WS frame the gateway accepts (websocket ReadLimitSize). The library default is
    # 1024 bytes; the deployed value is not published yet, so None = do not check.
    ws_max_frame_bytes: int | None = None
    # The same WebSockets with lite (short) field names, used by a client with encoding="lite". Unset, each is
    # the full URL with its ending /ws/full swapped for /ws/lite; set one only when that rule does not fit.
    md_ws_lite_url: str | None = None
    td_ws_lite_url: str | None = None

    def rest_url(self, gateway: str, path: str) -> str:
        base = {"md": self.md_url, "td": self.td_url, "edge": self.edge_url}[gateway]
        return base + path

    def ws_url(self, gateway: str, *, lite: bool = False) -> str:
        url = {"md": self.md_ws_url, "td": self.td_ws_url}[gateway]
        if not lite:
            return url
        field = f"{gateway}_ws_lite_url"
        explicit = getattr(self, field)
        if explicit and explicit.strip():
            return explicit
        if not url.endswith(_FULL_WS_PATH):
            raise ValueError(
                f"environment {self.name!r}: {url} does not end in {_FULL_WS_PATH}, so it has no lite URL; set {field}"
            )
        return url[: -len(_FULL_WS_PATH)] + _LITE_WS_PATH


def _hosted(name: str, domain: str, chain_id: int) -> Environment:
    return Environment(
        name=name,
        md_url=f"https://market-data.{domain}",
        td_url=f"https://trades.{domain}",
        edge_url=f"https://edge.{domain}",
        md_ws_url=f"wss://market-data.{domain}{_FULL_WS_PATH}",
        td_ws_url=f"wss://trades.{domain}{_FULL_WS_PATH}",
        md_ws_lite_url=f"wss://market-data.{domain}{_LITE_WS_PATH}",
        td_ws_lite_url=f"wss://trades.{domain}{_LITE_WS_PATH}",
        chain_id=chain_id,
    )


TESTNET = _hosted("testnet", "testnet.grvt.io", 326)
PROD = _hosted("prod", "grvt.io", 325)

ENVIRONMENTS = {"testnet": TESTNET, "prod": PROD}


def resolve_env(env: str | Environment, encoding: str = "lite") -> Environment:
    if isinstance(env, Environment):
        validate_env(env, encoding)
        return env
    try:
        return ENVIRONMENTS[env]
    except KeyError:
        raise ValueError(f"unknown env {env!r}; use one of {sorted(ENVIRONMENTS)} or pass an Environment") from None


def validate_env(env: Environment, encoding: str = "lite") -> None:
    """Raise ValueError when a custom Environment misses a URL or the chain id.

    Every field is used as given: nothing falls back to a named environment, so an empty URL or a chain id that
    is not a positive integer is an error, not a default. For lite, it also raises when a lite WebSocket URL can't
    be derived (see Environment.ws_url).
    """
    urls = ("md_url", "td_url", "edge_url", "md_ws_url", "td_ws_url")
    missing = [f for f in urls if not str(getattr(env, f) or "").strip()]
    if not env.chain_id or env.chain_id <= 0:
        missing.append("chain_id")
    if missing:
        raise ValueError(
            f"environment {env.name!r} is missing {', '.join(missing)}: a custom Environment must set every URL "
            "and the chain id"
        )
    if encoding == "lite":
        for gateway in ("md", "td"):
            env.ws_url(gateway, lite=True)
