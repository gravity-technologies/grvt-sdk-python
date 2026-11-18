"""A redirect must stop before credentials or a write can reach its target."""

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from grvt._runtime import ApiError, Runtime
from grvt._runtime.hooks import Hooks
from grvt._runtime.ws import WsSocket


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("cross_origin", [False, True])
async def test_ws_redirect_never_sends_second_handshake(gateway, status, cross_origin):
    received = []
    original = []

    async def target(request):
        received.append((dict(request.headers), await request.read()))
        return web.Response()

    target_app = web.Application()
    target_app.router.add_route("*", "/target", target)
    async with TestServer(target_app) as target_server:

        async def redirect(request):
            original.append(dict(request.headers))
            location = str(target_server.make_url("/target")) if cross_origin else "/target"
            return web.Response(status=status, headers={"Location": location})

        app = web.Application()
        app.router.add_get("/ws", redirect)
        app.router.add_route("*", "/target", target)
        async with TestServer(app) as server, Runtime(gateway.env, encoding="full", api_key="test-key") as rt:
            await rt.session.ensure()
            socket = WsSocket("td", str(server.make_url("/ws")), rt.session, Hooks())
            try:
                with pytest.raises(ApiError, match="redirects are disabled"):
                    await socket._connect()
            finally:
                await socket.close()
            assert len(original) == 1
            assert "Cookie" in original[0]
            assert "X-Grvt-Account-Id" in original[0]
            assert received == []
            assert gateway.received == []  # no signed frame was sent after the handshake failed
