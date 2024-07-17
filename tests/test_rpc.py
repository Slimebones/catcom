import asyncio
from aiohttp.web import WebSocketResponse as Ws
from pykit.rand import RandomUtils
from pykit.res import Err, Ok, Res

from rxcat import RpcReq, ServerBus


async def test_rpc(server_bus: ServerBus):
    async def update_email(data: dict) -> Res[int]:
        username = data["username"]
        email = data["email"]
        if username == "throw":
            return Err(Exception())
        assert username == "test_username"
        assert email == "test_email"
        return Ok(0)
    async def on_send(connsids: set[str], rawmsg: dict):
        pass
    ServerBus.register_rpc("update_email", update_email)
    server_bus._cfg.on_send = on_send
    await server_bus.inner__accept_net_msg(RpcReq(
        key="update_email:" + RandomUtils.makeid(), kwargs={}))
