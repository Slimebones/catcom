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
    ServerBus.register_rpc("update_email", update_email)
    ws = Ws()
    await server_bus.conn(ws)
    await ws.send_json(RpcReq(
        key="update_email:" + RandomUtils.makeid(), kwargs={}).model_dump())
    print(await ws.receive_json())
    assert 0
