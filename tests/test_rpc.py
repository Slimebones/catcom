import asyncio

from pykit.rand import RandomUtils
from pykit.res import Res
from result import Err, Ok
from rxcat import ServerBus, ServerBusCfg, Transport, Conn, ConnArgs, RpcReq
from tests.conftest import MockConn

async def test_rpc():
    async def update_email(data: dict) -> Res[int]:
        username = data["username"]
        email = data["email"]
        if username == "throw":
            return Err(Exception())
        assert username == "test_username"
        assert email == "test_email"
        return Ok(0)

    evt = asyncio.Event()
    async def on_send(connsid: str, rmsg: dict):
        evt.set()

    server_bus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                on_send=on_send)
        ])
    await server_bus.init(cfg)

    conn_1 = MockConn(ConnArgs(
        core=None))
    conn_task_1 = asyncio.create_task(server_bus.conn(conn_1))
    conn_2 = MockConn(ConnArgs(
        core=None))
    conn_task_2 = asyncio.create_task(server_bus.conn(conn_2))

    ServerBus.register_rpc("update_email", update_email)
    await server_bus.inner__accept_net_msg(RpcReq(
        key="update_email:" + RandomUtils.makeid(),
        kwargs={"username": "test_username", "email": "test_email"}))
    await asyncio.wait_for(evt.wait(), 1)
