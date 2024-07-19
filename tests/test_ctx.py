import asyncio

from pykit.rand import RandomUtils
from pykit.res import Res, eject
from result import Ok

from rxcat import ConnArgs, RpcReq, ServerBus
from rxcat._code import CodeStorage
from tests.conftest import MockConn, MockReq_1


async def test_subaction(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def f(req: MockReq_1):
        assert server_bus.get_ctx()["connsid"] == conn.sid

    await server_bus.sub(MockReq_1, f)
    conn_task = asyncio.create_task(server_bus.conn(conn))
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(MockReq_1)),
        "num": 1
    })
    conn_task.cancel()

async def test_rpc(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(data: dict) -> Res[int]:
        assert server_bus.get_ctx()["connsid"] == conn.sid
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    eject(ServerBus.register_rpc(srpc__update_email))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(RpcReq)),
        "key": rpc_key,
        "kwargs": {"username": "test_username", "email": "test_email"}
    })
    await asyncio.wait_for(conn.client__recv(), 1)

    conn_task.cancel()
