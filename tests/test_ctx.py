import asyncio

from pykit.rand import RandomUtils
from pykit.res import Res, eject
import pytest
from result import Ok

from rxcat import ConnArgs, EmptyRpcArgs, ServerBus, ServerBusCfg, SrpcReq, OkEvt
from rxcat._code import CodeStorage
from tests.conftest import MockConn, MockCtxManager, MockReq_1, get_mock_ctx_manager_for_msg, rxcat_mock_ctx


async def test_subfn(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def f(req: MockReq_1) -> Res[OkEvt]:
        assert server_bus.get_ctx()["connsid"] == conn.sid
        return Ok(OkEvt.create(req))

    await server_bus.sub(MockReq_1, f)
    conn_task = asyncio.create_task(server_bus.conn(conn))
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(MockReq_1)),
        "num": 1
    })
    await asyncio.wait_for(conn.client__recv(), 1)
    conn_task.cancel()

async def test_rpc(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
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
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(SrpcReq)),
        "key": rpc_key,
        "args": {"username": "test_username", "email": "test_email"}
    })
    await asyncio.wait_for(conn.client__recv(), 1)

    conn_task.cancel()

@pytest.mark.parametrize(
        "server_bus",
        [ServerBusCfg(sub_ctxfn=get_mock_ctx_manager_for_msg)],
        indirect=True)
async def test_sub_custom_ctx_manager(server_bus: ServerBus):
    async def f(req: MockReq_1) -> Res[OkEvt]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(OkEvt.create(req))

    await server_bus.sub(MockReq_1, f)
    await server_bus.pubr(MockReq_1(num=1))

@pytest.mark.parametrize(
        "server_bus",
        [ServerBusCfg(rpc_ctxfn=get_mock_ctx_manager_for_msg)],
        indirect=True)
async def test_rpc_custom_ctx_manager(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    eject(ServerBus.register_rpc(srpc__update_email))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(SrpcReq)),
        "key": rpc_key,
        "args": {}
    })
    await asyncio.wait_for(conn.client__recv(), 1)

    conn_task.cancel()
