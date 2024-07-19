import asyncio

from pykit.rand import RandomUtils
from pykit.res import Res, eject
from result import Ok

from rxcat import (
    ConnArgs,
    EmptyRpcArgs,
    OkEvt,
    ServerBus,
    ServerBusCfg,
    SrpcEvt,
    SrpcReq,
    Transport,
)
from rxcat._code import CodeStorage
from tests.conftest import (
    MockConn,
    MockReq_1,
    get_mock_ctx_manager_for_msg,
    rxcat_mock_ctx,
)


async def test_subfn(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def f(req: MockReq_1) -> Res[OkEvt]:
        assert server_bus.get_ctx()["connsid"] == conn.sid
        return Ok(OkEvt.create(req))

    await server_bus.sub(MockReq_1, f)
    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(MockReq_1)),
        "num": 1
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["mcodeid"] == eject(CodeStorage.get_mcodeid_for_mtype(OkEvt))
    conn_task.cancel()

async def test_rpc(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
        assert server_bus.get_ctx()["connsid"] == conn.sid
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    eject(ServerBus.register_rpc(srpc__update_email))
    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(SrpcReq)),
        "key": rpc_key,
        "args": {"username": "test_username", "email": "test_email"}
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["mcodeid"] == eject(CodeStorage.get_mcodeid_for_mtype(SrpcEvt))

    conn_task.cancel()

async def test_sub_custom_ctx_manager():
    server_bus = ServerBus.ie()
    await server_bus.init(ServerBusCfg(sub_ctxfn=get_mock_ctx_manager_for_msg))

    async def f(req: MockReq_1) -> Res[OkEvt]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(OkEvt.create(req))

    await server_bus.sub(MockReq_1, f)
    await server_bus.pubr(MockReq_1(num=1))

async def test_rpc_custom_ctx_manager():
    server_bus = ServerBus.ie()
    await server_bus.init(ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                server__register_process="none")
        ],
        sub_ctxfn=get_mock_ctx_manager_for_msg))

    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    eject(ServerBus.register_rpc(srpc__update_email))
    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(SrpcReq)),
        "key": rpc_key,
        "args": {}
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["mcodeid"] == eject(CodeStorage.get_mcodeid_for_mtype(SrpcEvt))

    conn_task.cancel()
