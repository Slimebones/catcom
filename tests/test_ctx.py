import asyncio

from pykit.code import Code
from pykit.res import Ok, Res
from pykit.uuid import uuid4

from rxcat import (
    ConnArgs,
    EmptyRpcArgs,
    ServerBus,
    ServerBusCfg,
    SrpcRecv,
    SrpcSend,
    Transport,
    ok,
)
from tests.conftest import (
    Mock_1,
    MockConn,
    get_mock_ctx_manager_for_msg,
    rxcat_mock_ctx,
)


async def test_subfn(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def f(data: Mock_1):
        assert server_bus.get_ctx()["connsid"] == conn.sid

    await server_bus.sub(Mock_1, f)
    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "data": {
            "num": 1
        }
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert \
        rmsg["datacodeid"] == (await Code.get_regd_codeid_by_type(ok)).eject()
    conn_task.cancel()

async def test_rpc(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
        assert server_bus.get_ctx()["connsid"] == conn.sid
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    ServerBus.reg_rpc(srpc__update_email).eject()
    rpc_token = uuid4()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "key": rpc_key,
        "args": {"username": "test_username", "email": "test_email"}
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["datacodeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    conn_task.cancel()

async def test_sub_custom_ctx_manager():
    server_bus = ServerBus.ie()
    await server_bus.init(ServerBusCfg(sub_ctxfn=get_mock_ctx_manager_for_msg))

    async def f(data: Mock_1):
        assert rxcat_mock_ctx.get()["name"] == "hello"

    await server_bus.sub(Mock_1, f)
    await server_bus.pubr(Mock_1(num=1))

async def test_rpc_custom_ctx_manager():
    server_bus = ServerBus.ie()
    await server_bus.init(ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                server__reg_process="none")
        ],
        sub_ctxfn=get_mock_ctx_manager_for_msg))

    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(args: EmptyRpcArgs) -> Res[int]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(0)

    conn_task = asyncio.create_task(server_bus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    ServerBus.reg_rpc(srpc__update_email).eject()
    rpc_token = uuid4()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "key": rpc_key,
        "args": {}
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["datacodeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    conn_task.cancel()
