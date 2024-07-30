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


async def test_subfn(sbus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def sub__f(body: Mock_1):
        assert sbus.get_ctx()["connsid"] == conn.sid

    await sbus.sub(sub__f)
    conn_task = asyncio.create_task(sbus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "body": {
            "num": 1
        }
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert \
        rmsg["bodycodeid"] == (await Code.get_regd_codeid_by_type(ok)).eject()
    conn_task.cancel()

async def test_rpc(sbus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(body: EmptyRpcArgs) -> Res[int]:
        assert sbus.get_ctx()["connsid"] == conn.sid
        return Ok(0)

    conn_task = asyncio.create_task(sbus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    ServerBus.reg_rpc(srpc__update_email).eject()
    rpc_token = uuid4()
    rpc_key = "update_email::" + rpc_token
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "body": {
            "key": rpc_key,
            "body": {"username": "test_username", "email": "test_email"}
        }
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["bodycodeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    conn_task.cancel()

async def test_sub_custom_ctx_manager():
    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(sub_ctxfn=get_mock_ctx_manager_for_msg))

    async def sub__f(body: Mock_1):
        assert rxcat_mock_ctx.get()["name"] == "hello"

    await sbus.sub(sub__f)
    await sbus.pubr(Mock_1(num=1))

async def test_rpc_custom_ctx_manager():
    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn)
        ],
        sub_ctxfn=get_mock_ctx_manager_for_msg))

    conn = MockConn(ConnArgs(core=None))
    async def srpc__update_email(body: EmptyRpcArgs) -> Res[int]:
        assert rxcat_mock_ctx.get()["name"] == "hello"
        return Ok(0)

    conn_task = asyncio.create_task(sbus.conn(conn))
    # recv welcome
    await asyncio.wait_for(conn.client__recv(), 1)
    ServerBus.reg_rpc(srpc__update_email).eject()
    rpc_token = uuid4()
    rpc_key = "update_email::" + rpc_token
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "body": {
            "key": rpc_key,
            "body": {}
        }
    })
    rmsg = await asyncio.wait_for(conn.client__recv(), 1)
    assert rmsg["bodycodeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    conn_task.cancel()
