import asyncio

from ryz.code import Code
from ryz.res import Ok, Res
from ryz.uuid import uuid4

from tests.conftest import (
    Mock_1,
    MockCon,
    get_mock_ctx_manager_for_msg,
    yon_mock_ctx,
)
from yon import (
    ConArgs,
    EmptyRpcArgs,
    ServerBus,
    ServerBusCfg,
    SrpcRecv,
    SrpcSend,
    Transport,
    ok,
)


async def test_subfn(sbus: ServerBus):
    con = MockCon(ConArgs(core=None))
    async def sub_f(msg: Mock_1):
        assert sbus.get_ctx()["consid"] == con.sid

    await sbus.sub(sub_f)
    con_task = asyncio.create_task(sbus.con(con))
    # recv welcome
    await asyncio.wait_for(con.client__recv(), 1)
    await con.client__send({
        "sid": uuid4(),
        "codeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "msg": {
            "num": 1
        }
    })
    rbmsg = await asyncio.wait_for(con.client__recv(), 1)
    assert \
        rbmsg["codeid"] == (await Code.get_regd_codeid_by_type(ok)).eject()
    con_task.cancel()

async def test_rpc(sbus: ServerBus):
    con = MockCon(ConArgs(core=None))
    async def srpc_update_email(msg: EmptyRpcArgs) -> Res[int]:
        assert sbus.get_ctx()["consid"] == con.sid
        return Ok(0)

    con_task = asyncio.create_task(sbus.con(con))
    # recv welcome
    await asyncio.wait_for(con.client__recv(), 1)
    ServerBus.reg_rpc(srpc_update_email).eject()
    rpc_key = "update_email"
    await con.client__send({
        "sid": uuid4(),
        "codeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "msg": {
            "key": rpc_key,
            "data": {"username": "test_username", "email": "test_email"}
        }
    })
    rbmsg = await asyncio.wait_for(con.client__recv(), 1)
    assert rbmsg["codeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    con_task.cancel()

async def test_sub_custom_ctx_manager():
    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(sub_ctxfn=get_mock_ctx_manager_for_msg))

    async def sub_f(msg: Mock_1):
        assert yon_mock_ctx.get()["name"] == "hello"

    await sbus.sub(sub_f)
    await sbus.pubr(Mock_1(num=1))

async def test_rpc_custom_ctx_manager():
    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                con_type=MockCon)
        ],
        sub_ctxfn=get_mock_ctx_manager_for_msg))

    con = MockCon(ConArgs(core=None))
    async def srpc_update_email(msg: EmptyRpcArgs) -> Res[int]:
        assert yon_mock_ctx.get()["name"] == "hello"
        return Ok(0)

    con_task = asyncio.create_task(sbus.con(con))
    # recv welcome
    await asyncio.wait_for(con.client__recv(), 1)
    ServerBus.reg_rpc(srpc_update_email).eject()
    rpc_key = "update_email"
    await con.client__send({
        "sid": uuid4(),
        "codeid": (await Code.get_regd_codeid_by_type(SrpcSend)).eject(),
        "msg": {
            "key": rpc_key,
            "data": {}
        }
    })
    rbmsg = await asyncio.wait_for(con.client__recv(), 1)
    assert rbmsg["codeid"] == \
        (await Code.get_regd_codeid_by_type(SrpcRecv)).eject()

    con_task.cancel()
