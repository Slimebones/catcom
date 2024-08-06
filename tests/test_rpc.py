import asyncio
from typing import Any

from pydantic import BaseModel
from ryz.code import get_fqname
from ryz.err import ValErr
from ryz.res import Err, Ok, Res
from ryz.uuid import uuid4

from tests.conftest import (
    EmptyMock,
    MockConn,
    find_codeid_in_welcome_rmsg,
)
from yon import ConnArgs, ServerBus, srpc


async def test_main(sbus: ServerBus):
    class UpdEmailArgs(BaseModel):
        username: str
        email: str
    async def srpc__update_email(body: UpdEmailArgs) -> Res[int]:
        username = body.username
        email = body.email
        if username == "throw":
            return Err(ValErr("hello"))
        assert username == "test_username"
        assert email == "test_email"
        return Ok(0)

    conn_1 = MockConn(ConnArgs(
        core=None))
    conn_task_1 = asyncio.create_task(sbus.conn(conn_1))

    welcome_rmsg = await asyncio.wait_for(conn_1.client__recv(), 1)
    yon_rpc_req_bodycodeid = find_codeid_in_welcome_rmsg(
        "yon::srpc_send", welcome_rmsg).eject()

    ServerBus.reg_rpc(srpc__update_email).eject()

    rpc_key = "update_email"
    await conn_1.client__send({
        "sid": uuid4(),
        "bodycodeid": yon_rpc_req_bodycodeid,
        "body": {
            "key": rpc_key,
            "body": {"username": "test_username", "email": "test_email"}
        }
    })
    rpc_recv = await asyncio.wait_for(conn_1.client__recv(), 1)
    rpc_body = rpc_recv["body"]
    assert rpc_body == 0

    rpc_key = "update_email"
    send_msid = uuid4()
    await conn_1.client__send({
        "sid": send_msid,
        "bodycodeid": yon_rpc_req_bodycodeid,
        "body": {
            "key": rpc_key,
            "body": {"username": "throw", "email": "test_email"}
        }
    })
    rpc_recv = await asyncio.wait_for(conn_1.client__recv(), 1)
    assert rpc_recv["lsid"] == send_msid
    rpc_body = rpc_recv["body"]
    assert rpc_body["errcode"] == ValErr.code()
    assert rpc_body["msg"] == "hello"
    assert rpc_body["name"] == get_fqname(ValErr())

    conn_task_1.cancel()

async def test_srpc_decorator():
    @srpc
    async def srpc__test(body: EmptyMock) -> Res[Any]:
        return Ok(None)
    bus = ServerBus.ie()
    await bus.init()
    assert "test" in bus._rpckey_to_fn  # noqa: SLF001

async def test_reg_custom_rpc_key():
    async def srpc__test(body: EmptyMock) -> Res[Any]:
        return Ok(None)
    bus = ServerBus.ie()
    await bus.init()
    bus.reg_rpc(srpc__test, "whocares").eject()
    assert "whocares" in bus._rpckey_to_fn  # noqa: SLF001
