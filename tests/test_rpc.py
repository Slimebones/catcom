import asyncio
from typing import Any

from pydantic import BaseModel
from pykit.code import get_fqname
from pykit.err import ValErr
from pykit.res import Err, Ok, Res
from pykit.uuid import uuid4

from rxcat import ConnArgs, ServerBus, srpc
from tests.conftest import (
    EmptyMock,
    MockConn,
    find_codeid_in_welcome_rmsg,
)


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
    rxcat_rpc_req_bodycodeid = find_codeid_in_welcome_rmsg(
        "rxcat::srpc_send", welcome_rmsg).eject()

    ServerBus.reg_rpc(srpc__update_email).eject()

    rpc_key = "update_email"
    await conn_1.client__send({
        "sid": uuid4(),
        "bodycodeid": rxcat_rpc_req_bodycodeid,
        "body": {
            "key": rpc_key,
            "body": {"username": "test_username", "email": "test_email"}
        }
    })
    rpc_recv = await asyncio.wait_for(conn_1.client__recv(), 1)
    rpc_data = rpc_recv["body"]
    assert rpc_data["key"] == rpc_key
    assert rpc_data["val"] == 0

    rpc_key = "update_email"
    await conn_1.client__send({
        "sid": uuid4(),
        "bodycodeid": rxcat_rpc_req_bodycodeid,
        "body": {
            "key": rpc_key,
            "body": {"username": "throw", "email": "test_email"}
        }
    })
    rpc_recv = await asyncio.wait_for(conn_1.client__recv(), 1)
    rpc_data = rpc_recv["body"]
    assert rpc_data["key"] == rpc_key
    val = rpc_data["val"]
    assert rpc_data["val"]["errcode"] == ValErr.code()
    assert val["msg"] == "hello"
    assert val["name"] == get_fqname(ValErr())

    conn_task_1.cancel()

async def test_srpc_decorator():
    @srpc
    async def srpc__test(body: EmptyMock) -> Res[Any]:
        return Ok(None)
    sbus = ServerBus.ie()
    await sbus.init()
    assert "test" in sbus._rpckey_to_fn  # noqa: SLF001
