import asyncio

from pykit.err import ValErr
from pykit.obj import get_fully_qualified_name
from pykit.rand import RandomUtils
from pykit.res import Res, eject
from result import Err, Ok

from rxcat import ConnArgs, ServerBus
from tests.conftest import (
    MockConn,
    find_errcodeid_in_welcome_rmsg,
    find_mcodeid_in_welcome_rmsg,
)


async def test_main(server_bus: ServerBus):
    async def srpc__update_email(data: dict) -> Res[int]:
        username = data["username"]
        email = data["email"]
        if username == "throw":
            return Err(ValErr("hello"))
        assert username == "test_username"
        assert email == "test_email"
        return Ok(0)

    conn_1 = MockConn(ConnArgs(
        core=None))
    conn_task_1 = asyncio.create_task(server_bus.conn(conn_1))

    welcome_rmsg = await asyncio.wait_for(conn_1.client__recv(), 1)
    rxcat_rpc_req_mcodeid = eject(find_mcodeid_in_welcome_rmsg(
        "rxcat_rpc_req", welcome_rmsg))

    eject(ServerBus.register_rpc(srpc__update_email))

    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn_1.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": rxcat_rpc_req_mcodeid,
        "key": rpc_key,
        "kwargs": {"username": "test_username", "email": "test_email"}
    })
    rpc_data = await asyncio.wait_for(conn_1.client__recv(), 1)
    assert rpc_data["key"] == rpc_key
    assert rpc_data["val"] == 0

    rpc_token = RandomUtils.makeid()
    rpc_key = "srpc__update_email:" + rpc_token
    await conn_1.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": rxcat_rpc_req_mcodeid,
        "key": rpc_key,
        "kwargs": {"username": "throw", "email": "test_email"}
    })
    rpc_data = await asyncio.wait_for(conn_1.client__recv(), 1)
    assert rpc_data["key"] == rpc_key
    val = rpc_data["val"]
    assert val["codeid"] == eject(
        find_errcodeid_in_welcome_rmsg("val-err", welcome_rmsg))
    assert val["msg"] == "hello"
    assert val["name"] == get_fully_qualified_name(ValErr())

    conn_task_1.cancel()
