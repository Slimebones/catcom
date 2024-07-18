import asyncio

from pykit.rand import RandomUtils
from pykit.res import Res
from result import Err, Ok

from rxcat import ConnArgs, ServerBus, ServerBusCfg, Transport
from tests.conftest import MockConn


async def test_rpc():
    async def update_email(data: dict) -> Res[int]:
        username = data["username"]
        email = data["email"]
        if username == "throw":
            return Err(Exception("hello"))
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
                on_send=on_send,
                server__register_process="none")
        ])
    await server_bus.init(cfg)

    conn_1 = MockConn(ConnArgs(
        core=None))
    conn_task_1 = asyncio.create_task(server_bus.conn(conn_1))

    welcome_rmsg = await asyncio.wait_for(conn_1.client__recv(), 1)
    rxcat_rpc_req_mcodeid = -1
    for i, code_container in enumerate(welcome_rmsg["indexedMcodes"]):
        if "rxcat_rpc_req" in code_container:
            rxcat_rpc_req_mcodeid = i
            break
    assert rxcat_rpc_req_mcodeid >= 0

    ServerBus.register_rpc("update_email", update_email)

    rpc_token = RandomUtils.makeid()
    rpc_key = "update_email:" + rpc_token
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
    rpc_key = "update_email:" + rpc_token
    await conn_1.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": rxcat_rpc_req_mcodeid,
        "key": rpc_key,
        "kwargs": {"username": "throw", "email": "test_email"}
    })
    rpc_data = await asyncio.wait_for(conn_1.client__recv(), 1)
    assert rpc_data["key"] == rpc_key
    assert rpc_data["val"] == 0

    conn_task_1.cancel()
