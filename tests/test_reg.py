import asyncio
from typing import Any

from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import create_err_dto
from pykit.res import Ok, Res, valerr
from pykit.uuid import uuid4

from rxcat import ConnArgs, RegErr, ServerBus, ServerBusCfg, ServerRegData, StaticCodeid, Transport
from tests.conftest import Mock_1, Mock_2, MockConn


async def test_main():
    async def reg_fn(
            connsid: str,
            tokens: list[str],
            client_data: dict[str, Any] | None):
        assert tokens == ["whocares_1", "whocares_2"]
        assert client_data == {"name": "mark"}
        return ServerRegData(data={"state": 12})

    flag = False
    async def sub__f(data: Mock_1):
        nonlocal flag
        flag = True

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                is_registration_enabled=True)
        ],
        reg_types=[
            Mock_1,
            Mock_2
        ],
        reg_fn=reg_fn)
    await asyncio.wait_for(sbus.init(cfg), 1)

    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": 0,
        "data": {
            "tokens": ["whocares_1", "whocares_2"],
            "data": {
                "name": "mark"
            }
        }
    })
    server_reg_evt = await asyncio.wait_for(conn.client__recv(), 1)
    assert server_reg_evt["datacodeid"] == 1
    assert server_reg_evt["data"]["data"] == {"state": 12}
    welcome = await asyncio.wait_for(conn.client__recv(), 1)
    assert welcome["datacodeid"] == StaticCodeid.Welcome

    (await sbus.sub(Mock_1, sub__f)).eject()
    (await sbus.pub(Mock_1(num=1))).eject()

    assert flag

    conn_task.cancel()

async def test_reject():
    async def reg_fn(
            connsid: str,
            tokens: list[str],
            client_data: dict[str, Any] | None):
        return RegErr("forbidden")

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                is_registration_enabled=True)
        ],
        reg_types=[
            Mock_1,
            Mock_2
        ],
        reg_fn=reg_fn)
    await asyncio.wait_for(sbus.init(cfg), 1)

    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": StaticCodeid.Reg,
        "data": {
            "tokens": ["whocares_1", "whocares_2"],
            "data": {
                "name": "mark"
            }
        }
    })
    reg_data_msg = await asyncio.wait_for(conn.client__recv(), 1)
    assert \
        reg_data_msg["datacodeid"] \
        == StaticCodeid.RegErr
    reg_data = reg_data_msg["data"]
    assert \
        reg_data \
        == (await create_err_dto(RegErr("forbidden"))) \
            .eject() \
            .model_dump(exclude={"stacktrace"})

    await asyncio.sleep(0.1)
    assert conn.is_closed()
    assert conn.out_queue.empty()

    conn_task.cancel()
