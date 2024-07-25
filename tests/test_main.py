import asyncio

from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import get_err_msg
from pykit.res import Ok
from pykit.uuid import uuid4

from rxcat import (
    ConnArgs,
    PubList,
    PubOpts,
    ServerBus,
)
from tests.conftest import (
    Mock_1,
    Mock_2,
    MockConn,
)


async def test_pubsub(sbus: ServerBus):
    flag = False

    async def sub__mock_1(data: Mock_1):
        assert isinstance(data, Mock_1)
        assert data.num == 1
        nonlocal flag
        flag = True

    (await sbus.sub(Mock_1, sub__mock_1)).eject()
    (await sbus.pub(Mock_1(num=1))).eject()

    assert flag

async def test_data_static_indexes(sbus: ServerBus):
    codes = (await Code.get_regd_codes()).eject()
    assert codes[0] == "rxcat__reg"
    assert codes[1] == "rxcat__server_reg_data"
    assert codes[2] == "rxcat__welcome"

async def test_pubsub_err(sbus: ServerBus):
    flag = False

    async def sub__test(data: ValErr):
        assert type(data) is ValErr
        assert get_err_msg(data) == "hello"
        nonlocal flag
        flag = True

    (await sbus.sub(ValErr, sub__test)).eject()
    (await sbus.pub(ValErr("hello"))).eject()
    assert flag

async def test_pubr(sbus: ServerBus):
    async def sub__test(data: ValErr):
        assert type(data) is ValErr
        assert get_err_msg(data) == "hello"
        return Ok(Mock_1(num=1))

    (await sbus.sub(ValErr, sub__test)).eject()
    response = (await sbus.pubr(
        ValErr("hello"), PubOpts(pubr__timeout=1))).eject()
    assert type(response) is Mock_1
    assert response.num == 1

async def test_lsid_net(sbus: ServerBus):
    """
    Tests correctness of published back to net responses.
    """
    async def sub__test(data: Mock_1):
        return Ok(PubList([Mock_2(num=2), Mock_2(num=3)]))

    await sbus.sub(Mock_1, sub__test)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "data": {
            "num": 1
        }
    })
    await asyncio.sleep(0.1)
    count = 0
    while not conn.out_queue.empty():
        response = await asyncio.wait_for(conn.client__recv(), 1)
        response_data = response["data"]
        count += 1
        if count == 1:
            assert response_data["num"] == 2
        elif count == 2:
            assert response_data["num"] == 3
        else:
            raise AssertionError
    assert count == 2

    conn_task.cancel()
