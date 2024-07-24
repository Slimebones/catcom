import asyncio
from pykit.code import Code
from pykit.err import ValErr
from pykit.res import Ok
from pykit.err_utils import get_err_msg
from pykit.uuid import uuid4

from rxcat import ServerBus, ConnArgs
from tests.conftest import Mock_1, Mock_2, MockConn, find_codeid_in_welcome_rmsg


async def test_pubsub(server_bus: ServerBus):
    flag = False

    async def sub__mock_1(data: Mock_1):
        assert isinstance(data, Mock_1)
        assert data.num == 1
        nonlocal flag
        flag = True

    (await server_bus.sub(Mock_1, sub__mock_1)).eject()
    (await server_bus.pub(Mock_1(num=1))).eject()

    assert flag

async def test_reg_req_has_index_0(server_bus: ServerBus):
    assert \
        (await Code.get_regd_codes()).eject()[0] == "rxcat__reg"

async def test_pubsub_err(server_bus: ServerBus):
    flag = False

    async def sub__test(data: ValErr):
        assert type(data) is ValErr
        assert get_err_msg(data) == "hello"
        nonlocal flag
        flag = True

    (await server_bus.sub(ValErr, sub__test)).eject()
    (await server_bus.pub(ValErr("hello"))).eject()
    assert flag

async def test_pubr(server_bus: ServerBus):
    async def sub__test(data: ValErr):
        assert type(data) is ValErr
        assert get_err_msg(data) == "hello"
        return Ok(Mock_1(num=1))

    (await server_bus.sub(ValErr, sub__test)).eject()
    response = (await server_bus.pubr(ValErr("hello"))).eject()
    assert type(response) is Mock_1
    assert response.num == 1

async def test_lsid_net(server_bus: ServerBus):
    """
    Tests correctness of published back to net responses.
    """
    async def sub__test(data: Mock_1):
        print(1)
        return Ok([Mock_2(num=2), Mock_2(num=3)])

    await server_bus.sub(Mock_1, sub__test)
    conn = MockConn(ConnArgs(
        core=None))
    conn_task = asyncio.create_task(server_bus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": Mock_1.code(),
        "data": {
            "num": 1
        }
    })
    count = 0
    while not conn.out_queue.empty():
        response = await asyncio.wait_for(conn.client__recv(), 1)
        assert type(response) is Mock_2
        count += 1
        if count == 1:
            assert response.num == 2
        elif count == 2:
            assert response.num == 3
        else:
            raise AssertionError
    assert count == 2
