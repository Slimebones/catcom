import asyncio

from pydantic import BaseModel
from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import get_err_msg
from pykit.res import Ok, Err, valerr
from pykit.uuid import uuid4

from rxcat import ConnArgs, InterruptPipeline, PubList, PubOpts, ServerBus, ServerBusCfg, StaticCodeid, Mdata, Transport
from tests.conftest import (
    EmptyMock,
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
    assert codes[0] == "rxcat__welcome"
    assert codes[1] == "rxcat__ok"

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

async def test_recv_empty_data(sbus: ServerBus):
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    async def sub__test(data: EmptyMock):
        return

    await sbus.sub(EmptyMock, sub__test)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == StaticCodeid.Ok

    conn_task.cancel()

async def test_send_empty_data(sbus: ServerBus):
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    async def sub__test(data: Mock_1):
        return Ok(EmptyMock())

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
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert \
        response["datacodeid"] \
        == (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    assert "data" not in response

    conn_task.cancel()

async def test_global_subfn_conditions():
    async def condition(data: Mdata) -> bool:
        return data.num == 0

    flag = False
    async def sub__test(data: Mock_1):
        # only mocks with num=0 should be passed
        assert data.num == 0
        nonlocal flag
        assert not flag
        flag = True

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        reg_types={Mock_1},
        global_subfn_conditions=[condition])
    await sbus.init(cfg)

    (await sbus.sub(Mock_1, sub__test)).eject()
    (await sbus.pub(Mock_1(num=1))).eject()

async def test_auth_example():
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    class Login(BaseModel):
        username: str

        @staticmethod
        def code():
            return "login"

    class Logout(BaseModel):
        @staticmethod
        def code():
            return "logout"

    async def ifilter__auth(data: Mdata) -> Mdata:
        sbus = ServerBus.ie()
        connsid_res = sbus.get_ctx_connsid()
        # skip inner messages
        if isinstance(connsid_res, Err) or not connsid_res.okval:
            return data

        connsid = connsid_res.okval
        tokens = sbus.get_conn_tokens(connsid).eject()
        # if data is mock_1, the conn must have tokens
        if isinstance(data, Mock_1) and not tokens:
            return InterruptPipeline(ValErr("forbidden"))
        return data

    async def sub__login(data: Login):
        if data.username == "right":
            ServerBus.ie().set_ctx_conn_tokens(["right"])
            return
        return valerr(f"wrong username {data.username}")

    async def sub__logout(data: Logout):
        ServerBus.ie().set_ctx_conn_tokens([])

    async def sub__mock_1(data: Mock_1):
        return

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn)
        ],
        reg_types={Mock_1, Mock_2, Login, Logout},
        global_subfn_inp_filters={ifilter__auth}
    )
    await sbus.init(cfg)

    (await sbus.reg_types({Login, Logout})).eject()
    (await sbus.sub(Login, sub__login)).eject()
    (await sbus.sub(Logout, sub__logout)).eject()
    (await sbus.sub(Mock_1, sub__mock_1)).eject()

    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    mock_1_datacodeid = (await Code.get_regd_codeid_by_type(Mock_1)).eject()
    valerr_datacodeid = (await Code.get_regd_codeid_by_type(ValErr)).eject()
    login_datacodeid = (await Code.get_regd_codeid_by_type(Login)).eject()
    logout_datacodeid = (await Code.get_regd_codeid_by_type(Logout)).eject()

    # unregistered mock_1
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": mock_1_datacodeid,
        "data": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == valerr_datacodeid
    assert response["data"]["msg"] == "forbidden"

    # register wrong username
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": login_datacodeid,
        "data": {
            "username": "wrong"
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == valerr_datacodeid
    assert response["data"]["msg"] == "wrong username wrong"

    # register right username
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": login_datacodeid,
        "data": {
            "username": "right"
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == StaticCodeid.Ok
    assert "right" in conn.get_tokens(), "does not contain registered token"

    # registered mock_1
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": mock_1_datacodeid,
        "data": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == StaticCodeid.Ok

    # logout
    await conn.client__send({
        "sid": uuid4(),
        "datacodeid": logout_datacodeid
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["datacodeid"] == StaticCodeid.Ok
    assert not conn.get_tokens()

    conn_task.cancel()
