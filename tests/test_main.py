import asyncio
from typing import Any

from pydantic import BaseModel
from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import get_err_msg
from pykit.res import Err, Ok, valerr
from pykit.uuid import uuid4

from rxcat import (
    ConnArgs,
    InterruptPipeline,
    Mbody,
    PubList,
    PubOpts,
    ServerBus,
    ServerBusCfg,
    StaticCodeid,
    Transport,
    sub,
)
from tests.conftest import (
    EmptyMock,
    Mock_1,
    Mock_2,
    MockConn,
)


async def test_pubsub(sbus: ServerBus):
    flag = False

    async def sub__mock_1(body: Mock_1):
        assert isinstance(body, Mock_1)
        assert body.num == 1
        nonlocal flag
        flag = True

    (await sbus.sub(sub__mock_1)).eject()
    (await sbus.pub(Mock_1(num=1))).eject()

    assert flag

async def test_data_static_indexes(sbus: ServerBus):
    codes = (await Code.get_regd_codes()).eject()
    assert codes[0] == "rxcat::welcome"
    assert codes[1] == "rxcat::ok"

async def test_pubsub_err(sbus: ServerBus):
    flag = False

    async def sub__test(body: ValErr):
        assert type(body) is ValErr
        assert get_err_msg(body) == "hello"
        nonlocal flag
        flag = True

    (await sbus.sub(sub__test)).eject()
    (await sbus.pub(ValErr("hello"))).eject()
    assert flag

async def test_pubr(sbus: ServerBus):
    async def sub__test(body: ValErr):
        assert type(body) is ValErr
        assert get_err_msg(body) == "hello"
        return Ok(Mock_1(num=1))

    (await sbus.sub(sub__test)).eject()
    response = (await sbus.pubr(
        ValErr("hello"), PubOpts(pubr__timeout=1))).eject()
    assert type(response) is Mock_1
    assert response.num == 1

async def test_lsid_net(sbus: ServerBus):
    """
    Tests correctness of published back to net responses.
    """
    async def sub__test(body: Mock_1):
        return Ok(PubList([Mock_2(num=2), Mock_2(num=3)]))

    await sbus.sub(sub__test)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "body": {
            "num": 1
        }
    })
    await asyncio.sleep(0.1)
    count = 0
    while not conn.out_queue.empty():
        response = await asyncio.wait_for(conn.client__recv(), 1)
        response_data = response["body"]
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
    async def sub__test(body: EmptyMock):
        return

    await sbus.sub(sub__test)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok

    conn_task.cancel()

async def test_send_empty_data(sbus: ServerBus):
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    async def sub__test(body: Mock_1):
        return Ok(EmptyMock())

    await sbus.sub(sub__test)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert \
        response["bodycodeid"] \
        == (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    assert "data" not in response

    conn_task.cancel()

async def test_global_subfn_conditions():
    async def condition(data: Mbody) -> bool:
        return data.num == 0

    flag = False
    async def sub__test(body: Mock_1):
        # only mocks with num=0 should be passed
        assert body.num == 0
        nonlocal flag
        assert not flag
        flag = True

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        reg_types={Mock_1},
        global_subfn_conditions=[condition])
    await sbus.init(cfg)

    (await sbus.sub(sub__test)).eject()
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

    async def ifilter__auth(data: Mbody) -> Mbody:
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

    async def sub__login(body: Login):
        if body.username == "right":
            ServerBus.ie().set_ctx_conn_tokens(["right"])
            return None
        return valerr(f"wrong username {body.username}")

    async def sub__logout(body: Logout):
        ServerBus.ie().set_ctx_conn_tokens([])

    async def sub__mock_1(body: Mock_1):
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
    (await sbus.sub(sub__login)).eject()
    (await sbus.sub(sub__logout)).eject()
    (await sbus.sub(sub__mock_1)).eject()

    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(sbus.conn(conn))

    await asyncio.wait_for(conn.client__recv(), 1)
    mock_1_bodycodeid = (await Code.get_regd_codeid_by_type(Mock_1)).eject()
    valerr_bodycodeid = (await Code.get_regd_codeid_by_type(ValErr)).eject()
    login_bodycodeid = (await Code.get_regd_codeid_by_type(Login)).eject()
    logout_bodycodeid = (await Code.get_regd_codeid_by_type(Logout)).eject()

    # unregistered mock_1
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": mock_1_bodycodeid,
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == valerr_bodycodeid
    assert response["body"]["msg"] == "forbidden"

    # register wrong username
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": login_bodycodeid,
        "body": {
            "username": "wrong"
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == valerr_bodycodeid
    assert response["body"]["msg"] == "wrong username wrong"

    # register right username
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": login_bodycodeid,
        "body": {
            "username": "right"
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok
    assert "right" in conn.get_tokens(), "does not contain registered token"

    # registered mock_1
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": mock_1_bodycodeid,
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok

    # logout
    await conn.client__send({
        "sid": uuid4(),
        "bodycodeid": logout_bodycodeid
    })
    response = await asyncio.wait_for(conn.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok
    assert not conn.get_tokens()

    conn_task.cancel()

async def test_sub_decorator():
    class Mock(BaseModel):
        @staticmethod
        def code():
            return "mock"

    @sub
    def sub__t(body: Mock) -> Any:
        return

    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(reg_types={Mock}))
    assert Mock.code() in sbus._code_to_subfns  # noqa: SLF001
