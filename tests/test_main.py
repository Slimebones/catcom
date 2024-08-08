import asyncio
from typing import Any

from pydantic import BaseModel
from ryz.code import Code
from ryz.err import ValErr
from ryz.err_utils import get_err_msg
from ryz.res import Err, Ok, valerr
from ryz.uuid import uuid4

from tests.conftest import (
    EmptyMock,
    Mock_1,
    Mock_2,
    MockCon,
)
from yon import (
    ConArgs,
    InterruptPipeline,
    Msg,
    PubList,
    PubOpts,
    ServerBus,
    ServerBusCfg,
    StaticCodeid,
    Transport,
    sub,
)


async def test_pubsub(sbus: ServerBus):
    flag = False

    async def sub__mock_1(msg: Mock_1):
        assert isinstance(msg, Mock_1)
        assert msg.num == 1
        nonlocal flag
        flag = True

    (await sbus.sub(sub__mock_1)).eject()
    (await sbus.pub(Mock_1(num=1))).eject()

    assert flag

async def test_data_static_indexes(sbus: ServerBus):
    codes = (await Code.get_regd_codes()).eject()
    assert codes[0] == "yon::welcome"
    assert codes[1] == "yon::ok"

async def test_pubsub_err(sbus: ServerBus):
    flag = False

    async def sub__test(msg: ValErr):
        assert type(msg) is ValErr
        assert get_err_msg(msg) == "hello"
        nonlocal flag
        flag = True

    (await sbus.sub(sub__test)).eject()
    (await sbus.pub(ValErr("hello"))).eject()
    assert flag

async def test_pubr(sbus: ServerBus):
    async def sub__test(msg: ValErr):
        assert type(msg) is ValErr
        assert get_err_msg(msg) == "hello"
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
    async def sub__test(msg: Mock_1):
        return Ok(PubList([Mock_2(num=2), Mock_2(num=3)]))

    await sbus.sub(sub__test)
    con = MockCon(ConArgs(core=None))
    con_task = asyncio.create_task(sbus.con(con))

    await asyncio.wait_for(con.client__recv(), 1)
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "body": {
            "num": 1
        }
    })
    await asyncio.sleep(0.1)
    count = 0
    while not con.out_queue.empty():
        response = await asyncio.wait_for(con.client__recv(), 1)
        response_data = response["body"]
        count += 1
        if count == 1:
            assert response_data["num"] == 2
        elif count == 2:
            assert response_data["num"] == 3
        else:
            raise AssertionError
    assert count == 2

    con_task.cancel()

async def test_recv_empty_data(sbus: ServerBus):
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    async def sub__test(msg: EmptyMock):
        return

    await sbus.sub(sub__test)
    con = MockCon(ConArgs(core=None))
    con_task = asyncio.create_task(sbus.con(con))

    await asyncio.wait_for(con.client__recv(), 1)
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok

    con_task.cancel()

async def test_send_empty_data(sbus: ServerBus):
    """
    Should validate empty data rmsg, or data set to None to empty base models
    """
    async def sub__test(msg: Mock_1):
        return Ok(EmptyMock())

    await sbus.sub(sub__test)
    con = MockCon(ConArgs(core=None))
    con_task = asyncio.create_task(sbus.con(con))

    await asyncio.wait_for(con.client__recv(), 1)
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": (await Code.get_regd_codeid_by_type(Mock_1)).eject(),
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert \
        response["bodycodeid"] \
        == (await Code.get_regd_codeid_by_type(EmptyMock)).eject()
    assert "data" not in response

    con_task.cancel()

async def test_global_subfn_conditions():
    async def condition(data: Msg) -> bool:
        return data.num == 0

    flag = False
    async def sub__test(msg: Mock_1):
        # only mocks with num=0 should be passed
        assert msg.num == 0
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

    async def ifilter__auth(data: Msg) -> Msg:
        sbus = ServerBus.ie()
        consid_res = sbus.get_ctx_consid()
        # skip inner messages
        if isinstance(consid_res, Err) or not consid_res.okval:
            return data

        consid = consid_res.okval
        tokens = sbus.get_con_tokens(consid).eject()
        # if data is mock_1, the con must have tokens
        if isinstance(data, Mock_1) and not tokens:
            return InterruptPipeline(ValErr("forbidden"))
        return data

    async def sub__login(msg: Login):
        if msg.username == "right":
            ServerBus.ie().set_ctx_con_tokens(["right"])
            return None
        return valerr(f"wrong username {msg.username}")

    async def sub__logout(msg: Logout):
        ServerBus.ie().set_ctx_con_tokens([])

    async def sub__mock_1(msg: Mock_1):
        return

    sbus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                con_type=MockCon)
        ],
        reg_types={Mock_1, Mock_2, Login, Logout},
        global_subfn_inp_filters={ifilter__auth}
    )
    await sbus.init(cfg)

    (await sbus.reg_types({Login, Logout})).eject()
    (await sbus.sub(sub__login)).eject()
    (await sbus.sub(sub__logout)).eject()
    (await sbus.sub(sub__mock_1)).eject()

    con = MockCon(ConArgs(core=None))
    con_task = asyncio.create_task(sbus.con(con))

    await asyncio.wait_for(con.client__recv(), 1)
    mock_1_bodycodeid = (await Code.get_regd_codeid_by_type(Mock_1)).eject()
    valerr_bodycodeid = (await Code.get_regd_codeid_by_type(ValErr)).eject()
    login_bodycodeid = (await Code.get_regd_codeid_by_type(Login)).eject()
    logout_bodycodeid = (await Code.get_regd_codeid_by_type(Logout)).eject()

    # unregistered mock_1
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": mock_1_bodycodeid,
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == valerr_bodycodeid
    assert response["body"]["msg"] == "forbidden"

    # register wrong username
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": login_bodycodeid,
        "body": {
            "username": "wrong"
        }
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == valerr_bodycodeid
    assert response["body"]["msg"] == "wrong username wrong"

    # register right username
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": login_bodycodeid,
        "body": {
            "username": "right"
        }
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok
    assert "right" in con.get_tokens(), "does not contain registered token"

    # registered mock_1
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": mock_1_bodycodeid,
        "body": {
            "num": 1
        }
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok

    # logout
    await con.client__send({
        "sid": uuid4(),
        "bodycodeid": logout_bodycodeid
    })
    response = await asyncio.wait_for(con.client__recv(), 1)
    assert response["bodycodeid"] == StaticCodeid.Ok
    assert not con.get_tokens()

    con_task.cancel()

async def test_sub_decorator():
    class Mock(BaseModel):
        @staticmethod
        def code():
            return "mock"

    @sub
    def sub__t(msg: Mock) -> Any:
        return

    sbus = ServerBus.ie()
    await sbus.init(ServerBusCfg(reg_types={Mock}))
    assert Mock.code() in sbus._code_to_subfns  # noqa: SLF001
