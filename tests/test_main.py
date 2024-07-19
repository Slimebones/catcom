from pykit.check import check
from pykit.err import ValueErr
from pykit.fcode import code
from pykit.res import Res, eject
from result import Err, Ok

from rxcat import ErrEvt, Evt, Req, ServerBus
from rxcat._code import CodeStorage
from tests.conftest import MockEvt_1, MockEvt_2, MockReq_1, MockReq_2


async def test_res_returning(server_bus: ServerBus):
    is_msg1_arrived = False
    is_msg2_arrived = False
    is_err_evt_arrived = False
    is_evt1_arrived = False

    async def ret_ok(msg) -> Res[MockEvt_1]:
        nonlocal is_msg1_arrived
        is_msg1_arrived = True
        return Ok(MockEvt_1(num=5, rsid=None))

    async def ret_err(msg):
        nonlocal is_msg2_arrived
        assert msg.msid
        is_msg2_arrived = True
        return Err(ValueErr("hello"))

    async def on_err_evt(evt: ErrEvt):
        nonlocal is_err_evt_arrived
        assert type(evt.inner__err) is ValueErr
        assert evt.err.msg == "hello"
        is_err_evt_arrived = True

    async def on_evt1(evt: MockEvt_1):
        nonlocal is_evt1_arrived
        assert isinstance(evt, MockEvt_1)
        assert evt.num == 5
        is_evt1_arrived = True

    await server_bus.sub(MockReq_1, ret_ok)
    await server_bus.sub(MockReq_2, ret_err)
    await server_bus.sub(ErrEvt, on_err_evt)
    await server_bus.sub(MockEvt_1, on_evt1)
    await server_bus.pub(MockReq_1(num=1))
    await server_bus.pub(MockReq_2(num=2))
    await check.aexpect(
        server_bus.pubr(MockReq_2(num=3)),
        ValueErr)

    assert is_msg1_arrived
    assert is_msg2_arrived
    assert is_err_evt_arrived
    assert is_evt1_arrived

async def test_inner_pubsub(server_bus: ServerBus):
    is_msg1_arrived = False
    is_msg2_arrived = False

    async def on_msg1(msg: MockEvt_1):
        nonlocal is_msg1_arrived
        assert msg.msid
        assert msg.num == 1
        is_msg1_arrived = True

    async def on_msg2(msg: MockEvt_2):
        nonlocal is_msg2_arrived
        assert msg.msid
        assert msg.num == 2
        is_msg2_arrived = True

    await server_bus.sub(MockEvt_1, on_msg1)
    await server_bus.sub(MockEvt_2, on_msg2)
    await server_bus.pub(MockEvt_1(num=1, rsid=None))
    await server_bus.pub(MockEvt_2(num=2, rsid=None))

    assert is_msg1_arrived
    assert is_msg2_arrived

async def test_evt_serialization(server_bus: ServerBus) -> None:
    msg1_mcodeid = eject(CodeStorage.get_mcodeid_for_mtype(MockEvt_1))

    m = MockEvt_1(num=1, rsid=None)

    sm = m.serialize_json(msg1_mcodeid)

    assert sm == {
        "msid": m.msid,
        "mcodeid": msg1_mcodeid,
        "num": m.num
    }

    m_after = MockEvt_1.deserialize_json(sm)

    assert m == m_after

def test_register_req_has_index_0(server_bus: ServerBus):
    assert CodeStorage.indexed_mcodes[0][0] == "rxcat_register_req"
