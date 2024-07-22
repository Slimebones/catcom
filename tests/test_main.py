from pykit.check import check
from pykit.code import get_fqname
from pykit.err import ValErr
from pykit.err_utils import ErrDto
from pykit.res import Res
from pykit.res import Err, Ok

from rxcat import ServerBus
from tests.conftest import Mock_1, Mock_2


async def test_subpub(server_bus: ServerBus):
    flag1 = False
    flag2 = False

    async def sub__mock_1(data: Mock_1):
        assert isinstance(data, Mock_1)
        assert data.num == 1
        nonlocal flag1
        flag1 = True

    async def sub__mock_2(data: Mock_2):
        assert isinstance(data, Mock_2)
        assert data.num == 2
        nonlocal flag1
        flag1 = True

    async def sub__err(data: ErrDto):
        assert data.name == get_fqname(ValErr)
        assert data.msg == "hello"
        nonlocal flag2
        flag2 = True

    await server_bus.sub(Mock_1, sub__mock_1)
    await server_bus.sub(Mock_2, sub__mock_2)
    await server_bus.sub(ErrDto, sub__err)
    await server_bus.pub(Mock_1(num=1))
    await server_bus.pub(ValErr("hello"))
    await check.aexpect(
        server_bus.pubr(Mock_2(num=2)),
        ValErr)

    assert flag1
    assert flag2

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
    await server_bus.pub(MockEvt_1(num=1, lsid=None))
    await server_bus.pub(MockEvt_2(num=2, lsid=None))

    assert is_msg1_arrived
    assert is_msg2_arrived

async def test_evt_serialization(server_bus: ServerBus) -> None:
    msg1_datacodeid = eject(CodeStorage.get_datacodeid_for_mtype(MockEvt_1))

    m = MockEvt_1(num=1, lsid=None)

    sm = m.serialize_for_net(msg1_datacodeid)

    assert sm == {
        "msid": m.msid,
        "datacodeid": msg1_datacodeid,
        "num": m.num
    }

    m_after = MockEvt_1.deserialize_from_net(sm)

    assert m == m_after

def test_register_req_has_index_0(server_bus: ServerBus):
    assert CodeStorage.indexed_mcodes[0][0] == "rxcat__register_req"
