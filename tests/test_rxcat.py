import pytest
from pykit.fcode import code

from rxcat import Evt, Req, ServerBus


@code("msg1")
class _Evt1(Evt):
    num: int


@code("msg2")
class _Evt2(Evt):
    num: int

@code("req1")
class _Req1(Req):
    num: int

@code("req2")
class _Req2(Req):
    num: int

@pytest.mark.asyncio
async def test_inner_pubsub(server_bus: ServerBus):
    is_msg1_arrived = False
    is_msg2_arrived = False

    async def on_msg1(msg: _Evt1):
        nonlocal is_msg1_arrived
        assert msg.msid
        assert msg.num == 1
        is_msg1_arrived = True

    async def on_msg2(msg: _Evt2):
        nonlocal is_msg2_arrived
        assert msg.msid
        assert msg.num == 2
        is_msg2_arrived = True

    await server_bus.sub(_Evt1, on_msg1)
    await server_bus.sub(_Evt2, on_msg2)
    await server_bus.pub(_Evt1(num=1, rsid=None))
    await server_bus.pub(_Evt2(num=2, rsid=None))

    assert is_msg1_arrived
    assert is_msg2_arrived

@pytest.mark.asyncio
async def test_evt_serialization(server_bus: ServerBus) -> None:
    msg1_mcodeid = server_bus.try_get_mcodeid_for_mtype(_Evt1)
    assert msg1_mcodeid is not None

    m = _Evt1(num=1, rsid=None)

    sm = m.serialize_json(msg1_mcodeid)

    # shouldn't contain lmsid since it's None
    assert sm == {
        "msid": m.msid,
        "mcodeid": msg1_mcodeid,
        "num": m.num
    }

    m_after = _Evt1.deserialize_json(sm)

    assert m == m_after
