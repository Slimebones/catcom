import pytest
from fcode import code

from rxcat import Bus, Evt, Req


@code("rxcat-test.msg1")
class _Evt1(Evt):
    num: int


@code("rxcat-test.msg2")
class _Evt2(Evt):
    num: int


@code("rxcat-test.req1")
class _Req1(Req):
    num: int


@code("rxcat-test.req2")
class _Req2(Req):
    num: int


@pytest.mark.asyncio
async def test_inner_pubsub(bus: Bus):
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

    await bus.sub(_Evt1, on_msg1)
    await bus.sub(_Evt2, on_msg2)
    await bus.pub(_Evt1(num=1, rsid="__system__"))
    await bus.pub(_Evt2(num=2, rsid="__system__"))

    assert is_msg1_arrived
    assert is_msg2_arrived


@pytest.mark.asyncio
async def test_evt_serialization(bus: Bus) -> None:
    msg1_mcodeid = bus.try_get_mcodeid_for_mtype(_Evt1)
    assert msg1_mcodeid is not None

    m = _Evt1(num=1, rsid="__system__")

    sm = m.serialize_json(msg1_mcodeid)

    # shouldn't contain lmsid since it's None
    assert sm == {
        "msid": m.msid,
        "rsid": "__system__",
        "mcodeid": msg1_mcodeid,
        "num": m.num
    }

    m_after = _Evt1.deserialize_json(sm)

    assert m == m_after
