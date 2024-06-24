from pykit.check import check
from pykit.err import ValueErr
from pykit.fcode import code
from pykit.res import Res
from result import Err, Ok

from rxcat import ErrEvt, Evt, Req, ServerBus


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

async def test_res_returning(server_bus: ServerBus):
    is_msg1_arrived = False
    is_msg2_arrived = False
    is_err_evt_arrived = False
    is_evt1_arrived = False

    async def ret_ok(msg) -> Res[_Evt1]:
        nonlocal is_msg1_arrived
        is_msg1_arrived = True
        return Ok(_Evt1(num=5, rsid=None))

    async def ret_err(msg):
        nonlocal is_msg2_arrived
        assert msg.msid
        is_msg2_arrived = True
        return Err(ValueErr("hello"))

    async def on_err_evt(evt: ErrEvt):
        nonlocal is_err_evt_arrived
        assert type(evt.inner_err) is ValueErr
        assert evt.errmsg == "hello"
        is_err_evt_arrived = True

    async def on_evt1(evt: _Evt1):
        nonlocal is_evt1_arrived
        assert isinstance(evt, _Evt1)
        assert evt.num == 5
        is_evt1_arrived = True

    await server_bus.sub(_Req1, ret_ok)
    await server_bus.sub(_Req2, ret_err)
    await server_bus.sub(ErrEvt, on_err_evt)
    await server_bus.sub(_Evt1, on_evt1)
    await server_bus.pub(_Req1(num=1))
    await server_bus.pub(_Req2(num=2))
    await check.aexpect(
        server_bus.pubr(_Req2(num=3)),
        ValueErr)

    assert is_msg1_arrived
    assert is_msg2_arrived
    assert is_err_evt_arrived
    assert is_evt1_arrived

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

async def test_evt_serialization(server_bus: ServerBus) -> None:
    msg1_mcodeid = server_bus.try_get_mcodeid_for_mtype(_Evt1)
    assert msg1_mcodeid is not None

    m = _Evt1(num=1, rsid=None)

    sm = m.serialize_json(msg1_mcodeid)

    assert sm == {
        "msid": m.msid,
        "mcodeid": msg1_mcodeid,
        "num": m.num
    }

    m_after = _Evt1.deserialize_json(sm)

    assert m == m_after
