import asyncio

import pytest
from orwynn.testing import Client

from src.fcode import code
from src.rxcat import Bus, Evt, InitdClientEvt, Req


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
    await bus.pub(_Evt1(num=1))
    await bus.pub(_Evt2(num=2))

    assert is_msg1_arrived
    assert is_msg2_arrived


@pytest.mark.asyncio
async def test_evt_serialization(bus: Bus) -> None:
    msg1_mcodeid = bus.try_get_mcodeid_for_mtype(_Evt1)
    assert msg1_mcodeid is not None

    m = _Evt1(num=1)

    sm = m.serialize_json(msg1_mcodeid)

    # shouldn't contain lmsid since it's None
    assert sm == {
        "msid": m.msid,
        "mcodeid": msg1_mcodeid,
        "num": m.num
    }

    m_after = _Evt1.deserialize_json(sm)

    assert m == m_after


@pytest.mark.asyncio
async def test_net_conn_and_pub(client: Client, bus: Bus):
    arrived_msg = None

    async def on_msg1(msg1: _Evt1):
        nonlocal arrived_msg
        arrived_msg = msg1

    await bus.sub(_Evt1, on_msg1)

    with client.websocket("/rxcat") as ws:
        m = _Evt1(num=1)
        data: dict = ws.receive_json()

        mcode = bus.try_get_mcode_for_mcodeid(data["mcodeid"])
        assert mcode == "pyrxcat.initd-client-evt"

        initd_evt = InitdClientEvt.deserialize_json(data)

        assert initd_evt.indexedMcodes
        assert initd_evt.indexedErrcodes

        m_mcodeid = bus.try_get_mcodeid_for_mtype(type(m))
        assert m_mcodeid is not None
        ws.send_json(m.serialize_json(m_mcodeid))

        # wait for arrival
        await asyncio.sleep(0.0001)
        assert arrived_msg == m

