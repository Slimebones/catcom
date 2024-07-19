import asyncio

from pykit.rand import RandomUtils
from rxcat import ServerBus, Req, ConnArgs
from pykit.res import eject
from rxcat._code import CodeStorage
from pykit.fcode import FcodeCore, code
from tests.conftest import MockConn, MockReq_1


async def test_subaction(server_bus: ServerBus):
    conn = MockConn(ConnArgs(core=None))
    async def f(req: MockReq_1):
        assert server_bus.get_ctx()["connsid"] == conn.sid

    await server_bus.sub(MockReq_1, f)
    conn_task = asyncio.create_task(server_bus.conn(conn))
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": eject(CodeStorage.get_mcodeid_for_mtype(MockReq_1)),
        "num": 1
    })
    conn_task.cancel()
