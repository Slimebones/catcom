import asyncio

from pykit.rand import RandomUtils
from rxcat import ServerBus, Req, ConnArgs
from rxcat._code import CodeStorage
from pykit.fcode import code
from tests.conftest import MockConn


async def test_subaction(server_bus: ServerBus):
    @code("test_ctx_test_subaction_req")
    class TestReq(Req):
        pass

    async def f(req: TestReq):
        print(server_bus.get_ctx())
        from rxcat import _rxcat_ctx
        print(_rxcat_ctx.get())

    await server_bus.sub(TestReq, f)
    conn = MockConn(ConnArgs(core=None))
    conn_task = asyncio.create_task(server_bus.conn(conn))
    await conn.client__send({
        "msid": RandomUtils.makeid(),
        "mcodeid": CodeStorage.get_mcodeid_for_mtype(type(TestReq))
    })
    assert 0
    conn_task.cancel()
