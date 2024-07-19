from rxcat import ServerBus, Req


async def test_subaction(server_bus: ServerBus):
    class TestReq(Req):
        pass

    async def f(req: TestReq):
        print(server_bus.get_ctx())

    assert 0
