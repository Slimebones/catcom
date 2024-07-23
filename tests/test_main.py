from pykit.code import Code

from rxcat import ServerBus
from tests.conftest import Mock_1


async def test_pubsub(server_bus: ServerBus):
    flag = False

    async def sub__mock_1(data: Mock_1):
        assert isinstance(data, Mock_1)
        assert data.num == 1
        nonlocal flag
        flag = True

    (await server_bus.sub(Mock_1, sub__mock_1)).eject()
    (await server_bus.pub(Mock_1(num=1))).eject()

    assert flag

async def test_reg_req_has_index_0(server_bus: ServerBus):
    assert \
        (await Code.get_regd_codes()).eject()[0] == "rxcat__reg"
