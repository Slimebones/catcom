from pykit.check import check
from pykit.code import Code, get_fqname
from pykit.err import ValErr
from pykit.err_utils import ErrDto

from rxcat import ServerBus
from tests.conftest import Mock_1, Mock_2


async def test_pubsub(server_bus: ServerBus):
    flag1 = False
    flag2 = False
    flag3 = False

    async def sub__mock_1(data: Mock_1):
        assert isinstance(data, Mock_1)
        assert data.num == 1
        nonlocal flag1
        flag1 = True

    async def sub__mock_2(data: Mock_2):
        assert isinstance(data, Mock_2)
        assert data.num == 2
        nonlocal flag2
        flag2 = True
        raise ValErr("mock_2_err")

    async def sub__err(data: ValErr):
        assert type(data) is ValErr
        assert data.args[0] == "hello"
        nonlocal flag3
        flag3 = True

    # await server_bus.sub(Mock_1, sub__mock_1)
    # await server_bus.sub(Mock_2, sub__mock_2)
    # await server_bus.sub(ValErr, sub__err)
    # await server_bus.pub(Mock_1(num=1))
    # await check.aexpect(
    #     server_bus.pubr(Mock_2(num=2)),
    #     ValErr)

    assert flag1
    assert flag2
    assert flag3

# async def test_register_req_has_index_0(server_bus: ServerBus):
#     assert \
#         (await Code.get_registered_codes()).eject()[0] == "rxcat__register_req"
