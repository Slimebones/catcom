import pytest_asyncio
from fcode import FcodeCore

from rxcat import ServerBus


@pytest_asyncio.fixture(autouse=True)
async def auto():
    yield

    FcodeCore.deflock = False
    FcodeCore.clean_non_decorator_codes()
    await ServerBus.destroy()

@pytest_asyncio.fixture
async def server_bus() -> ServerBus:
    bus = ServerBus.ie()
    await bus.init()
    return bus
