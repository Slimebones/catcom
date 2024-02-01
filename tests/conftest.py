import pytest_asyncio
from fcode import FcodeCore

from rxcat import Bus


@pytest_asyncio.fixture(autouse=True)
async def auto():
    yield

    FcodeCore.deflock = False
    FcodeCore.clean_non_decorator_codes()
    Bus.try_discard()

@pytest_asyncio.fixture
async def bus() -> Bus:
    bus = Bus.ie()
    await bus.init()
    return bus
