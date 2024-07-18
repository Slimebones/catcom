from typing import Self
import pytest_asyncio
from pykit.fcode import FcodeCore

from rxcat import ServerBus, Conn, ConnArgs


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

class MockConn(Conn[None]):
    def __init__(self, args: ConnArgs[None]) -> None:
        super().__init__(args)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> dict:
        return {}

    async def receive_json(self) -> dict:
        return {}

    async def send_bytes(self, data: bytes):
        return

    async def send_json(self, data: dict):
        return

    async def send_str(self, data: str):
        return

    async def close(self):
        return

