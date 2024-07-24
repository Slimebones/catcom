import asyncio
from asyncio import Queue
from contextvars import ContextVar
from typing import Self

import pytest_asyncio
from pydantic import BaseModel
from pykit.res import Ok, Res, valerr

from rxcat import (
    Conn,
    ConnArgs,
    ServerBus,
    ServerBusCfg,
    Transport,
)


class Mock_1(BaseModel):
    num: int

    @staticmethod
    def code() -> str:
        return "rxcat__mock_1"

class Mock_2(BaseModel):
    num: int

    @staticmethod
    def code() -> str:
        return "rxcat__mock_2"

@pytest_asyncio.fixture(autouse=True)
async def auto():
    yield
    await ServerBus.destroy()

@pytest_asyncio.fixture
async def server_bus() -> ServerBus:
    bus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                is_registration_enabled=False)
        ],
        reg_types=[
            Mock_1,
            Mock_2
        ])
    await asyncio.wait_for(bus.init(cfg), 1)
    return bus

@pytest_asyncio.fixture
async def reg_server_bus() -> ServerBus:
    bus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                is_registration_enabled=True)
        ],
        reg_types=[
            Mock_1,
            Mock_2
        ])
    await asyncio.wait_for(bus.init(cfg), 1)
    return bus

class MockConn(Conn[None]):
    def __init__(self, args: ConnArgs[None]) -> None:
        super().__init__(args)
        self.inp_queue: Queue[dict] = Queue()
        self.out_queue: Queue[dict] = Queue()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> dict:
        return await self.recv()

    async def recv(self) -> dict:
        return await self.inp_queue.get()

    async def send(self, data: dict):
        await self.out_queue.put(data)

    async def close(self):
        return

    async def client__send(self, data: dict):
        await self.inp_queue.put(data)

    async def client__recv(self) -> dict:
        return await self.out_queue.get()

rxcat_mock_ctx = ContextVar("rxcat_mock", default={})
class MockCtxManager:
    async def __aenter__(self):
        rxcat_mock_ctx.set({"name": "hello"})
    async def __aexit__(self, *args):
        return

async def get_mock_ctx_manager_for_msg(_) -> Res[MockCtxManager]:
    return Ok(MockCtxManager())

async def get_mock_ctx_manager_for_srpc_send(_) -> Res[MockCtxManager]:
    return Ok(MockCtxManager())

def find_codeid_in_welcome_rmsg(code: str, rmsg: dict) -> Res[int]:
    for i, code_container in enumerate(rmsg["data"]["codes"]):
        if code in code_container:
            return Ok(i)
    return valerr(code)
