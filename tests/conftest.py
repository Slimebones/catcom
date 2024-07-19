from asyncio import Queue
from contextvars import ContextVar
from typing import Self

import pytest_asyncio
from pykit.err import ValErr
from pykit.fcode import FcodeCore, code
from pykit.res import Res
from result import Err, Ok

from rxcat import Conn, ConnArgs, Evt, Req, ServerBus, ServerBusCfg, Transport, Msg, SrpcReq


@code("rxcat_mock_evt_1")
class MockEvt_1(Evt):
    num: int

@code("rxcat_mock_evt_2")
class MockEvt_2(Evt):
    num: int

@code("rxcat_mock_req_1")
class MockReq_1(Req):
    num: int

@code("rxcat_mock_req_2")
class MockReq_2(Req):
    num: int

@pytest_asyncio.fixture(autouse=True)
async def auto():
    yield

    FcodeCore.deflock = False
    FcodeCore.clean_non_decorator_codes()
    await ServerBus.destroy()

@pytest_asyncio.fixture
async def server_bus() -> ServerBus:
    bus = ServerBus.ie()
    cfg = ServerBusCfg(
        transports=[
            Transport(
                is_server=True,
                conn_type=MockConn,
                server__register_process="none")
        ])
    await bus.init(cfg)
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

async def get_mock_ctx_manager_for_msg(msg: Msg) -> MockCtxManager:
    return MockCtxManager()

async def get_mock_ctx_manager_for_srpc_req(req: SrpcReq) -> MockCtxManager:
    return MockCtxManager()

def find_mcodeid_in_welcome_rmsg(code: str, rmsg: dict) -> Res[int]:
    for i, code_container in enumerate(rmsg["indexed_mcodes"]):
        if code in code_container:
            return Ok(i)
    return Err(ValErr())

def find_errcodeid_in_welcome_rmsg(code: str, rmsg: dict) -> Res[int]:
    for i, code_container in enumerate(rmsg["indexed_errcodes"]):
        if code in code_container:
            return Ok(i)
    return Err(ValErr())
