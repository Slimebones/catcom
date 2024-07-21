import asyncio

from pykit.err import ValErr
from pykit.rand import RandomUtils
from pykit.res import Res
from result import Err, Ok
from pykit.res import eject


# TODO: move to pykit
class Lock:
    def __init__(self) -> None:
        self._evt = asyncio.Event()
        self._owner_token: str | None = None

    async def __aenter__(self):
        eject(await self.lock())

    async def __aexit__(self, *args):
        assert self._owner_token is not None
        eject(await self.unlock(self._owner_token))

    def is_locked(self) -> bool:
        return self._evt.is_set()

    async def lock(self) -> Res[str]:
        await self._evt.wait()
        self._evt.set()
        self._owner_token = RandomUtils.makeid()
        return Ok(self._owner_token)

    async def unlock(self, token: str) -> Res[None]:
        if self._owner_token is not None and token != self._owner_token:
            return Err(ValErr("invalid token to unlock"))
        self._evt.clear()
        self._owner_token = None
        return Ok(None)

    async def wait(self):
        return await self._evt.wait()
