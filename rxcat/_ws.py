from typing import Self

from aiohttp import WSMsgType
from aiohttp.web import WebSocketResponse as AiohttpWebsocket

from rxcat._transport import Conn, ConnArgs


class Ws(Conn[AiohttpWebsocket]):
    def __init__(self, args: ConnArgs[AiohttpWebsocket]) -> None:
        super().__init__(args)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> dict:
        connmsg = await self._core.receive()
        if connmsg.type in (
                WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
            raise StopAsyncIteration
        return connmsg.json()

    async def receive_json(self) -> dict:
        return await self._core.receive_json()

    async def send_bytes(self, data: bytes):
        return await self._core.send_bytes(data)

    async def send_json(self, data: dict):
        return await self._core.send_json(data)

    async def send_str(self, data: str):
        return await self._core.send_str(data)

    async def close(self):
        return await self._core.close()
