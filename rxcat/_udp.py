from rxcat._transport import Conn, ConnArgs, Transport
from aiohttp.web import WebSocketResponse as AiohttpWebsocket

class Udp(Conn[AiohttpWebsocket]):
    def __init__(self, args: ConnArgs[AiohttpWebsocket]) -> None:
        super().__init__(args)

    async def send_bytes(self, data: bytes):
        return await self._core.send_bytes(data)

    async def send_json(self, data: dict):
        return await self._core.send_json(data)

    async def send_str(self, data: str):
        return await self._core.send_str(data)

    async def close(self):
        return await self._core.close()
