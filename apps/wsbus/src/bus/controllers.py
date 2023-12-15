from orwynn.websocket import Websocket, WebsocketController

from src.bus.services import BusService


class BusWsController(WebsocketController):
    Route = "/bus"

    def __init__(
        self,
        bus_service: BusService,
    ) -> None:
        self._bus_service = bus_service

    async def main(
        self,
        websocket: Websocket,
    ):
        await self._bus_service.connect(websocket)
