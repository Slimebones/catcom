from orwynn.websocket import Websocket
from orwynn.testing import Client
from pykit.rnd import RandomUtils
import pytest

from src.message import SubscribeSystemRMessage
from src.message.dto import SubscribeSystemRMessageValue


@pytest.mark.asyncio
async def test_connect(client: Client):
    ws: Websocket
    with client.websocket(
        "/bus"
    ) as ws:
        await ws.send(SubscribeSystemRMessage(
            id=RandomUtils.makeid(),
            ownercode="whocares",
            value=SubscribeSystemRMessageValue()
        ).api["value"])
