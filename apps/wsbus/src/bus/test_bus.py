from typing import TYPE_CHECKING

import pytest
from orwynn.testing import Client
from pykit.rnd import RandomUtils

from src.message import SubscribeSystemRMessage
from src.message.dto import SubscribeSystemRMessageValue

if TYPE_CHECKING:
    from orwynn.websocket import Websocket


@pytest.mark.asyncio
async def test_connect(client: Client):
    ws: Websocket
    with client.websocket(
        "/bus",
    ) as ws:
        await ws.send(SubscribeSystemRMessage(
            id=RandomUtils.makeid(),
            ownercode="whocares",
            value=SubscribeSystemRMessageValue(),
        ).api["value"])
