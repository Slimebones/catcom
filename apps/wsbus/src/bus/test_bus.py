from typing import TYPE_CHECKING

import pytest
from orwynn.testing import Client
from pykit.rnd import RandomUtils

from src.message import SubscribeSystemRMessage
from src.message.dto import SubscribeSystemRMessageValue

if TYPE_CHECKING:
    from orwynn.websocket import Websocket


@pytest.mark.asyncio
async def test_subscribe(client: Client):
    ws: Websocket
    with client.websocket(
        "/bus",
    ) as ws:
        ws.send_json(SubscribeSystemRMessage(
            id=RandomUtils.makeid(),
            ownercode="whocares",
            value=SubscribeSystemRMessageValue(),
        ).api)  # type: ignore
        response = ws.receive_json()
        print(response)

    assert 0
