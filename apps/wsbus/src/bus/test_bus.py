import typing
from typing import TYPE_CHECKING

import pytest
from orwynn.testing import Client

from src.codes_auto import Codes
from src.message import SubscribedSystemEMessage, SubscribeSystemRMessage
from src.message.dto import SubscribeSystemRMessageValue

if TYPE_CHECKING:
    from orwynn.websocket import Websocket


@pytest.mark.asyncio
async def test_subscribe(client: Client):
    ws: Websocket
    with client.websocket(
        "/bus",
    ) as ws:
        inmessage = SubscribeSystemRMessage(
            ownercode="whocares",
            value=SubscribeSystemRMessageValue(),
        )
        ws.send_json(inmessage.api)  # type: ignore
        response = typing.cast(dict, ws.receive_json())
        outmessage = SubscribedSystemEMessage.recover(response)
        assert (
            outmessage.code
            == Codes.slimebones.catcom_wsbus.message.message.subscribed
        )
        assert (
            outmessage.sendercode
            == Codes.slimebones.catcom_wsbus.bus.service.bus
        )
        assert outmessage.lmid == inmessage.id
