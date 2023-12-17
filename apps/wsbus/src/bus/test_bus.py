import asyncio
import typing
from typing import TYPE_CHECKING
from fastapi import WebSocketDisconnect
from orwynn import DTO
from pykit import validation

import pytest
from orwynn.testing import Client

from src.codes_auto import Codes
from src.message import EMessage, RMessage, SubscribedSystemEMessage, SubscribeSystemRMessage
from src.message.dto import SubscribeSystemRMessageValue

if TYPE_CHECKING:
    from orwynn.websocket import Websocket


@pytest.mark.asyncio
async def test_subscribe(client: Client):
    ws: Websocket
    with client.websocket("/bus") as ws:
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


@pytest.mark.asyncio
async def test_subscribe_wrong_input(client: Client):
    ws: Websocket
    with client.websocket("/bus") as ws:
        ws.send_json("whoops")  # type: ignore
        validation.expect(
            ws.receive_json,
            WebSocketDisconnect
        )


@pytest.mark.asyncio
async def test_messaging(client: Client):
    class TestRM(RMessage):
        Code = "testrm"

    class TestEM(RMessage):
        Code = "testem"

    ws1: Websocket
    ws2: Websocket
    with client.websocket("/bus") as ws1:
        with client.websocket("/bus") as ws2:
            # subscribe both
            inmessage = SubscribeSystemRMessage(
                ownercode="testing_1",
                value=SubscribeSystemRMessageValue(),
            )
            ws1.send_json(inmessage.api)  # type: ignore
            ws1.receive_json()  # type: ignore
            inmessage = SubscribeSystemRMessage(
                ownercode="testing_2",
                value=SubscribeSystemRMessageValue(),
            )
            ws2.send_json(inmessage.api)  # type: ignore
            ws2.receive_json()  # type: ignore

            # send rm
            sentrm = TestRM(
                ownercode="testing_1",
                tocode="testing_2"
            )
            ws1.send_json(sentrm.api)  # type: ignore

            # receive rm
            rmraw = typing.cast(dict, ws2.receive_json())
            rm = TestRM.recover(rmraw)

            # send em
            ws2.send_json(TestEM(
                ownercode="testing_2",
                tocode="testing_1",
                lmid=rm.id,
            ).api)  # type: ignore

            # receive em
            emraw = typing.cast(dict, ws1.receive_json())
            em = TestEM.recover(emraw)
            assert em.lmid == sentrm.id
