import asyncio
import json

from fastapi import WebSocketDisconnect
from fastapi.websockets import WebSocketState
from orwynn import Service
from orwynn.log import Log
from orwynn.websocket import Websocket
from pydantic import ValidationError

from src.bus.models import Connection
from src.codes_auto import Codes
from src.errors import KeycodeError
from src.message import (
    Message,
    SubscribedSystemEMessage,
    SubscribeSystemRMessage,
)
from src.message.errors import MessageError


class BusService(Service):
    RawConnectionLifetime: float = 15.0
    SystemMessageCodes: list[str] = [
        Codes.slimebones.catcom_wsbus.message.message.subscribe,
        Codes.slimebones.catcom_wsbus.message.message.subscribed,
    ]

    def __init__(self) -> None:
        super().__init__()

        # TODO(ryzhovalex): clear tasks and closed cons when possible
        self._tasks: list[asyncio.Task] = []
        self._conn_by_id: dict[str, Connection] = {}
        self._connids_by_servicecode: dict[str, list[str]] = {}
        """
        Map of connection ids by service codes.

        A connection here is duplicated for each service code subscribed.

        Several connections can reference the same service code, so we have
        list of connections for each service code.
        """

    async def connect(
        self,
        websocket: Websocket,
    ) -> None:
        conn = Connection(
            source=websocket,
            servicecodes=set(),
        )
        self._conn_by_id[conn.id] = conn

        try:
            while True:
                await self._process_message(conn)
        except WebSocketDisconnect:
            # on websocket disconnect just pass to finally block
            pass
        # do not raise exception but log it in order to reach finally block
        except Exception as err:  # noqa: BLE001
            Log.error("bus error: " + str(err))
        finally:
            # the bus closes the connection without sending back explanation
            # messages, since in the current ws implementation it is not
            # possible for the client (at least for the test one) to receive
            # a bus error message and then receive a close (close always comes
            # first). Also closing immediatelly is a somewhat protection from
            # the malicious connections which are damaging the performance.
            # The instant closing of such connections costs less processing
            # time.

            del self._conn_by_id[conn.id]
            for servicecode in conn.servicecodes:
                self._connids_by_servicecode[servicecode].remove(conn.id)
            if (
                conn.source.application_state
                is not WebSocketState.DISCONNECTED
            ):
                await conn.source.close()

    def _check_subscribe_message(
        self,
        message: SubscribeSystemRMessage,
    ) -> None:
        if (
            message.code
            != Codes.slimebones.catcom_wsbus.message.message.subscribe
        ):
            raise MessageError(
                f"wrong connect message code {message.code},"
                " expected "
                + Codes.slimebones.catcom_wsbus.message.message.subscribe,
            )

        if message.fromcode is not None:
            raise MessageError(
                "subscribe message cannot have fromcode",
            )
        if message.tocode is not None:
            raise MessageError(
                "subscribe message cannot have tocode",
            )
        if message.roomids is not None:
            raise MessageError(
                "subscribe message cannot have roomids",
            )
        if message.lmid is not None:
            raise MessageError(
                "subscribe message cannot have lmid",
            )
        if message.type != "request":
            raise MessageError(
                "expected for message to have type \"request\""
                f" got {message.type} instead",
            )

    async def _process_subscribe_message(
        self,
        conn: Connection,
        message_dict: dict,
    ) -> None:
        try:
            message = SubscribeSystemRMessage.model_validate(message_dict)
        except ValidationError as err:
            raise MessageError(
                "cannot parse message " + str(message_dict)
            ) from err

        self._check_subscribe_message(message)

        servicecodes: list[str] = [message.sendercode]
        conn.senderservicecode = servicecodes[0]
        if message.value.legacysendercodes is not None:
            servicecodes.extend(message.value.legacysendercodes)

        for servicecode in servicecodes:
            if len(servicecode) == 0:
                raise KeycodeError("empty keycode for message " + str(message))

            if servicecode not in self._connids_by_servicecode:
                self._connids_by_servicecode[servicecode] = [conn.id]
            else:
                self._connids_by_servicecode[servicecode].append(conn.id)
            conn.servicecodes.add(servicecode)

        self._tasks.append(asyncio.create_task(conn.source.send_json(
            SubscribedSystemEMessage(
                ownercode=Codes.slimebones.catcom_wsbus.bus.service.bus,
                tocode=message.sendercode,
                lmid=message.id,
            ).api,
        )))

    async def _process_system_message(
        self,
        conn: Connection,
        message_dict: dict,
    ) -> None:
        match message_dict["code"]:
            case Codes.slimebones.catcom_wsbus.message.message.subscribe:
                await self._process_subscribe_message(
                    conn, message_dict,
                )
            case _:
                raise MessageError(
                    f"code {message_dict['code']} is not the system-request"
                    " one",
                )

    def _check_message_dict(self, message_dict: dict) -> None:
        # do manual validation instead of pydantic one for a slight performance
        assert "id" in message_dict and isinstance(message_dict["id"], str)
        assert (
            "type" in message_dict
            and message_dict["type"] in ["request", "event"]
        )
        assert "code" in message_dict and isinstance(message_dict["code"], str)
        assert (
            "ownercode" in message_dict
            and isinstance(message_dict["ownercode"], str)
        )

        # enforce that messages don't have any nullified fields to reduce
        # their size
        if "fromcode" in message_dict:
            assert isinstance(message_dict["fromcode"], str)
        if "tocode" in message_dict:
            assert isinstance(message_dict["tocode"], str)
        if "roomids" in message_dict:
            assert isinstance(message_dict["roomids"], list)
            assert len(message_dict["roomids"]) > 0
        if "lmid" in message_dict:
            assert isinstance(message_dict["lmid"], str)
        if "value" in message_dict:
            assert isinstance(message_dict["value"], dict)

        # messages also might contain arbitrary redundant data - this is not
        # checked here - too long to iterate over all fields

    async def _process_message(
        self,
        conn: Connection,
    ) -> None:
        message_dict: dict = await conn.source.receive_json()

        if "code" not in message_dict:
            raise MessageError(f"message {message_dict} is missing code")

        if message_dict["code"] in self.SystemMessageCodes:
            await self._process_system_message(conn, message_dict)
            return

        self._check_message_dict(message_dict)

        # do not use send_json, optimize a lot by dumping only once for all
        # recipients
        message_send_data: dict = {
            "type": "websocket.send",
            "text": json.dumps(message_dict, separators=(",", ":")),
        }

        if "tocode" not in message_dict or message_dict["tocode"] is None:
            for _conn in self._conn_by_id.values():
                # broadcast messages will be sent to the
                # broadcast's owner too!
                self._tasks.append(asyncio.create_task(
                    _conn.source.send(message_send_data),
                ))
            return

        for _connid in self._connids_by_servicecode[message_dict["tocode"]]:
            _conn = self._conn_by_id[_connid]
            self._tasks.append(asyncio.create_task(
                _conn.source.send(
                    message_send_data,
            )))
