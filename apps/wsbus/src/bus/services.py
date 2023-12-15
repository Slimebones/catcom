import asyncio
import json

from orwynn import Service
from orwynn.websocket import Websocket
from pydantic import ValidationError
from pykit.rnd import RandomUtils

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
        Codes.slimebones.wsbus.message.message.subscribe,
        Codes.slimebones.wsbus.message.message.subscribed,
    ]

    def __init__(self) -> None:
        super().__init__()

        # TODO(ryzhovalex): clear tasks and closed cons when possible
        self._tasks: list[asyncio.Task] = []
        self._conns_by_servicecode: dict[str, list[Connection]] = {}
        """
        Map of connections by service codes.

        A connection here is duplicated for each service code subscribed.

        Several connections can reference the same service code, so we have
        list of connections for each service code.
        """

    async def connect(
        self,
        websocket: Websocket,
    ) -> None:
        conn = Connection(
            id=RandomUtils.makeid(),
            source=websocket,
        )

        try:
            while True:
                await self._process_message(conn)
        finally:
            # TODO(ryzhovalex): return error emessages back

            # closed conns are cleared later
            conn.is_closed = True


    def _check_subscribe_message(
        self,
        message: SubscribeSystemRMessage,
    ) -> None:
        if (
            message.code
            != Codes.slimebones.wsbus.message.message.subscribe
        ):
            raise MessageError(
                f"wrong connect message code {message.code},"
                " expected "
                + Codes.slimebones.wsbus.message.message.subscribe,
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
        if message.linkedmessageid is not None:
            raise MessageError(
                "subscribe message cannot have linkedmessageid",
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
    ) -> str:
        try:
            message = SubscribeSystemRMessage.model_validate(message_dict)
        except ValidationError as err:
            raise MessageError("cannot parse message") from err

        self._check_subscribe_message(message)

        servicecode: str = message.ownercode

        if len(servicecode) == 0:
            raise KeycodeError("empty keycode")

        if servicecode not in self._conns_by_servicecode:
            self._conns_by_servicecode[servicecode] = [conn]
        else:
            self._conns_by_servicecode[servicecode].append(conn)

        if conn.is_closed:
            raise ConnectionError(
                "connection was closed before receiving the subscribed event",
            )
        self._tasks.append(asyncio.create_task(conn.source.send_json(
            SubscribedSystemEMessage(
                id=RandomUtils.makeid(),
                ownercode=Codes.slimebones.wsbus.bus.service.bus,
                tocode=message.sendercode,
                linkedmessageid=message.id,
            ).api,
        )))
        return servicecode

    async def _process_system_message(
        self,
        conn: Connection,
        message_dict: dict,
    ) -> None:
        match message_dict["code"]:
            case Codes.slimebones.wsbus.message.message.subscribe:
                await self._process_subscribe_message(
                    conn, message_dict,
                )
            case _:
                raise MessageError(
                    f"code {message_dict['code']} is not the system-request"
                    " one",
                )

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

        message = Message.model_validate(message_dict)

        # do not use send_json, optimize a lot by dumping only once for all
        # recipients
        message_send_data: dict = {
            "type": "websocket.send",
            "text": json.dumps(message_dict, separators=(",", ":")),
        }

        if message.tocode is None:
            sent_conn_ids: set[str] = set()
            for _conns in self._conns_by_servicecode.values():
                for _conn in _conns:
                    if _conn.is_closed:
                        continue

                    if _conn.id not in sent_conn_ids:
                        # broadcast messages will be sent to the
                        # broadcast's owner too!
                        self._tasks.append(asyncio.create_task(
                            _conn.source.send(message_send_data),
                        ))
                        sent_conn_ids.add(_conn.id)
            return

        for _conn in self._conns_by_servicecode[message.tocode]:
            if _conn.is_closed:
                continue
            self._tasks.append(asyncio.create_task(
                _conn.source.send(
                    message_send_data,
            )))
