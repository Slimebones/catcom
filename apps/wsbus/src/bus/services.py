import asyncio
import json
from orwynn import Service
from orwynn.dto import NotFoundError
from orwynn.log import Log
from orwynn.websocket import Websocket
from pydantic import ValidationError
from pykit.errors import AlreadyEventError, UnsupportedError, StrExpectError
from pykit.dt import DTUtils
from pykit.rnd import RandomUtils
from src.bus.models import Connection
from src.codes_auto import Codes
from src.errors import KeycodeError

from src.message import SubscribeSystemRMessage, SubscribedSystemEMessage, Message
from src.message.errors import MessageError


class BusService(Service):
    RawConnectionLifetime: float = 15.0
    SystemMessageCodes: list[str] = [
        Codes.slimebones.wsbus.message.message.subscribe,
        Codes.slimebones.wsbus.message.message.subscribed
    ]

    def __init__(self) -> None:
        self._conns_by_servicecode: dict[str, list[Connection]] = {}
        """
        Map of connections by service codes.

        A connection here is duplicated for each service code subscribed.

        Several connections can reference the same service code, so we have
        list of connections for each service code.
        """

    async def connect(
        self,
        websocket: Websocket
    ) -> None:
        conn = Connection(
            id=RandomUtils.makeid(),
            source=websocket
        )

        try:
            while True:
                await self._process_message(conn)
        finally:
            # closed conns are cleared later
            conn.is_closed = True

    async def _process_subscribe_message(
        self,
        conn: Connection,
        message_dict: dict
    ) -> str:
        try:
            connect_message = SubscribeSystemRMessage.model_validate({
                "type": "ok",
                "value": message_dict
            })
        except ValidationError as err:
            raise MessageError("cannot parse message") from err

        if (
            connect_message.code
            != Codes.slimebones.wsbus.message.message.subscribe
        ):
            raise MessageError(
                f"wrong connect message code {connect_message.code},"
                " expected "
                + Codes.slimebones.wsbus.message.message.subscribe
            )

        if connect_message.fromcode is not None:
            raise MessageError(
                "subscribe message cannot have fromcode"
            )
        if connect_message.tocode is not None:
            raise MessageError(
                "subscribe message cannot have tocode"
            )
        if connect_message.roomids is not None:
            raise MessageError(
                "subscribe message cannot have roomids"
            )
        if connect_message.linkedmessageid is not None:
            raise MessageError(
                "subscribe message cannot have linkedmessageid"
            )
        if connect_message.type != "request":
            raise MessageError(
                "expected for message to have type \"request\""
                f" got {connect_message.type} instead"
            )

        servicecode: str = connect_message.ownercode

        if len(servicecode) == 0:
            raise KeycodeError("empty keycode")

        if servicecode not in self._conns_by_servicecode:
            self._conns_by_servicecode[servicecode] = [conn]
        else:
            self._conns_by_servicecode[servicecode].append(conn)

        if conn.is_closed:
            raise ConnectionError(
                "connection was closed before receiving the subscribed event"
            )
        asyncio.create_task(conn.source.send_json(SubscribedSystemEMessage(
            id=RandomUtils.makeid(),
            ownercode=Codes.slimebones.wsbus.bus.service.bus,
            tocode=connect_message.sendercode,
            linkedmessageid=connect_message.id
        )))
        return servicecode

    async def _process_system_message(
        self,
        conn: Connection,
        message_dict: dict
    ) -> None:
        match message_dict["code"]:
            case Codes.slimebones.wsbus.message.message.subscribe:
                await self._process_subscribe_message(
                    conn, message_dict
                )
            case _:
                raise MessageError(
                    f"code {message_dict['code']} is not the system-request"
                    " one"
                )

    async def _process_message(
        self,
        conn: Connection
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
            "text": json.dumps(message_dict, separators=(",", ":"))
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
                        asyncio.create_task(
                            _conn.source.send(message_send_data)
                        )
                        sent_conn_ids.add(_conn.id)
            return

        for _conn in self._conns_by_servicecode[message.tocode]:
            if _conn.is_closed:
                continue
            asyncio.create_task(
                _conn.source.send(
                    message_send_data
            ))
