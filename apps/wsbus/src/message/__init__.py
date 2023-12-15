from orwynn import DTO, UnitDTO

from src.codes_auto import Codes
from src.message.dto import SubscribeSystemRMessageValue


class Message(UnitDTO):
    ownercode: str
    value: DTO | None = None

    type: str | None = None
    fromcode: str | None = None
    tocode: str | None = None
    roomids: list[str] | None = None
    linkedmessageid: str | None = None

    @property
    def sendercode(self) -> str:
        return self.fromcode if self.fromcode is not None else self.ownercode


class EMessage(Message):
    type = "event"


class RMessage(Message):
    type = "request"


class SystemRMessage(RMessage):
    pass

class SystemEMessage(RMessage):
    pass


class SubscribeSystemRMessage(SystemRMessage):
    Code = Codes.slimebones.wsbus.message.message.subscribe
    value: SubscribeSystemRMessageValue


class SubscribedSystemEMessage(SystemEMessage):
    Code = Codes.slimebones.wsbus.message.message.subscribed
