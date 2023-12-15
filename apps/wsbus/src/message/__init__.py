from orwynn import DTO, UnitDTO
from orwynn.proxy.indicationonly import ApiIndicationOnlyProxy

from src.codes_auto import Codes
from src.message.dto import SubscribeSystemRMessageValue


class Message(UnitDTO):
    ownercode: str

    type: str | None = None
    fromcode: str | None = None
    tocode: str | None = None
    roomids: list[str] | None = None
    linkedmessageid: str | None = None
    value: DTO | None = None

    @property
    def sendercode(self) -> str:
        return self.fromcode if self.fromcode is not None else self.ownercode

    @property
    def api(self) -> dict:
        api_indication = ApiIndicationOnlyProxy.ie().api_indication
        api = api_indication.digest(self)
        # remove model's default shell
        api = api["value"]

        # decrease json size by deleting null fields
        if self.value is None:
            del api["value"]
        else:
            # digest value's dto separately
            api["value"] = api_indication.digest(self.value)
        if self.type is None:
            del api["type"]
        if self.fromcode is None:
            del api["fromcode"]
        if self.tocode is None:
            del api["tocode"]
        if self.roomids is None:
            del api["roomids"]
        if self.linkedmessageid is None:
            del api["linkedmessageid"]

        return api

class EMessage(Message):
    type: str | None = "event"


class RMessage(Message):
    type: str | None = "request"


class SystemRMessage(RMessage):
    pass

class SystemEMessage(RMessage):
    pass


class SubscribeSystemRMessage(SystemRMessage):
    Code = Codes.slimebones.wsbus.message.message.subscribe
    value: SubscribeSystemRMessageValue


class SubscribedSystemEMessage(SystemEMessage):
    Code = Codes.slimebones.wsbus.message.message.subscribed
