from typing import Self

from orwynn import DTO, UnitDTO
from orwynn.proxy.indicationonly import ApiIndicationOnlyProxy
from pykit import validation
from pykit.rnd import RandomUtils

from src.codes_auto import Codes
from src.message.dto import SubscribeSystemRMessageValue


class Message(UnitDTO):
    ownercode: str

    id: str = ""
    type: str | None = None
    fromcode: str | None = None
    tocode: str | None = None
    roomids: list[str] | None = None
    lmid: str | None = None
    value: DTO | Exception | None = None

    def __init__(self, **data):
        if "id" not in data:
            data["id"] = RandomUtils.makeid()
        super().__init__(**data)

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
            # digest value's dto or error separately
            api["value"] = api_indication.digest(self.value)
        if self.type is None:
            del api["type"]
        if self.fromcode is None:
            del api["fromcode"]
        if self.tocode is None:
            del api["tocode"]
        if self.roomids is None:
            del api["roomids"]
        if self.lmid is None:
            del api["lmid"]

        return api

    @classmethod
    def recover(cls, mp: dict) -> Self:
        """Recovers model of this class using dictionary."""
        # TODO(ryzhovalex): replace this with orwynn indication functions
        mp = {
            "type": "ok",
            "value": mp,
        }

        return validation.apply(
            ApiIndicationOnlyProxy.ie().api_indication.recover(
                cls, mp,
            ),
            cls,
        )

    class Config:
        arbitrary_types_allowed = True


class EMessage(Message):
    type: str | None = "event"


class RMessage(Message):
    type: str | None = "request"


class SystemRMessage(RMessage):
    pass

class SystemEMessage(RMessage):
    pass


class SubscribeSystemRMessage(SystemRMessage):
    Code = Codes.slimebones.catcom_wsbus.message.message.subscribe
    value: SubscribeSystemRMessageValue


class SubscribedSystemEMessage(SystemEMessage):
    Code = Codes.slimebones.catcom_wsbus.message.message.subscribed
