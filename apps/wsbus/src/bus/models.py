from orwynn import Model
from orwynn.dto import Websocket
from pykit.rnd import RandomUtils


class Connection(Model):
    id: str = ""
    source: Websocket
    senderservicecode: str | None = None
    servicecodes: set[str]

    def __init__(self, **data):
        if "id" not in data:
            data["id"] = RandomUtils.makeid()
        super().__init__(**data)

    class Config:
        arbitrary_types_allowed = True
