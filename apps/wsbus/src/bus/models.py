from orwynn import Model
from orwynn.dto import Websocket


class Connection(Model):
    id: str
    source: Websocket
    is_closed: bool = False

    class Config:
        arbitrary_types_allowed = True
