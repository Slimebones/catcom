"""
Transport layer of rxcat protocol.

Communication is typically managed externally, rxcat only accept incoming
connections.

For a server general guideline would be to setup external connection manager,
and pass new established connections to ServerBus.conn method, where
connection processing further relies on ServerBus.
"""
from typing import Generic, TypeVar

from pydantic import BaseModel
from pykit.rand import RandomUtils

TCore = TypeVar("TCore")

class ConnArgs(BaseModel, Generic[TCore]):
    core: TCore
    tokens: set[str] | None = None

class Conn(Generic[TCore]):
    def __init__(self, args: ConnArgs[TCore]) -> None:
        self._sid = RandomUtils.makeid()
        self._core = args.core
        self._is_closed = False

        self._tokens: set[str] = set()
        if args.tokens:
            self._tokens = args.tokens.copy()

    @property
    def sid(self) -> str:
        return self._sid

    @property
    def tokens(self) -> set[str]:
        return self._tokens.copy()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    async def send_str(self, data: str):
        raise NotImplementedError

    async def send_json(self, data: dict):
        raise NotImplementedError

    async def send_bytes(self, data: bytes):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

class Transport(BaseModel):
    is_server: bool
    conn_type: Conn

    max_inp_queue_size: int = 10000
    max_out_queue_size: int = 10000

    inactivity_timeout: float | None = None
    """
    Default inactivity timeout for a connection.

    If nothing is received on a connection for this amount of time, it
    is disconnected.

    None means no timeout applied.
    """
    mtu: int = 1400
    """
    Max size of a packet that can be sent by the transport.

    Note that this is total size including any headers that could be added
    by the transport.
    """

    protocol: str
    host: str
    port: int
    route: str

    @property
    def url(self) -> str:
        return \
            self.protocol \
            + "://" \
            + self.host \
            + ":" \
            + str(self.port) \
            + "/" \
            + self.route
