"""
Transport layer of rxcat protocol.

Communication is typically managed externally, rxcat only accept incoming
connections.

For a server general guideline would be to setup external connection manager,
and pass new established connections to ServerBus.conn method, where
connection processing further relies on ServerBus.
"""
from asyncio import Queue, Task
from typing import Generic, Literal, Protocol, Self, TypeVar, runtime_checkable

from pydantic import BaseModel
from pykit.rand import RandomUtils

TConnCore = TypeVar("TConnCore")

# we pass connsid to OnSend and OnRecv functions instead of Conn to
# not allow these methods to operate on connection, but instead request
# required information about it via the bus
@runtime_checkable
class OnSendFn(Protocol):
    async def __call__(self, connsid: str, rmsg: dict): ...

# generic Protocol[TConnMsg] is not used due to variance issues
@runtime_checkable
class OnRecvFn(Protocol):
    async def __call__(self, connsid: str, rmsg: dict): ...

class ConnArgs(BaseModel, Generic[TConnCore]):
    core: TConnCore
    tokens: set[str] | None = None

    class Config:
        arbitrary_types_allowed = True

class Conn(Generic[TConnCore]):
    """
    Connection abstract class.

    Methods "recv" and "send" always work with dicts, so implementations
    must perform necessary operations to convert incoming data to dict
    and outcoming data to transport layer's default structure (typically
    bytes). This is dictated by the need to product rxcat.Msg objects, which
    can be conveniently done only through parsed dict object.
    """
    def __init__(self, args: ConnArgs[TConnCore]) -> None:
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

    def __aiter__(self) -> Self:
        raise NotImplementedError

    async def __anext__(self) -> dict:
        raise NotImplementedError

    async def recv(self) -> dict:
        raise NotImplementedError

    async def send(self, data: dict):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

class Transport(BaseModel):
    is_server: bool
    conn_type: type[Conn]

    protocol: str = ""
    host: str = ""
    port: int = 0
    route: str = ""

    is_registration_enabled: bool = False

    max_inp_queue_size: int = 10000
    """
    If less or equal than zero, no limitation is applied.
    """
    max_out_queue_size: int = 10000
    """
    If less or equal than zero, no limitation is applied.
    """

    # TODO: add "max_msgs_per_minute" to limit connection's activity

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

    on_send: OnSendFn | None = None
    on_recv: OnRecvFn | None = None

    class Config:
        arbitrary_types_allowed = True

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

class ActiveTransport(BaseModel):
    transport: Transport
    inp_queue: Queue[tuple[Conn, dict]]
    out_queue: Queue[tuple[Conn, dict]]
    inp_queue_processor: Task
    out_queue_processor: Task

    class Config:
        arbitrary_types_allowed = True
