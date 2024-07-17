"""
Rxcat implementation for Python.

Note that no string codes are used in base serialized messages, instead all of
them are replaced by codeids, which is known at server boot and shared with
every client on connection. For now this is two types of codes:
    - message codes (mcode, mcodeid)
    - error codes (errcode, errcodeid) - required since we use general
        "ThrownErrEvt" for every err, and attach an additional "errcodeid".
"""

import asyncio
from contextvars import ContextVar
import functools
import typing
from asyncio import Task
from asyncio.queues import Queue
from collections.abc import Awaitable, Callable
from typing import (
    TYPE_CHECKING,
    Any,
    Coroutine,
    Protocol,
    Self,
    TypeVar,
    runtime_checkable,
)

from aiohttp import WSMessage
from aiohttp.web import WebSocketResponse as Websocket
import loguru
from pydantic import BaseModel
from pykit.err import AlreadyProcessedErr, InpErr, NotFoundErr, ValueErr
from pykit.fcode import FcodeCore, code
from pykit.log import log
from pykit.pointer import Pointer
from pykit.rand import RandomUtils
from pykit.res import Res, eject
from pykit.singleton import Singleton
from result import Err, Ok, UnwrapError

if TYPE_CHECKING:
    from aiohttp.http import WSMessage as Wsmsg

__all__ = [
    "ServerBus",
    "Msg",
    "Req",
    "Evt",
    "ErrEvt",
    "ResourceServerErr",
    "RegisterProtocol"
]

# TODO:
#   make child of pykit.InternalErr (as it gets implementation) - to be able
#   to enable/disable internal errors sending to net (and additionally log
#   them by the server bus)
@code("resource_server_err")
class ResourceServerErr(Exception):
    pass

class ServerRegisterData(BaseModel):
    """
    Data formed by the server on RegisterProtocol call.
    """
    data: dict[str, Any] | None = None
    """
    Dict to be sent back to client with extra information from the resource
    server.
    """

@runtime_checkable
class RegisterProtocol(Protocol):
    """
    Register function to be called on client RegisterReq arrival.
    """
    async def __call__(
        self,
        /,
        tokens: list[str],
        client_data: dict[str, Any] | None) -> Res[ServerRegisterData]: ...

class internal_InvokedActionUnhandledErr(Exception):
    def __init__(self, action: Callable, err: Exception):
        super().__init__(
            f"invoked {action} unhandled err: {err!r}"
        )

class internal_BusUnhandledErr(Exception):
    def __init__(self, err: Exception):
        super().__init__(
            f"bus unhandled err: {err}"
        )

class ServerBusCfg(BaseModel):
    register: RegisterProtocol | None = None
    """
    Function used to register client.

    Defaults to None. If None, all users are still required to send
    RegisterReq, but no alternative function will be called.
    """

    msg_queue_max_size: int = 10000
    register_timeout: float = 60
    """
    How much time a client has to send a register request after they connected.

    If the timeout is reached, the bus disconnects a client.
    """

    are_errs_catchlogged: bool = False
    """
    Whether to catch and reraise thrown to the bus errors.
    """

    class Config:
        arbitrary_types_allowed = True

class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    @abs
    """
    msid: str = ""

    m_connsid: str | None = None
    """
    From which conn the msg is originated.

    Only actual for the server. If set to None, it means that the msg is inner.
    Otherwise it is always set to connsid.
    """

    m_target_connsids: list[str] = []
    """
    To which connsids the published msg should be addressed.
    """

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        if "msid" not in data:
            data["msid"] = RandomUtils.makeid()
        super().__init__(**data)

    def __hash__(self) -> int:
        assert self.msid
        return hash(self.msid)

    # todo: use orwynn indication funcs for serialize/deserialize methods

    def serialize_json(self, mcodeid: int) -> dict:
        res: dict = self.model_dump()

        # DEL SERVER MSG FIELDS
        #       we should do these before key deletion setup
        if "m_connsid" in res and res["m_connsid"] is not None:
            # connsids must exist only inside server bus, it's probably an err
            # if a msg is tried to be serialized with connsid, but we will
            # throw a warning for now, and ofcourse del the field
            log.warn(
                "connsids must exist only inside server bus, but it is tried"
                f" to serialize msg {self} with connsid != None"
            )
            del res["m_connsid"]
        if "m_target_connsids" in res:
            del res["m_target_connsids"]

        is_msid_found = False
        keys_to_del: list[str] = []
        for k, v in res.items():
            if k == "msid":
                is_msid_found = True
                continue
            # all inner keys are deleted from the final serialization
            if k.startswith("inner_"):
                keys_to_del.append(k)
                continue
            if v is None:
                keys_to_del.append(k)

        if not is_msid_found:
            raise ValueError(f"no msid field for raw msg {res}")
        for k in keys_to_del:
            del res[k]

        # for json field "codeidsize" is not required - codeid is always
        # 32 bits (or whatever json default int size is)
        res["mcodeid"] = mcodeid

        return res

    @classmethod
    def deserialize_json(cls, data: dict) -> Self:
        """Recovers model of this class using dictionary."""

        if "mcodeid" in data:
            del data["mcodeid"]

        if "rsid" not in data:
            data["rsid"] = None

        return cls(**data)

class Req(Msg):
    """
    @abs
    """

    def get_res_connsids(self) -> list[str]:
        return [self.m_connsid] if self.m_connsid is not None else []

class Evt(Msg):
    """
    @abs
    """
    rsid: str | None
    """
    In response to which request the event has been sent.
    """

    m_isContinious: bool | None = None
    """
    Whether receiving bus should delete pubaction entry after call pubaction
    with this evt. If true, the entry is not deleted.
    """

    def as_res_from_req(self, req: Req) -> Self:
        self.rsid = req.msid
        self.m_target_connsids = req.get_res_connsids()
        return self

TEvt = TypeVar("TEvt", bound=Evt)
TReq = TypeVar("TReq", bound=Req)
PubAction = Callable[[TReq, TEvt], Awaitable[Res[None] | None]]

@code("ok-evt")
class OkEvt(Evt):
    """
    Confirm that a req processed successfully.

    This evt should have a rsid defined, otherwise it is pointless since
    it is too general.
    """

@code("err-evt")
class ErrEvt(Evt):
    """
    Represents any err that can be thrown.

    Only Server-endpoints can throw errs to the bus.
    """
    errcodeid: int | None = None
    errmsg: str

    inner_err: Exception | None = None
    """
    Err that only exists on the inner bus and won't be serialized.
    """

    isThrownByPubAction: bool | None = None
    """
    Errs can be thrown by req-listening action or by req+evt listening
    pubaction.

    In the second case, we should set this flag to True to avoid infinite
    msg loop, where after pubaction fail, the err evt is generated with the
    same req sid, and again is sent to the same pubaction which caused this
    err.

    If this flag is set, the bus will prevent pubaction trigger, for this err
    evt, but won't disable the pubaction.
    """

@code("initd-client-evt")
class InitdClientEvt(Evt):
    """
    Welcome evt sent to every connected client.

    Contains information required to survive in harsh rx environment.
    """

    indexedMcodes: list[list[str]]
    """
    Collection of active and legacy mcodes, indexed by their mcodeid,
    first mcode under each index's list is an active one.
    """

    indexedErrcodes: list[list[str]]
    """
    Collection of active and legacy errcodes, indexed by their
    errcodeid, first errcode under each index's list is an active one.
    """

    # ... here an additional info how to behave properly on the bus can be sent

TMsg = TypeVar("TMsg", bound=Msg)
Subscriber = Callable[[TMsg], Awaitable[Res[TMsg | list[TMsg] | None] | None]]

class PubOpts(BaseModel):
    must_send_to_inner: bool = True

    must_send_to_net: bool = True
    """
    Will send to net if True and code is defined for the msg passed.
    """

    pubr_must_ignore_err_evt: bool = False
    """
    Whether pubr must ignore returned ErrEvt and return it as it is.
    """

    on_missing_connsid: Callable[[str], Coroutine] | None = None

    pubr_timeout: float | None = None
    """
    Timeout of awaiting for published message response arrival. Defaults to
    None, which means no timeout is set.
    """

MsgFilter = Callable[[Msg], Awaitable[bool]]

class SubOpts(BaseModel):
    must_receive_last_msg: bool = True
    filters: list[MsgFilter] = []
    """
    All filters should succeed before msg being passed to a subscriber.
    """

@code("rxcat_register_req")
class RegisterReq(Req):
    tokens: list[str]
    """
    Client's list of token to manage signed connection.

    Can be empty. The bus does not decide how to operate over the client's
    connection based on tokens, the resource server does, to which these
    tokens are passed.
    """

    data: dict[str, Any] | None = None
    """
    Extra client data passed to the resource server.
    """

class _SubAction(BaseModel):
    sid: str
    subscriber: Subscriber
    opts: SubOpts
    is_removing: bool = False
    """
    Subsids to be safely removed to not break any iteration cycles or
    other stuff.
    """

    class Config:
        arbitrary_types_allowed = True

class ConnData(BaseModel):
    conn: Websocket
    tokens: list[str]

    class Config:
        arbitrary_types_allowed = True

_rxcat_ctx = ContextVar("rxcat_ctx", default={})

class ServerBus(Singleton):
    """
    Rxcat server bus implementation.
    """

    def __init__(self):
        self._is_initd = False

    async def init(self, cfg: ServerBusCfg = ServerBusCfg()):
        self._cfg = cfg

        FcodeCore.defcode("rxcat_fallback_err", Exception)
        # only server is able to index mcodes, client is not able to send
        # theirs mcodes on conn, so the server must know client codes at boot
        #
        # active and legacy codes are bundled together under the same id, so
        # we use a dict here
        self._MCODE_TO_MCODEID: dict[str, int] = {}
        self._ERRCODE_TO_ERRCODEID: dict[str, int] = {}

        self._INDEXED_ACTIVE_MCODES: list[str] = []
        self._INDEXED_ACTIVE_ERRCODES: list[str] = []

        self.INDEXED_MCODES: list[list[str]] = []
        self.INDEXED_ERRCODES: list[list[str]] = []

        self._init_mcodes()
        self._init_errcodes()
        self._fallback_errcodeid: int = \
            self._ERRCODE_TO_ERRCODEID["rxcat_fallback_err"]

        self._sid_to_conn_data: dict[str, ConnData] = {}

        self._subsid_to_mtype: dict[str, type[Msg]] = {}
        self._subsid_to_subaction: dict[str, _SubAction] = {}
        self._mtype_to_subactions: \
            dict[type[Msg], list[_SubAction]] = {}
        self._type_to_last_msg: dict[type[Msg], Msg] = {}

        # network in and out unprocessed yet raw msgs
        self._net_inp_connsid_and_wsmsg_queue: Queue[tuple[str, Wsmsg]] = \
            Queue(self._cfg.msg_queue_max_size)
        self._net_out_connsids_and_rawmsg_queue: Queue[
            tuple[set[str], dict]
        ] = Queue(self._cfg.msg_queue_max_size)

        self._net_inp_queue_processor: Task | None = None
        self._net_out_queue_processor: Task | None = None

        self._preserialized_initd_client_evt: dict = {}
        self._initd_client_evt_mcodeid: int | None = None

        if not self._net_inp_queue_processor:
            self._net_inp_queue_processor = asyncio.create_task(
                self._process_net_inp_queue()
            )
        if not self._net_out_queue_processor:
            self._net_out_queue_processor = asyncio.create_task(
                self._process_net_out_queue()
            )

        self._rsid_to_req_and_pubaction: dict[str, tuple[Req, PubAction]] = {}
        self._rsids_to_del_on_next_pubaction: set[str] = set()

        self._is_initd = True
        self._is_post_initd = False

    async def close_conn(self, sid: str) -> Res[None]:
        if sid not in self._sid_to_conn_data:
            return Err(NotFoundErr(f"conn with sid {sid}"))
        conn = self._sid_to_conn_data[sid].conn
        del self._sid_to_conn_data[sid]
        if not conn.closed:
            await conn.close()
        return Ok(None)

    @property
    def is_initd(self) -> bool:
        return self._is_initd

    async def throw(
        self,
        err: Exception,
        triggered_msg: Msg | None = None,
        pub_opts: PubOpts = PubOpts(),
        *,
        m_to_connsids: list[str] | None = None,
        is_thrown_by_pubaction: bool | None = None
    ):
        """
        Pubs ThrownErrEvt.

        If given err has no code attached, the default is
        used.

        The thrown err evt will be sent to connections if one of is true:
            - the triggered msg has conn id attached
            - the m_to_connsids field is given

        If both is true, the m_to_connsids will be used as override.

        result.UnwrapError will be fetched for the Result's err_value, and
        checked that this value is an instance of Exception before making
        it as the final sent err.

        Args:
            err:
                Err to throw as evt.
            triggered_msg:
                Msg which caused an error. If the msg is Evt with rsid
                defined, it will be send back to the requestor.
            pub_opts:
                Extra pub opts to send to Bus.pub method.
        """
        if isinstance(err, UnwrapError):
            res = err.result
            assert isinstance(res, Err)
            res_err_value = res.err_value
            if isinstance(res_err_value, Exception):
                err = res_err_value
            else:
                err = ResourceServerErr(
                    f"got res with err value {res_err_value},"
                    " which is not an instance of Exception")
        errcodeid: int | None = self.try_get_errcodeid_for_errtype(type(err))
        errmsg: str = ", ".join([str(a) for a in err.args])

        rsid: str | None = None
        if isinstance(triggered_msg, Evt):
            rsid = triggered_msg.rsid
        elif isinstance(triggered_msg, Req):
            rsid = triggered_msg.msid
        else:
            assert triggered_msg is None

        final_to_connsids = []
        if m_to_connsids is not None:
            final_to_connsids = m_to_connsids
        elif triggered_msg and triggered_msg.m_connsid is not None:
            final_to_connsids = [triggered_msg.m_connsid]

        evt = ErrEvt(
            errcodeid=errcodeid,
            errmsg=errmsg,
            inner_err=err,
            rsid=rsid,
            m_target_connsids=final_to_connsids,
            isThrownByPubAction=is_thrown_by_pubaction
        )

        if errcodeid is None:
            errcodeid = self._fallback_errcodeid

        log.err(f"thrown err evt: {evt}", 1)
        if self._cfg.are_errs_catchlogged:
            log.catch(err)
        await self.pub(evt, None, pub_opts)

    def try_get_mcodeid_for_mcode(self, mcode: str) -> int | None:
        res = self._MCODE_TO_MCODEID.get(mcode, -1)
        if res == -1:
            return None
        return res

    def try_get_errcodeid_for_errcode(self, errcode: str) -> int | None:
        res = self._ERRCODE_TO_ERRCODEID.get(errcode, -1)
        if res == -1:
            return None
        return res

    def get_errcode_for_errcodeid(self, errcodeid: int) -> Res[str]:
        for k, v in self._ERRCODE_TO_ERRCODEID.items():
            if v == errcodeid:
                return Ok(k)
        return Err(NotFoundErr(f"errcode for errcodeid {errcodeid}"))

    def _init_mcodes(self):
        collections = FcodeCore.try_get_all_codes(Msg)
        assert collections, "must have at least one mcode defined"

        # put RegisterReq to index 0, according to the protocol
        register_req_start_index = -1
        for i, c in enumerate(collections):
            if c[0] == "rxcat_register_req":
                register_req_start_index = i
        assert register_req_start_index >= 0, "RegisterReq must be found"
        # replace whatever is at index 0, and put register req there
        collections[0], collections[register_req_start_index] = \
            collections[register_req_start_index], collections[0]

        self.INDEXED_MCODES = collections
        for id, mcodes in enumerate(collections):
            self._INDEXED_ACTIVE_MCODES.append(mcodes[0])
            for mcode in mcodes:
                self._MCODE_TO_MCODEID[mcode] = id

    def _init_errcodes(self):
        collections = FcodeCore.try_get_all_codes(Exception)
        assert collections, "must have at least one errcode defined"
        self.INDEXED_ERRCODES = collections

        for id, errcodes in enumerate(collections):
            self._INDEXED_ACTIVE_ERRCODES.append(errcodes[0])
            for errcode in errcodes:
                self._ERRCODE_TO_ERRCODEID[errcode] = id

    async def postinit(self):
        # init mcodes and errcodes for the second time to catch up all defined
        # ones
        self._init_mcodes()
        self._init_errcodes()
        # restrict any further code defines since we start sending code data
        # to clients
        FcodeCore.deflock = True

        if self._initd_client_evt_mcodeid is None:
            mcodeid = self.try_get_mcodeid_for_mtype(InitdClientEvt)
            assert mcodeid is not None
            self._initd_client_evt_mcodeid = mcodeid

        if not self._preserialized_initd_client_evt:
            self._preserialized_initd_client_evt = InitdClientEvt(
                indexedMcodes=self.INDEXED_MCODES,
                indexedErrcodes=self.INDEXED_ERRCODES,
                rsid=None
            ).serialize_json(self._initd_client_evt_mcodeid)
        self._is_post_initd = True

    @classmethod
    async def destroy(cls):
        """
        Should be used only on server close or test interchanging.
        """
        bus = ServerBus.ie()

        if not bus._is_initd:  # noqa: SLF001
            return

        if bus._net_inp_queue_processor:  # noqa: SLF001
            bus._net_inp_queue_processor.cancel()  # noqa: SLF001
        if bus._net_out_queue_processor:  # noqa: SLF001
            bus._net_out_queue_processor.cancel()  # noqa: SLF001

        ServerBus.try_discard()

    async def conn(self, conn: Websocket) -> None:
        if not self._is_post_initd:
            await self.postinit()

        log.info(f"accept new conn {conn}", 2)

        try:
            connsid = eject(await self._read_first_wsmsg(conn))
            await conn.send_json(self._preserialized_initd_client_evt)
            await self._read_ws(connsid, conn)
        finally:
            if not conn.closed:
                await conn.close()
            if connsid in self._sid_to_conn_data:
                del self._sid_to_conn_data[connsid]

    async def _read_first_wsmsg(self, conn: Websocket) -> Res[str]:
        first_wsmsg = await conn.receive(self._cfg.register_timeout)
        connsid = RandomUtils.makeid()
        first_msg = await self._try_parse_wsmsg(connsid, first_wsmsg)
        if not first_msg:
            return Err(ValueErr("failed to parse first msg from"))
        if not isinstance(first_msg, RegisterReq):
            return Err(ValueErr(
                f"first msg should be RegisterReq, got {first_msg}"))

        if self._cfg.register is not None:
            register_res = await self._cfg.register(
                first_msg.tokens, first_msg.data)
            if isinstance(register_res, Err):
                return register_res

        # assign connsid only after receiving correct register req and
        # approving of it by the resource server
        self._sid_to_conn_data[connsid] = ConnData(
            conn=conn, tokens=first_msg.tokens)

        return Ok(connsid)

    async def _read_ws(self, connsid: str, conn: Websocket):
        async for wsmsg in conn:
            log.info(f"receive: {wsmsg}", 2)
            self._net_inp_connsid_and_wsmsg_queue.put_nowait((connsid, wsmsg))

    async def _process_net_inp_queue(self) -> None:
        while True:
            connsid, wsmsg = await self._net_inp_connsid_and_wsmsg_queue.get()
            msg = await self._try_parse_wsmsg(connsid, wsmsg)
            if msg is None:
                continue
            # publish to inner bus with no duplicate net resending
            await self.pub(msg, None, PubOpts(must_send_to_net=False))

    async def _try_parse_wsmsg(  # noqa: PLR0911
            self, connsid: str, wsmsg: WSMessage) -> Msg | None:
        try:
            rawmsg: dict = wsmsg.json()
        except Exception:
            log.err(f"unable to parse ws msg {wsmsg}")
            return None

        msid: str | None = rawmsg.get("msid", None)
        if not msid:
            log.err("msg without msid => skip silently")
            return None

        # for future rsid navigation
        # rsid: str | None = raw_msg.get("rsid", None)

        mcodeid: int | None = rawmsg.get("mcodeid", None)
        if mcodeid is None:
            await self.throw(
                ValueError(
                    f"got msg {rawmsg} with undefined mcodeid"
                )
            )
            return None
        if mcodeid < 0:
            await self.throw(
                ValueError(
                    f"invalid mcodeid {mcodeid}"
                )
            )
            return None
        if mcodeid > len(self._INDEXED_ACTIVE_MCODES) - 1:
            await self.throw(ValueError(
                f"unrecognized mcodeid {mcodeid}"
            ))
            return None

        t: type[Msg] | None = \
            FcodeCore.try_get_type_for_any_code(
                self._INDEXED_ACTIVE_MCODES[mcodeid]
            )
        assert t is not None, "if mcodeid found, mtype must be found"

        t = typing.cast(type[Msg], t)
        rawmsg["m_connsid"] = connsid
        try:
            msg = t.deserialize_json(rawmsg)
        except Exception as err:
            log.err_or_catch(err, 2)
            return None

        return msg

    async def _process_net_out_queue(self) -> None:
        while True:
            connsids, rawmsg = \
                await self._net_out_connsids_and_rawmsg_queue.get()

            log.info(f"send to connsids {connsids}: {rawmsg}", 2)
            coros: list[Coroutine] = [
                self._sid_to_conn_data[connsid].conn.send_json(rawmsg)
                for connsid in connsids
            ]
            await asyncio.gather(*coros)

    async def sub(
        self,
        mtype: type[TMsg],
        action: Subscriber,
        opts: SubOpts = SubOpts(),
    ) -> Callable:
        """
        Subscribes to certain message.

        Once the message is occured within the bus, the provided action is
        called.

        Args:
            mtype:
                Message type to subscribe to.
            action:
                Action to fire once the messsage has arrived.
            opts (optional):
                Subscription options.
        Returns:
            Unsubscribe function.
        """
        subsid = RandomUtils.makeid()
        subaction = _SubAction(
            sid=subsid, subscriber=typing.cast(Callable, action), opts=opts)

        if mtype not in self._mtype_to_subactions:
            self._mtype_to_subactions[mtype] = []
        self._mtype_to_subactions[mtype].append(subaction)
        self._subsid_to_subaction[subsid] = subaction
        self._subsid_to_mtype[subsid] = mtype

        if opts.must_receive_last_msg and mtype in self._type_to_last_msg:
            last_msg = self._type_to_last_msg[mtype]
            await self._try_call_subaction(
                subaction,
                last_msg
            )

        return functools.partial(self.unsub, subsid)

    async def unsub(self, subsid: str) -> Res[None]:
        if subsid not in self._subsid_to_mtype:
            return Err(ValueError(f"sub with id {subsid} not found"))

        assert self._subsid_to_mtype[subsid] in self._mtype_to_subactions

        msg_type = self._subsid_to_mtype[subsid]

        assert subsid in self._subsid_to_mtype, "all maps must be synced"
        assert subsid in self._subsid_to_subaction, "all maps must be synced"
        del self._subsid_to_mtype[subsid]
        del self._subsid_to_subaction[subsid]
        del self._mtype_to_subactions[msg_type]
        return Ok(None)

    async def unsub_many(
        self,
        sids: list[str],
    ) -> None:
        for sid in sids:
            await self.unsub(sid)

    async def pubr(
        self,
        req: Req,
        opts: PubOpts = PubOpts()
    ) -> Evt:
        """
        Publishes a message and awaits for the response.
        """
        aevt = asyncio.Event()
        pointer = Pointer(target=Evt(rsid=""))

        def wrapper(aevt: asyncio.Event, evtf_pointer: Pointer[Evt]):
            async def pubaction(_, evt: Evt):
                aevt.set()
                # set if even ErrEvt is returned. It will be handled outside
                # this wrapper, or ignored to be returned to the caller,
                # if opts.pubr_must_ignore_err_evt is given.
                evtf_pointer.target = evt

            return pubaction

        await self.pub(
            req,
            wrapper(aevt, pointer),
            opts
        )
        if opts.pubr_timeout is None:
            await aevt.wait()
        else:
            await asyncio.wait_for(aevt.wait(), opts.pubr_timeout)
        assert pointer.target
        assert \
            type(pointer.target) is not Evt, \
            "usage of base evt class detected," \
                " or probably pubr pubaction worked incorrectly"

        if (
            isinstance(pointer.target, ErrEvt)
            and not opts.pubr_must_ignore_err_evt
        ):
            final_err = pointer.target.inner_err
            if not final_err:
                log.warn(
                    f"on pubr got err evt {pointer.target} without"
                     " inner_err attached, which is strange and unexpected"
                     " => use default Exception"
                )
                final_err = Exception(pointer.target.errmsg)
            raise final_err

        return pointer.target

    async def pub(
        self,
        msg: Msg,
        pubaction: PubAction | None = None,
        opts: PubOpts = PubOpts(),
    ):
        if pubaction is not None and not isinstance(msg, Req):
            raise InpErr(f"for defined pubaction, {msg} should be req")

        if (
            isinstance(msg, Req)
            and pubaction is not None
        ):
            if msg.msid in self._rsid_to_req_and_pubaction:
                raise AlreadyProcessedErr(f"{msg} for pubr")
            self._rsid_to_req_and_pubaction[msg.msid] = (msg, pubaction)

        mtype = type(msg)
        self._type_to_last_msg[mtype] = msg

        # SEND ORDER
        #
        #   1. Net (only if has mcodeid and this is in the required list)
        #   2. Inner (always for every registered subaction)
        #   3. As a response (only if this msg type has the associated paction)

        if opts.must_send_to_net:
            await self._pub_to_net(mtype, msg, opts)

        if opts.must_send_to_inner and mtype in self._mtype_to_subactions:
            await self._send_to_inner_bus(mtype, msg)

        if isinstance(msg, Evt):
            await self._send_evt_as_response(msg)

    async def _send_to_inner_bus(self, mtype: type[TMsg], msg: TMsg):
        subactions = self._mtype_to_subactions[mtype]
        if not subactions:
            await self.throw(ValueErr(
                f"no subactions for msg type {mtype}"))
        removing_subaction_sids = []
        for subaction in subactions:
            await self._try_call_subaction(subaction, msg)
            if subaction.is_removing:
                removing_subaction_sids.append(subaction.sid)
        for removing_subaction_sid in removing_subaction_sids:
            (await self.unsub(removing_subaction_sid)).unwrap_or(None)

    async def _pub_to_net(
        self,
        mtype: type,
        msg: Msg,
        opts: PubOpts
    ):
        mcodeid: int | None = self.try_get_mcodeid_for_mtype(mtype)
        if mcodeid is not None:
            connsids: set[str] = set(msg.m_target_connsids)
            if connsids:
                connsids_to_del: set[str] = set()

                # clean unexistent connsids
                for connsid in connsids:
                    if connsid not in self._sid_to_conn_data:
                        log.err(
                            f"no conn with id {connsid}"
                            " => del from recipients"
                        )
                        connsids_to_del.add(connsid)
                        if opts.on_missing_connsid:
                            try:
                                await opts.on_missing_connsid(connsid)
                            except Exception as err:
                                log.err("during on_missing_connsid fn call"
                                        " the following err has occured:")
                                log.err_or_catch(err, 1)
                                continue
                for connsid in connsids_to_del:
                    connsids.remove(connsid)

                rawmsg = msg.serialize_json(mcodeid)
                self._net_out_connsids_and_rawmsg_queue.put_nowait(
                    (connsids, rawmsg)
                )

    async def _send_evt_as_response(self, evt: Evt):
        if not evt.rsid:
            return

        req_and_pubaction = self._rsid_to_req_and_pubaction.get(
            evt.rsid,
            None
        )

        if isinstance(evt, ErrEvt) and evt.isThrownByPubAction:
            # skip pubaction errs to avoid infinite msg loop
            return

        if req_and_pubaction is not None:
            req = req_and_pubaction[0]
            pubaction = req_and_pubaction[1]
            await self._try_call_pubaction(pubaction, req, evt)

    def _try_del_pubaction(self, rsid: str) -> bool:
        if rsid not in self._rsid_to_req_and_pubaction:
            return False
        del self._rsid_to_req_and_pubaction[rsid]
        return True

    async def _try_call_pubaction(
        self,
        pubaction: PubAction,
        req: Req,
        evt: Evt
    ) -> bool:
        f = True
        try:
            res = await pubaction(req, evt)
            if isinstance(res, Err):
                eject(res)
        except Exception as err:
            # technically, the msg which caused the err is evt, since on evt
            # the pubaction is finally called
            await self.throw(
                err,
                evt,
                m_to_connsids=req.m_target_connsids,
                is_thrown_by_pubaction=True
            )
            f = False
        if not evt.m_isContinious:
            self._try_del_pubaction(req.msid)
        return f

    def _get_ctx_dict_for_msg(self, msg: Msg) -> dict:
        ctx_dict = _rxcat_ctx.get().copy()

        if msg.m_connsid:
            ctx_dict["connsid"] = msg.m_connsid
        if isinstance(msg, ErrEvt):
            if msg.errcodeid is not None:
                errcode_res = self.get_errcode_for_errcodeid(msg.errcodeid)
                if isinstance(errcode_res, Ok):
                    ctx_dict["errcode"] = errcode_res.ok
        else:
            mcode = self.try_get_mcode_for_mtype(type(msg))
            if mcode is not None:
                ctx_dict["mcode"] = mcode

        ctx_dict["msg"] = msg.model_dump()

        return ctx_dict

    async def _call_subaction(self, subaction: _SubAction, msg: Msg):
        _rxcat_ctx.set(self._get_ctx_dict_for_msg(msg))

        res = await subaction.subscriber(msg)
        if isinstance(res, (Err, Ok)):
            val = eject(res)
            if isinstance(val, Msg):
                await self.pub(val)
            elif isinstance(val, list):
                for m in val:
                    if isinstance(m, Msg):
                        await self.pub(m)
                        continue
                    log.err(
                        f"subscriber #{subaction.sid} returned a list"
                        f" with a non-msg item: {m} => skip")
            elif val is not None:
                log.err(
                    f"subscriber #{subaction.sid} returned an unexpected"
                    f" object within the result: {val} => skip")

    async def _try_call_subaction(
        self,
        subaction: _SubAction,
        msg: Msg
    ) -> bool:
        filters = subaction.opts.filters
        for filter in filters:
            this_filter_f = await filter(msg)
            if not this_filter_f:
                return False

        try:
            await self._call_subaction(subaction, msg)
        except Exception as err:
            if isinstance(msg, ErrEvt):
                log.err(
                    f"ErrEvt subscriber has returned an err {err}, which may"
                    " result in an endless recursion => remove the subscriber,"
                    " but send this err further")
                # but subscriber must be removed carefully, to
                # not break outer iteration cycles
                subaction.is_removing = True
                return False
            await self.throw(err, msg)
            return False

        return True

    def try_get_mcodeid_for_mtype(self, mtype: type[Msg]) -> int | None:
        mcode: str | None = \
            FcodeCore.try_get_active_code_for_type(mtype)
        if mcode:
            mcodeid: int = self._MCODE_TO_MCODEID.get(mcode, -1)
            assert mcodeid != -1, "must find mcodeid for mcode"
            return mcodeid
        return None

    def try_get_mcode_for_mtype(self, mtype: type[Msg]) -> str | None:
        return FcodeCore.try_get_active_code_for_type(mtype)

    def try_get_mcode_for_mcodeid(self, mcodeid: int) -> str | None:
        for k, v in self._MCODE_TO_MCODEID.items():
            if v == mcodeid:
                return k
        return None

    def try_get_errcodeid_for_errtype(
        self, errtype: type[Exception]
    ) -> int | None:
        errcode: str | None = \
            FcodeCore.try_get_active_code_for_type(errtype)
        if errcode:
            errcodeid: int = self._ERRCODE_TO_ERRCODEID.get(errcode, -1)
            assert errcodeid != -1, "must find mcodeid for mcode"
            return errcodeid
        return None

    def  try_get_errcode_for_errtype(
        self, errtype: type[Exception]
    ) -> str | None:
        return FcodeCore.try_get_active_code_for_type(errtype)
