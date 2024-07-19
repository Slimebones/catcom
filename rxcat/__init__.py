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
import contextlib
import functools
import typing
from asyncio import Queue
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from inspect import isclass
from typing import (
    Any,
    ClassVar,
    Coroutine,
    Protocol,
    runtime_checkable,
)

from pydantic import BaseModel
from pykit.err import AlreadyProcessedErr, InpErr, NotFoundErr, ValErr
from pykit.fcode import FcodeCore, code
from pykit.log import log
from pykit.obj import get_fully_qualified_name
from pykit.pointer import Pointer
from pykit.rand import RandomUtils
from pykit.res import Res, eject
from pykit.singleton import Singleton
from result import Err, Ok, UnwrapError

from rxcat._code import CodeStorage
from rxcat._err import ErrDto
from rxcat._msg import (
    ErrEvt,
    Evt,
    InitdClientEvt,
    Msg,
    OkEvt,
    Req,
    TEvt,
    TMsg,
    TReq,
)
from rxcat._rpc import RpcEvt, RpcFn, RpcReq, TRpcFn
from rxcat._transport import (
    ActiveTransport,
    Conn,
    ConnArgs,
    OnRecvFn,
    OnSendFn,
    Transport,
)
from rxcat._udp import Udp
from rxcat._ws import Ws

__all__ = [
    "ServerBus",

    "ResourceServerErr",
    "RegisterFn",

    "Msg",
    "Req",
    "Evt",
    "ErrEvt",
    "OkEvt",

    "RpcReq",
    "RpcEvt",
    "RpcFn",
    "server_rpc",

    "Conn",
    "ConnArgs",
    "Transport",
    "Ws",
    "Udp",
    "OnSendFn",
    "OnRecvFn",

    "ErrDto",
]

# placed here and not at _rpc.py to avoid circulars
def server_rpc(code: str):
    def inner(target: TRpcFn) -> TRpcFn:
        ServerBus.register_rpc(code, target)
        return target
    return inner

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
class RegisterFn(Protocol):
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

_rxcat_ctx = ContextVar("rxcat_ctx", default={})
PubAction = Callable[[TReq, TEvt], Awaitable[Res[None] | None]]

@runtime_checkable
class CtxManager(Protocol):
    async def __aenter__(self): ...
    async def __aexit__(self, *args): ...

class ServerBusCfg(BaseModel):
    transports: list[Transport] | None = None
    """
    List of available transport mechanisms.

    For each transport the server bus will be able to accept incoming
    connections and treat them the same.

    "None" enables only default Websocket transport.

    The transports should be managed externally, and established connections
    are passed to ServerBus.conn, with ownership transfer.

    If ServerBus.conn receive connection not listed in this list, an error
    will be returned.
    """

    register_fn: RegisterFn | None = None
    """
    Function used to register client.

    Defaults to None. If None, all users are still required to send
    RegisterReq, but no alternative function will be called.
    """

    subaction_ctxfn: Callable[[Msg], Awaitable[CtxManager]] | None = None
    rpc_ctxfn: Callable[[RpcReq], Awaitable[CtxManager]] | None = None

    are_errs_catchlogged: bool = False
    """
    Whether to catch and reraise thrown to the bus errors.
    """

    class Config:
        arbitrary_types_allowed = True

class ServerBus(Singleton):
    """
    Rxcat server bus implementation.
    """
    _code_to_rpcfn: ClassVar[dict[str, RpcFn]] = {}
    DEFAULT_TRANSPORT: ClassVar[Transport] = Transport(
        is_server=True,
        conn_type=Ws,
        max_inp_queue_size=10000,
        max_out_queue_size=10000,
        protocol="ws",
        host="localhost",
        port=3000,
        route="rx"
    )

    def __init__(self):
        self._is_initd = False

    def get_ctx(self) -> dict:
        return _rxcat_ctx.get().copy()

    async def init(self, cfg: ServerBusCfg = ServerBusCfg()):
        self._cfg = cfg

        self._init_transports()

        FcodeCore.defcode("rxcat_fallback_err", Exception)
        # only server is able to index mcodes, client is not able to send
        # theirs mcodes on conn, so the server must know client codes at boot

        CodeStorage.update()

        self._sid_to_conn: dict[str, Conn] = {}

        self._subsid_to_mtype: dict[str, type[Msg]] = {}
        self._subsid_to_subaction: dict[str, _SubAction] = {}
        self._mtype_to_subactions: \
            dict[type[Msg], list[_SubAction]] = {}
        self._type_to_last_msg: dict[type[Msg], Msg] = {}

        self._preserialized_initd_client_evt: dict = {}
        self._initd_client_evt_mcodeid: int | None = None

        self._rsid_to_req_and_pubaction: dict[str, tuple[Req, PubAction]] = {}
        self._rsids_to_del_on_next_pubaction: set[str] = set()

        self._is_initd = True
        self._is_post_initd = False

        self._rpc_tasks: set[asyncio.Task] = set()

    def _init_transports(self):
        self._conn_type_to_atransport: dict[type[Conn], ActiveTransport] = {}
        transports = self._cfg.transports
        if not self._cfg.transports:
            transports = [self.DEFAULT_TRANSPORT]
        for transport in typing.cast(list[Transport], transports):
            if transport.conn_type in self._conn_type_to_atransport:
                log.err(
                    f"conn type {transport.conn_type} is already registered"
                    " => skip")
                continue
            if not transport.is_server:
                log.err(
                    f"only server transports are accepted, got {transport}"
                    " => skip")
                continue

            inp_queue = Queue(transport.max_inp_queue_size)
            out_queue = Queue(transport.max_out_queue_size)
            inp_task = asyncio.create_task(self._process_inp_queue(
                transport, inp_queue))
            out_task = asyncio.create_task(self._process_out_queue(
                transport, out_queue))
            atransport = ActiveTransport(
                transport=transport,
                inp_queue=inp_queue,
                out_queue=out_queue,
                inp_queue_processor=inp_task,
                out_queue_processor=out_task)
            self._conn_type_to_atransport[transport.conn_type] = atransport


    async def close_conn(self, sid: str) -> Res[None]:
        if sid not in self._sid_to_conn:
            return Err(NotFoundErr(f"conn with sid {sid}"))
        conn = self._sid_to_conn[sid]
        del self._sid_to_conn[sid]
        if not conn.is_closed:
            await conn.close()
        return Ok(None)

    @property
    def is_initd(self) -> bool:
        return self._is_initd

    @classmethod
    def register_rpc(cls, code: str, fn: RpcFn):
        if code in cls._code_to_rpcfn:
            log.err(f"rpc code {code} is already registered => skip")
            return
        cls._code_to_rpcfn[code] = fn

    def _checkthrow_norpc_msg(self, msg: Msg | type[Msg], ctx: str):
        """
        Since rpc msgs cannot participate in actions like "sub" and "pub",
        we have a separate fn to check this.
        """
        iscls = isclass(msg)
        if (
            (
                iscls and (issubclass(msg, RpcReq) or issubclass(msg, RpcEvt)))
            or (
                not iscls and (
                    isinstance(msg, (RpcEvt, RpcReq))))):
            raise ValErr(
                f"msg {msg} in context of \"{ctx}\" cannot be associated with"
                " rpc")

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
            err=ErrDto.create(
                err, CodeStorage.try_get_errcodeid_for_errtype(type(err))),
            inner__err=err,
            rsid=rsid,
            m_target_connsids=final_to_connsids,
            inner__is_thrown_by_pubaction=is_thrown_by_pubaction
        )

        log.err(f"thrown err evt: {evt}", 1)
        if self._cfg.are_errs_catchlogged:
            log.catch(err)
        await self.pub(evt, None, pub_opts)

    async def postinit(self):
        # update codes for the second time to catch up all defined
        # ones
        CodeStorage.update()
        # restrict any further code defines since we start sending code data
        # to clients
        FcodeCore.deflock = True

        if self._initd_client_evt_mcodeid is None:
            mcodeid = eject(CodeStorage.get_mcodeid_for_mtype(InitdClientEvt))
            self._initd_client_evt_mcodeid = mcodeid

        if not self._preserialized_initd_client_evt:
            self._preserialized_initd_client_evt = InitdClientEvt(
                indexedMcodes=CodeStorage.indexed_mcodes,
                indexedErrcodes=CodeStorage.indexed_errcodes,
                rsid=None
            ).serialize_json(self._initd_client_evt_mcodeid)
        self._is_post_initd = True

    @classmethod
    async def destroy(cls):
        """
        Should be used only on server close or test interchanging.
        """
        bus = ServerBus.ie()

        if not bus._is_initd: # noqa: SLF001
            return

        for atransport in bus._conn_type_to_atransport.values(): # noqa: SLF001
            atransport.inp_queue_processor.cancel()
            atransport.out_queue_processor.cancel()

        ServerBus.try_discard()

    async def conn(self, conn: Conn):
        if not self._is_post_initd:
            await self.postinit()

        atransport = self._conn_type_to_atransport.get(type(conn), None)
        if atransport is None:
            log.err(
                f"cannot find registered transport for conn {conn}"
                " => close conn")
            with contextlib.suppress(Exception):
                await conn.close()
        atransport = typing.cast(ActiveTransport, atransport)

        if conn.sid in self._sid_to_conn:
            log.err("conn with such sid already active => skip")
            return

        log.info(f"accept new conn {conn}", 2)
        self._sid_to_conn[conn.sid] = conn

        try:
            if atransport.transport.server__register_process == "register_req":
                eject(await self._read_first_msg(conn, atransport))
            await conn.send(self._preserialized_initd_client_evt)
            await self._read_ws(conn, atransport)
        finally:
            if not conn.is_closed:
                try:
                    await conn.close()
                except Exception as err:
                    log.err(
                        f"err {get_fully_qualified_name(err)} is raised"
                        f" during conn {conn} closing, #stacktrace")
                    log.catch(err)
            if conn.sid in self._sid_to_conn:
                del self._sid_to_conn[conn.sid]

    async def _receive_from_conn(
            self,
            conn: Conn,
            atransport: ActiveTransport) -> dict:
        try:
            return await asyncio.wait_for(
                conn.recv(),
                atransport.transport.inactivity_timeout)
        except TimeoutError as err:
            raise TimeoutError(
                f"inactivity of conn {conn} for transport {atransport}"
            ) from err

    async def _read_first_msg(
            self, conn: Conn, atransport: ActiveTransport) -> Res:
        rmsg = await self._receive_from_conn(conn, atransport)
        msg = await self.parse_rmsg(rmsg, conn)
        if not msg:
            return Err(ValErr("failed to parse first msg from"))
        if not isinstance(msg, RegisterReq):
            return Err(ValErr(
                f"first msg should be RegisterReq, got {msg}"))

        register_res = Ok(None)
        if self._cfg.register_fn is not None:
            register_res = await self._cfg.register_fn(
                msg.tokens, msg.data)
            if isinstance(register_res, Err):
                return register_res

        # TODO:
        #   send back register data for the client if register_res is Ok
        return register_res

    async def _read_ws(self, conn: Conn, atransport: ActiveTransport):
        async for rmsg in conn:
            log.info(f"receive: {rmsg}", 2)
            atransport.inp_queue.put_nowait((conn, rmsg))

    async def _process_inp_queue(
            self,
            transport: Transport,
            queue: Queue[tuple[Conn, dict]]):
        while True:
            conn, rmsg = await queue.get()
            if transport.on_recv:
                with contextlib.suppress(Exception):
                    # we don't pass whole conn to avoid control leaks
                    await transport.on_recv(conn.sid, rmsg)
            msg = await self.parse_rmsg(rmsg, conn)
            await self._accept_net_msg(msg)

    async def _process_out_queue(
            self,
            transport: Transport,
            queue: Queue[tuple[Conn, dict]]):
        while True:
            conn, rmsg = await queue.get()

            if transport.on_send:
                with contextlib.suppress(Exception):
                    await transport.on_send(conn.sid, rmsg)

            log.info(f"send to connsid {conn.sid}: {rmsg}", 2)

            await conn.send(rmsg)

    async def _accept_net_msg(self, msg: Msg | None):
        if msg is None:
            return
        elif isinstance(msg, RpcEvt):
            log.err(
                f"server bus won't accept RpcEvt messages, got {msg}"
                " => skip")
            return
        elif isinstance(msg, RpcReq):
            # process rpc in a separate task to not block inp queue
            # processing
            task = asyncio.create_task(self._call_rpc(msg))
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)
            return
        # publish to inner bus with no duplicate net resending
        await self.pub(msg, None, PubOpts(must_send_to_net=False))

    async def _call_rpc(self, req: RpcReq):
        code, _ = req.key.split(":")
        if code not in self._code_to_rpcfn:
            log.err(f"no such rpc code {code} for req {req} => skip")
            return
        fn = self._code_to_rpcfn[code]

        _rxcat_ctx.set(self._get_ctx_dict_for_msg(req))

        ctx_manager: CtxManager | None = None
        if self._cfg.rpc_ctxfn is not None:
            try:
                ctx_manager = await self._cfg.rpc_ctxfn(req)
            except Exception as err:
                log.err(
                    f"err {get_fully_qualified_name(err)} is occured"
                    f" during rpx ctx manager retrieval for req {req} => skip")
                log.catch(err)
                return
        try:
            if ctx_manager:
                async with ctx_manager:
                    res = await fn(req.kwargs)
            else:
                res = await fn(req.kwargs)
        except Exception as err:
            log.warn(
                f"unhandled exception occured for rpcfn on req {req}"
                " => wrap it to usual RpcEvt;"
                f" exception {get_fully_qualified_name(err)}")
            log.catch(err)
            res = Err(err)

        val: Any
        if isinstance(res, Ok):
            val = res.ok_value
        elif isinstance(res, Err):
            val = ErrDto.create(
                res.err_value,
                CodeStorage.try_get_errcodeid_for_errtype(type(res.err_value)))
        else:
            log.err(
                f"rpcfn on req {req} returned non-res val {res} => skip")
            return

        evt = RpcEvt(rsid=None, key=req.key, val=val).as_res_from_req(req)
        await self._pub_to_net(type(evt), evt)

    async def parse_rmsg(
            self, rmsg: dict, conn: Conn) -> Msg | None:

        msid: str | None = rmsg.get("msid", None)
        if not msid:
            log.err("msg without msid => skip silently")
            return None

        # for future rsid navigation
        # rsid: str | None = rmsg.get("rsid", None)

        mcodeid: int | None = rmsg.get("mcodeid", None)
        if mcodeid is None:
            await self.throw(
                ValErr(
                    f"got msg {rmsg} with undefined mcodeid"
                )
            )
            return None
        if mcodeid < 0:
            await self.throw(
                ValErr(
                    f"invalid mcodeid {mcodeid}"
                )
            )
            return None
        if mcodeid > len(CodeStorage.indexed_active_mcodes) - 1:
            await self.throw(ValErr(
                f"unrecognized mcodeid {mcodeid}"
            ))
            return None

        t: type[Msg] | None = \
            FcodeCore.try_get_type_for_any_code(
                CodeStorage.indexed_active_mcodes[mcodeid])
        assert t is not None, "if mcodeid found, mtype must be found"

        t = typing.cast(type[Msg], t)
        rmsg["m_connsid"] = conn.sid
        try:
            msg = t.deserialize_json(rmsg)
        except Exception as err:
            log.err_or_catch(err, 2)
            return None

        return msg

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
        self._checkthrow_norpc_msg(mtype, "subscription")
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
            return Err(ValErr(f"sub with id {subsid} not found"))

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
            final_err = pointer.target.inner__err
            if not final_err:
                log.warn(
                    f"on pubr got err evt {pointer.target} without"
                     " inner_err attached, which is strange and unexpected"
                     " => use default Exception"
                )
                final_err = Exception(pointer.target.err.msg)
            raise final_err

        return pointer.target

    async def pub(
        self,
        msg: Msg,
        pubaction: PubAction | None = None,
        opts: PubOpts = PubOpts(),
    ):
        self._checkthrow_norpc_msg(msg, "publication")
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
            await self.throw(ValErr(
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
        opts: PubOpts = PubOpts()
    ):
        mcodeid_res = CodeStorage.get_mcodeid_for_mtype(mtype)
        if isinstance(mcodeid_res, Ok) and msg.m_target_connsids:
            mcodeid = mcodeid_res.ok_value
            rmsg = msg.serialize_json(mcodeid)
            for connsid in msg.m_target_connsids:
                if connsid not in self._sid_to_conn:
                    log.err(
                        f"no conn with id {connsid} for msg {msg}"
                        " => skip"
                    )
                    # do we really need this?
                    if opts.on_missing_connsid:
                        try:
                            await opts.on_missing_connsid(connsid)
                        except Exception as err:
                            log.err(
                                "during on_missing_consid fn call err"
                                f" {get_fully_qualified_name(err)}"
                                " #stacktrace")
                            log.catch(err)
                    continue
                conn = self._sid_to_conn[connsid]
                conn_type = type(conn)
                # if we have conn in self._sid_to_conn, we must have transport
                if conn_type not in self._conn_type_to_atransport:
                    log.err("broken state of conn_type_to_atransport => skip")
                    continue
                atransport = self._conn_type_to_atransport[conn_type]
                await atransport.out_queue.put((conn, rmsg))

    async def _send_evt_as_response(self, evt: Evt):
        if not evt.rsid:
            return

        req_and_pubaction = self._rsid_to_req_and_pubaction.get(
            evt.rsid,
            None
        )

        if isinstance(evt, ErrEvt) and evt.inner__is_thrown_by_pubaction:
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

        return ctx_dict

    async def _call_subaction(self, subaction: _SubAction, msg: Msg):
        _rxcat_ctx.set(self._get_ctx_dict_for_msg(msg))

        if self._cfg.subaction_ctxfn is not None:
            ctx_manager = await self._cfg.subaction_ctxfn(msg)
            async with ctx_manager:
                res = await subaction.subscriber(msg)
        else:
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
