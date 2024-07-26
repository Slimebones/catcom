"""
Rxcat implementation for Python.
"""

import asyncio
import contextlib
import functools
import typing
from asyncio import Queue
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from inspect import isclass, signature
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterable,
    Protocol,
    runtime_checkable,
)

from pydantic import BaseModel
from pykit.code import Code, Coded
from pykit.err import AlreadyProcessedErr, ErrDto, NotFoundErr, ValErr
from pykit.err_utils import create_err_dto
from pykit.log import log
from pykit.ptr import Ptr
from pykit.res import Err, Ok, Res, Result, UnwrapErr, aresultify, valerr
from pykit.singleton import Singleton
from pykit.uuid import uuid4

from rxcat._msg import (
    Mbody,
    Msg,
    TMbody_contra,
    Welcome,
    ok,
)
from rxcat._rpc import EmptyRpcArgs, RpcFn, SrpcRecv, SrpcSend
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
    "SubFn",
    "ok",

    "ResourceServerErr",

    "Mbody",

    "RpcFn",
    "srpc",
    "SrpcSend",
    "SrpcRecv",
    "EmptyRpcArgs",
    "StaticCodeid",

    "Conn",
    "ConnArgs",
    "Transport",
    "Ws",
    "Udp",
    "OnSendFn",
    "OnRecvFn",

    "PubList",
    "InterruptPipeline",
    "SkipMe",

    "sub"
]

class StaticCodeid:
    """
    Static codeids defined by Rxcat protocol.
    """
    Welcome = 0
    Ok = 1

SubFnRetval = Mbody | Iterable[Mbody] | None
@runtime_checkable
class SubFn(Protocol, Generic[TMbody_contra]):
    async def __call__(self, body: TMbody_contra) -> SubFnRetval: ...

def sub(target: SubFn):
    ServerBus.subfn_init_queue.add(target)
    def inner(*args, **kwargs) -> Any:
        return target(*args, **kwargs)
    return inner

# placed here and not at _rpc.py to avoid circulars
def srpc(target: RpcFn):
    ServerBus.reg_rpc(target).eject()
    def inner(*args, **kwargs) -> Any:
        return target(*args, **kwargs)
    return inner

class PubList(list[Mbody]):
    """
    List of mbody to be published.

    Useful as retval from subfn to signify that this list should be unpacked
    and each item be published.
    """

class SkipMe:
    """
    Used by subscribers to prevent any actions on it's retval, since
    returning None will cause bus to publish Ok(None).
    """

class InterruptPipeline:
    def __init__(self, body: Mbody) -> None:
        self.body = body

class ResourceServerErr(Exception):
    @staticmethod
    def code() -> str:
        return "rxcat::resource_server_err"

class Internal__InvokedActionUnhandledErr(Exception):
    def __init__(self, action: Callable, err: Exception):
        super().__init__(
            f"invoked {action} unhandled err: {err!r}"
        )

class Internal__BusUnhandledErr(Exception):
    def __init__(self, err: Exception):
        super().__init__(
            f"bus unhandled err: {err}"
        )

class PubOpts(BaseModel):
    subfn: SubFn | None = None

    target_connsids: list[str] | None = None
    """
    Connection sids to publish to.

    Defaults to only ctx connsid, if it exists.
    """

    lsid: str | None = None
    """
    Lsid to be used in the msg.

    Available operators:
        - $ctx::msid - use "msid" field of the ctx as lsid
    """

    send_to_inner: bool = True
    """
    Whether to send to inner bus subscribers.
    """

    send_to_net: bool = True
    """
    Will send to net if True and code is defined for the msg passed.
    """

    pubr__timeout: float | None = None
    """
    Timeout of awaiting for published message response arrival. Defaults to
    None, which means no timeout is set.
    """

    class Config:
        arbitrary_types_allowed = True

MbodyCondition = Callable[[Mbody], Awaitable[bool]]
MbodyFilter = Callable[[Mbody], Awaitable[Mbody]]
SubFnRetvalFilter = Callable[[SubFnRetval], Awaitable[SubFnRetval]]

class SubOpts(BaseModel):
    recv_last_msg: bool = True
    """
    Whether to receive last stored msg with the same body code.
    """
    conditions: Iterable[MbodyCondition] | None = None
    """
    Conditions that must be true in order for the subscriber to be called.

    Are applied to the body only after passing it through ``in_filters``.

    If all conditions fail for a subscriber, it is skipped completely
    (returns RetState.SkipMe).
    """
    inp_filters: Iterable[MbodyFilter] | None = None
    out_filters: Iterable[SubFnRetvalFilter] | None = None

    warn_unconventional_subfn_names: bool = True

_rxcat_ctx = ContextVar("rxcat", default={})

@runtime_checkable
class CtxManager(Protocol):
    async def __aenter__(self): ...
    async def __aexit__(self, *args): ...

class ServerBusCfg(BaseModel):
    """
    Global subfn functions are applied **before** local ones passed to SubOpts.
    """

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

    reg_types: Iterable[type] | None = None
    """
    Types to register on bus initialization.
    """

    sub_ctxfn: Callable[[Msg], Awaitable[Res[CtxManager]]] | None = None
    rpc_ctxfn: Callable[[SrpcSend], Awaitable[Res[CtxManager]]] | None = None

    trace_errs_on_pub: bool = True
    log_net_send: bool = True
    log_net_recv: bool = True

    global_subfn_conditions: Iterable[MbodyCondition] | None = None
    global_subfn_inp_filters: Iterable[MbodyFilter] | None = None
    global_subfn_out_filters: Iterable[SubFnRetvalFilter] | None = None

    consider_sub_decorators: bool = True

    class Config:
        arbitrary_types_allowed = True

class ServerBus(Singleton):
    """
    Rxcat server bus implementation.
    """
    subfn_init_queue: ClassVar[set[SubFn]] = set()
    """
    Queue of subscription functions to be subscribed on bus's initialization.

    Is not cleared, so after bus recreation, it's no need to reimport all subs.

    Using this queue can be disabled by cfg.consider_sub_decorators.
    """
    _rpccode_to_fn: ClassVar[dict[str, tuple[RpcFn, type[BaseModel]]]] = {}
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
    DEFAULT_CODE_ORDER: ClassVar[list[str]] = [
        "rxcat::welcome",
        "rxcat::ok"
    ]

    def __init__(self):
        self._is_initd = False

    def get_conn_tokens(
            self, connsid: str) -> Res[list[str]]:
        conn = self._sid_to_conn.get(connsid, None)
        if conn is None:
            return valerr(f"no conn with sid {connsid}")
        return Ok(conn.get_tokens())

    def set_conn_tokens(
            self, connsid: str, tokens: list[str]) -> Res[None]:
        conn = self._sid_to_conn.get(connsid, None)
        if conn is None:
            return valerr(f"no conn with sid {connsid}")
        conn.set_tokens(tokens)
        return Ok(None)

    def get_ctx_conn_tokens(self) -> Res[list[str]]:
        connsid_res = self.get_ctx_connsid()
        if isinstance(connsid_res, Err):
            return connsid_res
        return self.get_conn_tokens(connsid_res.okval)

    def set_ctx_conn_tokens(
            self, tokens: list[str]) -> Res[None]:
        connsid_res = self.get_ctx_connsid()
        if isinstance(connsid_res, Err):
            return connsid_res
        return self.set_conn_tokens(connsid_res.okval, tokens)

    async def close_conn(self, connsid: str) -> Res[None]:
        conn = self._sid_to_conn.get(connsid, None)
        if conn is None:
            return valerr(f"no conn with sid {connsid}")
        if conn.is_closed:
            return valerr("already closed")
        if connsid in self._sid_to_conn:
            del self._sid_to_conn[connsid]
        return await aresultify(conn.close())

    def get_ctx(self) -> dict:
        return _rxcat_ctx.get().copy()

    async def init(self, cfg: ServerBusCfg = ServerBusCfg()):
        if self._is_initd:
            return

        self._cfg = cfg

        self._init_transports()

        self._sid_to_conn: dict[str, Conn] = {}

        self._subsid_to_code: dict[str, str] = {}
        self._subsid_to_subfn: dict[str, SubFn] = {}
        self._code_to_subfns: dict[str, list[SubFn]] = {}
        self._code_to_last_mbody: dict[str, Mbody] = {}

        self._preserialized_welcome_msg: dict = {}

        self._lsid_to_subfn: dict[str, SubFn] = {}
        """
        Subscribers awaiting arrival of linked message.
        """
        self._lsids_to_del_on_next_pubfn: set[str] = set()

        self._is_initd = True
        self._is_post_initd = False

        self._rpc_tasks: set[asyncio.Task] = set()

        reg_types = [] if cfg.reg_types is None else cfg.reg_types
        (await self.reg_types([
            Welcome,
            ok,
            SrpcSend,
            SrpcRecv,
            ValErr,
            NotFoundErr,
            Coded(
                code="exception",
                val=Exception),
            Coded(
                code="value_error",
                val=ValueError),
            Coded(
                code="type_error",
                val=TypeError),
            Coded(
                code="system_error",
                val=SystemError),
            Coded(
                code="key_error",
                val=KeyError),
            Coded(
                code="attribute_error",
                val=AttributeError),
            *reg_types
        ])).eject()

        if self._cfg.consider_sub_decorators:
            for subfn in self.subfn_init_queue:
                (await self.sub(subfn)).eject()

    @property
    def is_initd(self) -> bool:
        return self._is_initd

    @classmethod
    async def get_regd_type(cls, code: str) -> Res[type]:
        return await Code.get_regd_type_by_code(code)

    async def reg_types(
            self, types: Iterable[type | Coded[type]]) -> Res[None]:
        """
        Reg codes for types.

        No err is raised on existing code redefinition. Err is printed on
        invalid codes.

        Be careful with this method, once called it enables a lock on msg
        serialization and other processes for the time of codes modification.
        Also, after the reging, all the clients gets notified about
        the changed codes with the repeated welcome message.

        So it's better to be called once and at the start of the program.
        """
        if not self._is_initd:
            return valerr("bus should be initialized")
        upd_res = await Code.upd(types, self.DEFAULT_CODE_ORDER)
        if isinstance(upd_res, Err):
            return upd_res
        return await self._set_welcome()

    @classmethod
    def reg_rpc(cls, fn: RpcFn) -> Res[None]:
        """
        Regs server rpc (srpc).
        """
        rpc_code = fn.__name__ # type: ignore

        if rpc_code in cls._rpccode_to_fn:
            return Err(ValErr(f"rpc code {rpc_code} is already regd"))
        if not rpc_code.startswith("srpc__"):
            return Err(ValErr(f"code {rpc_code} must start with \"srpc__\""))

        sig = signature(fn)
        sig_param = sig.parameters.get("body")
        if not sig_param:
            return Err(ValErr(
                f"rpc fn {fn} with code {rpc_code} must accept"
                " \"body: AnyBaseModel\" as it's sole argument"))
        args_type = sig_param.annotation
        if args_type is BaseModel:
            return Err(ValErr(
                f"rpc fn {fn} with code {rpc_code} cannot declare BaseModel"
                " as it's direct args type"))
        if not issubclass(args_type, BaseModel):
            return Err(ValErr(
                f"rpc fn {fn} with code {rpc_code} must accept args in form"
                f" of BaseModel, got {args_type}"))

        cls._rpccode_to_fn[rpc_code] = (fn, args_type)
        return Ok(None)

    async def postinit(self):
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

        cls._rpccode_to_fn.clear()
        Code.destroy()

        ServerBus.try_discard()

    async def conn(self, conn: Conn):
        if not self._is_post_initd:
            await self.postinit()

        atransport = self._conn_type_to_atransport.get(type(conn), None)
        if atransport is None:
            log.err(
                f"cannot find regd transport for conn {conn}"
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
            await conn.send(self._preserialized_welcome_msg)
            await self._read_ws(conn, atransport)
        except Exception as err:
            await log.atrack(err, f"during conn {conn} main loop => close")
        finally:
            if not conn.is_closed():
                try:
                    await conn.close()
                except Exception as err:
                    await log.atrack(err, f"during conn {conn} closing")
            if conn.sid in self._sid_to_conn:
                del self._sid_to_conn[conn.sid]

    def _get_bodytype_from_subfn(
            self, subfn: SubFn[TMbody_contra]) -> Res[type[TMbody_contra]]:
        sig = signature(subfn)
        params = list(sig.parameters.values())
        if len(params) != 1:
            return valerr(
                f"subfn {subfn} must accept one argument, got {len(params)}")
        param = params[0]
        assert isclass(param.annotation)
        return Ok(param.annotation)

    async def sub(
        self,
        subfn: SubFn[TMbody_contra],
        opts: SubOpts = SubOpts(),
    ) -> Res[Callable[[], Awaitable[Res[None]]]]:
        """
        Subscribes to certain message.

        Once the message is occured within the bus, the provided action is
        called.

        Args:
            subfn:
                Function to fire once the messsage has arrived.
            opts (optional):
                Subscription options.
        Returns:
            Unsubscribe function.
        """
        bodytype_res = self._get_bodytype_from_subfn(subfn)
        if isinstance(bodytype_res, Err):
            return bodytype_res
        bodytype = bodytype_res.okval

        if (
                not subfn.__name__.startswith("sub__")  # type: ignore
                and opts.warn_unconventional_subfn_names):
            log.warn(f"prefix subscription function {subfn} with \"sub__\"")

        r = self._check_norpc_mbody(bodytype, "subscription")
        if isinstance(r, Err):
            return r
        subsid = uuid4()
        subfn = self._apply_opts_to_subfn(subfn, opts)

        if not isclass(bodytype):
            return valerr(f"bodytype {bodytype} should be a class")
        code_res = Code.get_from_type(bodytype)
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval

        if not Code.has_code(code):
            return valerr(f"code \"{code}\" is not regd")

        if code not in self._code_to_subfns:
            self._code_to_subfns[code] = []
        self._code_to_subfns[code].append(subfn)
        self._subsid_to_subfn[subsid] = subfn
        self._subsid_to_code[subsid] = code

        if opts.recv_last_msg and code in self._code_to_last_mbody:
            last_body = self._code_to_last_mbody[code]
            await self._call_subfn(subfn, last_body)

        return Ok(functools.partial(self.unsub, subsid))

    async def unsub(self, subsid: str) -> Res[None]:
        if subsid not in self._subsid_to_code:
            return Err(ValErr(f"sub with id {subsid} not found"))

        assert self._subsid_to_code[subsid] in self._code_to_subfns

        msg_type = self._subsid_to_code[subsid]

        assert subsid in self._subsid_to_code, "all maps must be synced"
        assert subsid in self._subsid_to_subfn, "all maps must be synced"
        del self._subsid_to_code[subsid]
        del self._subsid_to_subfn[subsid]
        del self._code_to_subfns[msg_type]
        return Ok(None)

    async def unsub_many(
        self,
        sids: list[str],
    ) -> None:
        for sid in sids:
            (await self.unsub(sid)).ignore()

    async def pubr(
        self,
        body: Mbody,
        opts: PubOpts = PubOpts()
    ) -> Res[Mbody]:
        """
        Publishes a message and awaits for the response.

        If the response is Exception, it is wrapped to res::Err.
        """
        aevt = asyncio.Event()
        ptr: Ptr[Mbody] = Ptr(target=None)

        def wrapper(aevt: asyncio.Event, ptr: Ptr[Mbody]):
            async def fn(body: Mbody):
                aevt.set()
                ptr.target = body
            return fn

        if opts.subfn is not None:
            log.warn("don't pass PubOpts.subfn to pubr, it gets overwritten")
        opts.subfn = wrapper(aevt, ptr)
        pub_res = await self.pub(body, opts)
        if isinstance(pub_res, Err):
            return pub_res
        if opts.pubr__timeout is None:
            await aevt.wait()
        else:
            try:
                await asyncio.wait_for(aevt.wait(), opts.pubr__timeout)
            except asyncio.TimeoutError as err:
                return Err(err)

        if (isinstance(ptr.target, Exception)):
            return Err(ptr.target)

        return Ok(ptr.target)

    def get_ctx_key(self, key: str) -> Res[Any]:
        val = _rxcat_ctx.get().get(key, None)
        if val:
            return Ok(val)
        return Err(NotFoundErr(f"\"{key}\" entry in rxcat ctx"))

    def get_ctx_connsid(self) -> Res[str]:
        return self.get_ctx_key("connsid")

    async def pub(
            self,
            body: Mbody | Result | Msg,
            opts: PubOpts = PubOpts()) -> Res[None]:
        """
        Publishes body to the bus.

        For received UnwrapErr, it's res.errval will be used.

        Received Exceptions are additionally logged if
        cfg.trace_errs_on_pub == True.

        Passed Result will be fetched for the value.

        Passing rxcat::Msg is restricted to internal usage.
        """
        if isinstance(body, Ok):
            body = body.okval
        elif isinstance(body, Err):
            body = body.errval

        if isinstance(body, Msg):
            msg = body
            body = msg.body
            code = msg.skip__bodycode
        else:
            msg_res = self._make_msg(body, opts)
            if isinstance(msg_res, Err):
                return msg_res
            msg = msg_res.okval
            code = msg.skip__bodycode

        r = self._check_norpc_mbody(msg, "publication")
        if isinstance(r, Err):
            return r

        if opts.subfn is not None:
            if msg.sid in self._lsid_to_subfn:
                return Err(AlreadyProcessedErr(f"{msg} for pubr"))
            self._lsid_to_subfn[msg.sid] = opts.subfn

        self._code_to_last_mbody[code] = body

        await self._exec_pub_send_order(msg, opts)
        return Ok(None)

    def _unpack_err(self, body: Exception, track: bool) -> Mbody:
        if isinstance(body, Exception):
            if isinstance(body, UnwrapErr):
                res = body.result
                assert isinstance(res, Err)
                if isinstance(res.errval, Exception):
                    body = res.errval
                else:
                    body = ResourceServerErr(
                        f"got res with err value {res.errval},"
                        " which is not an instance of Exception")
            if track:
                log.track(body, "unpack err")
        return body

    def _unpack_lsid(self, lsid: str | None) -> Res[str | None]:
        if lsid == "$ctx::msid":
            # by default we publish as response to current message, so we
            # use the current's message sid as linked sid
            msid_res = self.get_ctx_key("msid")
            if isinstance(msid_res, Err):
                return msid_res
            lsid = msid_res.okval
            assert isinstance(lsid, str)
        elif isinstance(lsid, str) and lsid.startswith("$"):
            return valerr(f"unrecognized PubOpts.lsid operator: {lsid}")
        return Ok(lsid)

    def _make_msg(
            self, body: Mbody, opts: PubOpts = PubOpts()) -> Res[Msg]:
        code_res = Code.get_from_type(type(body))
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval
        if not Code.has_code(code):
            return valerr(f"code {code} is not registered")

        body = self._unpack_err(body, self._cfg.trace_errs_on_pub)

        lsid_res = self._unpack_lsid(opts.lsid)
        if isinstance(lsid_res, Err):
            return lsid_res
        lsid = lsid_res.okval

        target_connsids = None
        if opts.target_connsids:
            target_connsids = opts.target_connsids
        else:
            # try to get ctx connsid, otherwise left as none
            connsid_res = self.get_ctx_key("connsid")
            if isinstance(connsid_res, Ok):
                assert isinstance(connsid_res.okval, str)
                target_connsids = [connsid_res.okval]

        return Ok(Msg(
            lsid=lsid,
            skip__bodycode=code,
            body=body,
            skip__target_connsids=target_connsids))

    async def _exec_pub_send_order(self, msg: Msg, opts: PubOpts):
        # SEND ORDER
        #
        #   1. Net
        #   2. Inner
        #   3. As a response

        if opts.send_to_net:
            await self._pub_msg_to_net(msg)
        if opts.send_to_inner and msg.skip__bodycode in self._code_to_subfns:
            await self._send_to_inner_bus(msg)
        if msg.lsid:
            await self._send_as_linked(msg)

    async def _send_to_inner_bus(self, msg: Msg):
        subfns = self._code_to_subfns[msg.skip__bodycode]
        if not subfns:
            return
        for subfn in subfns:
            await self._call_subfn(subfn, msg)

    async def _pub_msg_to_net(self, msg: Msg):
        if msg.skip__target_connsids:
            rmsg = (await msg.serialize_to_net()).unwrap_or(None)
            if rmsg is None:
                return
            await self._pub_rmsg_to_net(rmsg, msg.skip__target_connsids)

    async def _pub_rmsg_to_net(self, rmsg: dict, connsids: Iterable[str]):
        for connsid in connsids:
            if connsid not in self._sid_to_conn:
                log.err(
                    f"no conn with id {connsid} for rmsg {rmsg}"
                    " => skip")
                continue
            conn = self._sid_to_conn[connsid]
            conn_type = type(conn)
            # if we have conn in self._sid_to_conn, we must have transport
            if conn_type not in self._conn_type_to_atransport:
                log.err("broken state of conn_type_to_atransport => skip")
                continue
            atransport = self._conn_type_to_atransport[conn_type]
            await atransport.out_queue.put((conn, rmsg))

    async def _send_as_linked(self, msg: Msg):
        if not msg.lsid:
            return
        subfn = self._lsid_to_subfn.get(msg.lsid, None)
        if subfn is not None:
            await self._call_subfn(subfn, msg)

    def _try_del_subfn(self, lsid: str) -> bool:
        if lsid not in self._lsid_to_subfn:
            return False
        del self._lsid_to_subfn[lsid]
        return True

    def _gen_ctx_dict_for_msg(self, msg: Msg) -> dict:
        ctx_dict = _rxcat_ctx.get().copy()

        ctx_dict["msid"] = msg.sid
        if msg.skip__connsid:
            ctx_dict["connsid"] = msg.skip__connsid

        return ctx_dict

    async def _call_subfn(self, subfn: SubFn, msg: Msg):
        """
        Calls subfn and pubs any response captured (including errors).

        Note that even None response is published as ok(None).
        """
        _rxcat_ctx.set(self._gen_ctx_dict_for_msg(msg))

        if self._cfg.sub_ctxfn is not None:
            try:
                ctx_manager = (await self._cfg.sub_ctxfn(msg)).eject()
            except Exception as err:
                await log.atrack(
                    err, f"rpx ctx manager retrieval for body {msg.body}")
                return
            async with ctx_manager:
                retval = await subfn(msg.body)
        else:
            retval = await subfn(msg.body)

        vals = self._parse_subfn_retval(subfn, retval)
        if not vals:
            return

        # by default all subsriber's body are intended to be linked to
        # initial message, so we attach this message ctx msid
        lsid = _rxcat_ctx.get().get("subfn_lsid", "$ctx::msid")
        pub_opts = PubOpts(lsid=lsid)
        for val in vals:
            if val is None:
                val = ok()
            await (await self.pub(val, pub_opts)).atrack(
                f"during subfn {subfn} retval publication")

    def _parse_subfn_retval(
            self,
            subfn: SubFn,
            retval: SubFnRetval) -> Iterable[Mbody]:
        # unpack here, though it can be done inside pub(), but we want to
        # process iterables here
        if isinstance(retval, Ok):
            retval = retval.okval
        if isinstance(retval, Err):
            retval = retval.errval

        if isinstance(retval, SkipMe):
            return []
        if isinstance(retval, InterruptPipeline):
            log.err(
                f"retval {retval} cannot be returned from subfn {subfn}")
            return []
        if isclass(retval):
            log.err(f"subfn {subfn} shouldn't return class {retval}")
            return []

        vals = []
        if isinstance(retval, PubList):
            vals = retval
        else:
            vals = [retval]

        return vals

    def set_ctx_subfn_lsid(self, lsid: str | None):
        """
        Can be used to change subfn lsid behaviour.

        Useful at ``SubOpts.out_filters``, see ``disable_subfn_lsid``.
        """
        ctx_dict = _rxcat_ctx.get().copy()
        ctx_dict["subfn__lsid"] = lsid

    def _check_norpc_mbody(
            self, body: Mbody | type[Mbody], disp_ctx: str) -> Res[None]:
        """
        Since rpc msgs cannot participate in actions like "sub" and "pub",
        we have a separate fn to check this.
        """
        iscls = isclass(body)
        if (
            (
                iscls
                and (issubclass(body, SrpcSend) or issubclass(body, SrpcRecv)))
            or (
                not iscls
                and (isinstance(body, (SrpcSend, SrpcRecv))))):
            return Err(ValErr(
                f"mbody {body} in context of \"{disp_ctx}\" cannot be"
                " associated with rpc"))
        return Ok(None)

    def _apply_opts_to_subfn(
            self, subfn: SubFn, opts: SubOpts) -> SubFn:
        async def wrapper(body: Mbody) -> Any:
            # globals are applied before locals
            inp_filters = [
                *(self._cfg.global_subfn_inp_filters or []),
                *(opts.inp_filters or [])
            ]
            conditions = [
                *(self._cfg.global_subfn_conditions or []),
                *(opts.conditions or [])
            ]
            out_filters = [
                *(self._cfg.global_subfn_out_filters or []),
                *(opts.out_filters or [])
            ]

            for f in inp_filters:
                body = await f(body)
                if isinstance(body, InterruptPipeline):
                    return body.body

            for f in conditions:
                flag = await f(body)
                # if any condition fails, skip the subfn
                if not flag:
                    return SkipMe()

            retbody = await subfn(body)

            for f in out_filters:
                retbody = await f(retbody)
                if isinstance(retbody, InterruptPipeline):
                    return retbody.body

            return retbody
        return wrapper

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
            if self._cfg.log_net_recv:
                log.info(f"NET::RECV | {conn.sid} | {rmsg}")
            if transport.on_recv:
                with contextlib.suppress(Exception):
                    # we don't pass whole conn to avoid control leaks
                    await transport.on_recv(conn.sid, rmsg)
            msg_res = await self._parse_rmsg(rmsg, conn)
            if isinstance(msg_res, Err):
                await msg_res.atrack()
                continue
            await self._accept_net_msg(msg_res.okval)

    async def _process_out_queue(
            self,
            transport: Transport,
            queue: Queue[tuple[Conn, dict]]):
        while True:
            conn, rmsg = await queue.get()

            if self._cfg.log_net_send:
                log.info(f"NET::SEND | {conn.sid} | {rmsg}")

            if transport.on_send:
                with contextlib.suppress(Exception):
                    await transport.on_send(conn.sid, rmsg)

            log.info(f"send to connsid {conn.sid}: {rmsg}", 2)

            await conn.send(rmsg)

    async def _accept_net_msg(self, msg: Msg):
        if isinstance(msg.body, SrpcRecv):
            log.err(
                f"server bus won't accept RpcRecv messages, got {msg}"
                " => skip")
            return
        elif isinstance(msg.body, SrpcSend):
            # process rpc in a separate task to not block inp queue
            # processing
            task = asyncio.create_task(self._call_rpc(msg))
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)
            return
        # publish to inner bus with no duplicate net resending
        pub_res = await self.pub(msg, PubOpts(send_to_net=False))
        if isinstance(pub_res, Err):
            await (
                await self.pub(
                    pub_res,
                    PubOpts(lsid=msg.lsid))).atrack()

    async def _call_rpc(self, msg: Msg):
        body = msg.body
        code, _ = body.key.split(":")
        if code not in self._rpccode_to_fn:
            log.err(f"no such rpc code {code} for req {body} => skip")
            return
        fn, args_type = self._rpccode_to_fn[code]

        _rxcat_ctx.set(self._gen_ctx_dict_for_msg(msg))

        ctx_manager: CtxManager | None = None
        if self._cfg.rpc_ctxfn is not None:
            try:
                ctx_manager = (await self._cfg.rpc_ctxfn(body)).eject()
            except Exception as err:
                await log.atrack(
                    err,
                    f"rpx ctx manager retrieval for body {body} => skip")
                return
        try:
            if ctx_manager:
                async with ctx_manager:
                    res = await fn(args_type.model_validate(body.args))
            else:
                res = await fn(args_type.model_validate(body.args))
        except Exception as err:
            await log.atrack(
                err, f"rpcfn on req {body} => wrap to usual RpcRecv")
            res = Err(err)

        val: Any
        if isinstance(res, Ok):
            val = res.okval
        elif isinstance(res, Err):
            val = (await create_err_dto(res.errval)).eject()
            val = typing.cast(ErrDto, val).model_dump(exclude={"stacktrace"})
        else:
            log.err(
                f"rpcfn on req {body} returned non-res val {res} => skip")
            return

        # val must be any serializable by pydantic object, so here we pass it
        # directly to Msg, which will do serialization automatically under the
        # hood
        evt = Msg(
            lsid=msg.sid,
            skip__target_connsids=[msg.skip__connsid],
            skip__bodycode=SrpcRecv.code(),
            body=SrpcRecv(
                key=body.key,
                val=val))
        # we publish directly to the net since inner participants can't
        # subscribe to this
        await self._pub_msg_to_net(evt)

    async def _parse_rmsg(
            self, rmsg: dict, conn: Conn) -> Res[Msg]:
        msid: str | None = rmsg.get("sid", None)
        if not msid:
            return valerr("msg without sid")
        # msgs coming from net receive connection sid
        rmsg["skip__connsid"] = conn.sid
        msg_res = await Msg.deserialize_from_net(rmsg)
        return msg_res

    def _init_transports(self):
        self._conn_type_to_atransport: dict[type[Conn], ActiveTransport] = {}
        transports = self._cfg.transports
        if not self._cfg.transports:
            transports = [self.DEFAULT_TRANSPORT]
        for transport in typing.cast(list[Transport], transports):
            if transport.conn_type in self._conn_type_to_atransport:
                log.err(
                    f"conn type {transport.conn_type} is already regd"
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

    async def _set_welcome(self) -> Res[None]:
        codes_res = await Code.get_regd_codes()
        if isinstance(codes_res, Err):
            return codes_res
        codes = codes_res.okval
        welcome = Welcome(codes=codes)
        self._preserialized_welcome_msg = (await Msg(
            skip__bodycode=Welcome.code(),
            body=welcome).serialize_to_net()).eject()
        rewelcome_res = await self._rewelcome_all_conns()
        if isinstance(rewelcome_res, Err):
            return rewelcome_res
        return Ok(None)

    async def _rewelcome_all_conns(self) -> Res[None]:
        return Ok(await self._pub_rmsg_to_net(
            self._preserialized_welcome_msg,
            self._sid_to_conn.keys()))
