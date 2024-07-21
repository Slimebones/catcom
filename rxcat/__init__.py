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
from inspect import isclass, signature
from typing import (
    Any,
    ClassVar,
    Coroutine,
    Iterable,
    Literal,
    Protocol,
    runtime_checkable,
)
from pykit.uuid import uuid4

from pydantic import BaseModel
from pykit.err import AlreadyProcessedErr, InpErr, NotFoundErr, ValErr
from pykit.err_utils import create_err_dto
from pykit.log import log
from pykit.pointer import Pointer
from pykit.res import Err, Ok, UnwrapErr, Res, valerr
from pykit.err import ErrDto
from pykit.singleton import Singleton

from rxcat._msg import (
    Mdata,
    Msg,
    Register,
    TMsg,
    Welcome,
)
from rxcat._rpc import EmptyRpcArgs, RpcFn, SrpcSend, SrpcRecv, TRpcFn
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
from rxcat._msg import ok
from pykit.code import Code, Coded, get_fqname

__all__ = [
    "ServerBus",
    "SubFn",
    "ok",

    "ResourceServerErr",
    "RegisterFn",

    "Mdata",

    "RpcFn",
    "srpc",
    "SrpcSend",
    "SrpcRecv",
    "EmptyRpcArgs",

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
def srpc():
    def inner(target: TRpcFn) -> TRpcFn:
        ServerBus.register_rpc(target).eject()
        return target
    return inner

class ResourceServerErr(Exception):
    def code(self) -> str:
        return "rxcat__resource_server_err"

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

SubFn = Callable[
    [Mdata],
    Awaitable[Res[Mdata | list[Mdata] | None] | None]]

class PubOpts(BaseModel):
    subfn: SubFn | None = None

    target_connsids: Iterable[str] | None = None
    """
    Connection sids to publish to.

    Defaults to ctx connsid, if exists.
    """

    use_ctx_msid_as_lsid: bool = False

    send_to_inner: bool = True
    """
    Whether to send to inner bus subscribers.
    """

    send_to_net: bool = True
    """
    Will send to net if True and code is defined for the msg passed.
    """

    log_errs: bool = True
    """
    Whether to log information about published errs.
    """

    pubr__timeout: float | None = None
    """
    Timeout of awaiting for published message response arrival. Defaults to
    None, which means no timeout is set.
    """

DataFilter = Callable[[Mdata], Awaitable[bool]]

class SubOpts(BaseModel):
    recv_last_msg: bool = True
    """
    Whether to receive last stored msg with the same data code.
    """
    filters: list[DataFilter] | None = None
    """
    All filters should succeed before data being passed to a subscriber.
    """

_rxcat_ctx = ContextVar("rxcat", default={})

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

    sub_ctxfn: Callable[[Msg], Awaitable[Res[CtxManager]]] | None = None
    rpc_ctxfn: Callable[[SrpcSend], Awaitable[Res[CtxManager]]] | None = None

    trace_errs_on_pub: bool = True

    class Config:
        arbitrary_types_allowed = True

class ServerBus(Singleton):
    """
    Rxcat server bus implementation.
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
    DEFAULT_CODE_ORDER: list[str] = [
        "rxcat__register_req",
        "rxcat__welcome"
    ]

    def __init__(self):
        self._is_initd = False

    def get_ctx(self) -> dict:
        return _rxcat_ctx.get().copy()

    async def init(self, cfg: ServerBusCfg = ServerBusCfg()):
        (await self.register(
            Register,
            Welcome,
            ok,
            ErrDto,
            SrpcSend,
            SrpcRecv)).eject()

        self._cfg = cfg

        self._init_transports()

        self._sid_to_conn: dict[str, Conn] = {}

        self._subsid_to_code: dict[str, str] = {}
        self._subsid_to_subfn: dict[str, SubFn] = {}
        self._code_to_subfns: dict[str, list[SubFn]] = {}
        self._code_to_last_mdata: dict[str, Mdata] = {}

        self._preserialized_welcome_msg: dict = {}

        self._lsid_to_subfn: dict[str, SubFn] = {}
        """
        Subscribers awaiting arrival of linked message.
        """
        self._lsids_to_del_on_next_pubfn: set[str] = set()

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

    async def _set_welcome(self) -> Res[Welcome]:
        codes_res = await Code.get_registered_codes()
        if isinstance(codes_res, Err):
            return codes_res
        welcome = Welcome(codes=codes_res.okval)
        self._preserialized_welcome_msg = (await Msg(
            skip__datacode=Welcome.code(),
            data=welcome).serialize_to_net()).eject()
        rewelcome_res = await self._rewelcome_all_conns()
        if isinstance(rewelcome_res, Err):
            return rewelcome_res
        return Ok(welcome)

    async def _rewelcome_all_conns(self) -> Res[None]:
        return await self.pub(
            self._preserialized_welcome_msg,
            PubOpts(target_connsids=self._sid_to_conn.keys()))

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
    async def get_registered_type(cls, code: str) -> Res[type]:
        return await Code.get_registered_type(code)

    async def register(self, *types: type) -> Res[None]:
        """
        Register codes for types.

        No err is raised on existing code redefinition. Err is printed on
        invalid codes.

        Be careful with this method, once called it enables a lock on msg
        serialization and other processes for the time of codes modification.
        Also, after the registering, all the clients gets notified about
        the changed codes with the repeated welcome message.

        So it's better to be called once and at the start of the program.
        """
        updr = await Code.upd(types, self.DEFAULT_CODE_ORDER)
        if isinstance(updr, Err):
            return updr
        return await self._rewelcome_all_conns()

    @classmethod
    def register_rpc(cls, fn: RpcFn) -> Res[None]:
        """
        Registers server rpc (srpc).
        """
        rpc_code = fn.__name__ # type: ignore

        if rpc_code in cls._rpccode_to_fn:
            return Err(ValErr(f"rpc code {rpc_code} is already registered"))
        if not rpc_code.startswith("srpc__"):
            return Err(ValErr(f"code {rpc_code} must start with \"srpc__\""))

        sig = signature(fn)
        sig_param = sig.parameters.get("args")
        if not sig_param:
            return Err(ValErr(
                f"rpc fn {fn} with code {rpc_code} must accept"
                " \"args: AnyBaseModel\" as it's sole argument"))
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
                (await self._read_first_msg(conn, atransport)).eject()
            await conn.send(self._preserialized_welcome_msg)
            await self._read_ws(conn, atransport)
        finally:
            if not conn.is_closed:
                try:
                    await conn.close()
                except Exception as err:
                    log.err(
                        f"err {get_fqname(err)} is raised"
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
        if not isinstance(msg, Register):
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
            msg_res = await self.parse_rmsg(rmsg, conn)
            if isinstance(msg_res, Err):
                log.err(f"err during rmsg {rmsg} parsing #stacktrace")
                log.catch(msg_res.errval)
                continue
            await self._accept_net_msg(msg_res.okval)

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

    async def _accept_net_msg(self, msg: Msg):
        if isinstance(msg.data, SrpcRecv):
            log.err(
                f"server bus won't accept RpcSend messages, got {msg}"
                " => skip")
            return
        elif isinstance(msg.data, SrpcSend):
            # process rpc in a separate task to not block inp queue
            # processing
            task = asyncio.create_task(self._call_rpc(msg))
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)
            return
        # publish to inner bus with no duplicate net resending
        await self.pub(msg, PubOpts(send_to_net=False))

    async def _call_rpc(self, msg: Msg):
        data = msg.data
        code, _ = data.key.split(":")
        if code not in self._rpccode_to_fn:
            log.err(f"no such rpc code {code} for req {data} => skip")
            return
        fn, args_type = self._rpccode_to_fn[code]

        _rxcat_ctx.set(self._get_ctx_dict_for_msg(data))

        ctx_manager: CtxManager | None = None
        if self._cfg.rpc_ctxfn is not None:
            try:
                ctx_manager = (await self._cfg.rpc_ctxfn(data)).eject()
            except Exception as err:
                log.err(
                    f"err {get_fqname(err)} is occured"
                    f" during rpx ctx manager retrieval for data {data}"
                    " => skip; #stacktrace")
                log.catch(err)
                return
        try:
            if ctx_manager:
                async with ctx_manager:
                    res = await fn(args_type.model_validate(data.args))
            else:
                res = await fn(args_type.model_validate(data.args))
        except Exception as err:
            log.warn(
                f"unhandled exception occured for rpcfn on req {data}"
                " => wrap it to usual RpcEvt;"
                f" exception {get_fqname(err)}")
            log.catch(err)
            res = Err(err)

        val: Any
        if isinstance(res, Ok):
            val = res.okval
        elif isinstance(res, Err):
            val = create_err_dto(res.errval).eject()
        else:
            log.err(
                f"rpcfn on req {data} returned non-res val {res} => skip")
            return

        # val must be any serializable by pydantic object, so here we pass it
        # directly to Msg, which will do serialization automatically under the
        # hood
        evt = Msg(
            lsid=msg.sid,
            skip__target_connsids=[msg.connsid],
            key=data.key,
            val=val)
        await self._pub_to_net(evt)

    async def parse_rmsg(
            self, rmsg: dict, conn: Conn) -> Res[Msg]:
        msid: str | None = rmsg.get("sid", None)
        if not msid:
            return valerr("msg without sid")
        # msgs coming from net receive connection sid
        rmsg["skip__connsid"] = conn.sid
        msg_res = await Msg.deserialize_from_net(rmsg)
        return msg_res

    async def sub(
        self,
        datatype: type[Mdata] | str,
        subfn: SubFn,
        opts: SubOpts = SubOpts(),
    ) -> Res[Callable]:
        """
        Subscribes to certain message.

        Once the message is occured within the bus, the provided action is
        called.

        Args:
            datatype:
                Data implementing ``code() -> str`` method or direct code
                to subscribe to.
            fn:
                Function to fire once the messsage has arrived.
            opts (optional):
                Subscription options.
        Returns:
            Unsubscribe function.
        """
        r = self._check_norpc_mdata(datatype, "subscription")
        if isinstance(r, Err):
            return r
        subsid = uuid4()
        if opts.filters:
            subfn = self._apply_filters_to_subfn(subfn, opts.filters)

        code: str
        if isinstance(datatype, str):
            code = datatype
            validate_res = Code.validate(code)
            if isinstance(validate_res, Err):
                return validate_res
        else:
            code_res = Code.get_from_type(datatype)
            if isinstance(code_res, Err):
                return code_res
            code = code_res.okval
        if code not in self._code_to_subfns:
            self._code_to_subfns[code] = []
        self._code_to_subfns[code].append(subfn)
        self._subsid_to_subfn[subsid] = subfn
        self._subsid_to_code[subsid] = code

        if opts.recv_last_msg and code in self._code_to_last_mdata:
            last_data = self._code_to_last_mdata[code]
            await self._call_subfn(subfn, last_data)

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
        data: Mdata,
        opts: PubOpts = PubOpts()
    ) -> Res[Mdata]:
        """
        Publishes a message and awaits for the response.

        If the response is Exception, it is wrapped to res::Err.
        """
        aevt = asyncio.Event()
        ptr: Pointer[Mdata] = Pointer(target=None)

        def wrapper(aevt: asyncio.Event, ptr: Pointer[Mdata]):
            async def fn(data: Mdata):
                aevt.set()
                ptr.target = data
            return fn

        if opts.subfn is not None:
            log.warn("don't pass PubOpts.subfn to pubr, it gets overwritten")
        opts.subfn = wrapper(aevt, ptr)
        pub_res = await self.pub(data, opts)
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

    async def pub(
            self,
            data: Mdata,
            opts: PubOpts = PubOpts()) -> Res[None]:
        """
        Publishes data to the bus.

        Received Exception instance will be parsed to ErrDto. If it is an
        UnwrapError, it's value will be retrieved, validated as an Exception,
        and then parsed to ErrDto.

        Received Exceptions are additionally logged if
        cfg.trace_errs_on_pub == True.
        """
        code_res = Code.get_from_type(type(data))
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval

        if isinstance(data, Exception):
            if isinstance(data, UnwrapErr):
                res = data.result
                assert isinstance(res, Err)
                if isinstance(res.errval, Exception):
                    data = res.errval
                else:
                    data = ResourceServerErr(
                        f"got res with err value {res.errval},"
                        " which is not an instance of Exception")
            err = typing.cast(Exception, data)
            if opts.log_errs:
                log.err(f"pub err {get_fqname(err)} #stacktrace")
                log.catch(err)
            err_dto_res = create_err_dto(data)
            if isinstance(err_dto_res, Err):
                return err_dto_res
            data = err_dto_res.okval

        lsid = None
        if opts.use_ctx_msid_as_lsid:
            # by default we publish as response to current message, so we
            # use the current's message sid as linked sid
            msid_res = self.get_ctx_key("msid")
            if isinstance(msid_res, Err):
                return msid_res
            lsid = msid_res.okval
            assert isinstance(lsid, str)

        target_connsids = None
        if opts.target_connsids:
            target_connsids = opts.target_connsids
        else:
            # try to get ctx connsid, otherwise left as none
            connsid_res = self.get_ctx_key("connsid")
            if isinstance(connsid_res, Ok):
                assert isinstance(connsid_res.okval, str)
                target_connsids = [connsid_res.okval]

        msg = Msg(
            lsid=lsid,
            skip__datacode=code,
            data=data,
            skip__target_connsids=target_connsids)

        r = self._check_norpc_mdata(msg, "publication")
        if isinstance(r, Err):
            return r

        if opts.subfn is not None:
            if msg.sid in self._lsid_to_subfn:
                return Err(AlreadyProcessedErr(f"{msg} for pubr"))
            self._lsid_to_subfn[msg.sid] = opts.subfn

        self._code_to_last_mdata[code] = data

        # SEND ORDER
        #
        #   1. Net (only if has mcodeid and this is in the required list)
        #   2. Inner (always for every registered subfn)
        #   3. As a response (only if this msg type has the associated paction)

        if opts.send_to_net:
            await self._pub_to_net(msg)

        if opts.send_to_inner and code in self._code_to_subfns:
            await self._send_to_inner_bus(msg)

        if msg.lsid:
            await self._send_as_linked(msg)

        return Ok(None)

    async def _send_to_inner_bus(self, msg: Msg):
        subfns = self._code_to_subfns[msg.skip__datacode]
        if not subfns:
            return
        removing_subfn_sids = []
        for subfn in subfns:
            await self._call_subfn(subfn, msg)
            if subfn.is_removing:
                removing_subfn_sids.append(subfn.sid)
        for removing_subfn_sid in removing_subfn_sids:
            (await self.unsub(removing_subfn_sid)).unwrap_or(None)

    async def _pub_to_net(self, msg: Msg):
        if msg.skip__target_connsids:
            rmsg = (await msg.serialize_to_net()).unwrap_or(None)
            if rmsg is None:
                return
            for connsid in msg.skip__target_connsids:
                if connsid not in self._sid_to_conn:
                    log.err(
                        f"no conn with id {connsid} for msg {msg}"
                        " => skip"
                    )
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

    def _get_ctx_dict_for_msg(self, msg: Msg) -> dict:
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
        _rxcat_ctx.set(self._get_ctx_dict_for_msg(msg))

        if self._cfg.sub_ctxfn is not None:
            try:
                ctx_manager = (await self._cfg.sub_ctxfn(msg)).eject()
            except Exception as err:
                log.err(
                    f"err {get_fqname(err)} is occured"
                    f" during rpx ctx manager retrieval for data {msg.data}"
                    " => skip; #stacktrace")
                log.catch(err)
                return
            async with ctx_manager:
                res = await subfn.fn(msg)
        else:
            res = await subfn.fn(msg)

        vals = []
        if isinstance(res, Ok):
            val = res.okval
            if isinstance(val, list):
                vals = val
            else:
                vals = [val]

        for val in vals:
            # TODO: replace ignore() with warn() as it gets available in
            #       pykit::res
            (await self.pub(val)).ignore()

    def _check_norpc_mdata(
            self, data: Mdata | type[Mdata], disp_ctx: str) -> Res[None]:
        """
        Since rpc msgs cannot participate in actions like "sub" and "pub",
        we have a separate fn to check this.
        """
        iscls = isclass(data)
        if (
            (
                iscls
                and (issubclass(data, SrpcSend) or issubclass(data, SrpcRecv)))
            or (
                not iscls
                and (isinstance(data, (SrpcSend, SrpcRecv))))):
            return Err(ValErr(
                f"mdata {data} in context of \"{disp_ctx}\" cannot be associated"
                " with rpc"))
        return Ok(None)

    def _apply_filters_to_subfn(
            self, subfn: SubFn, filters: Iterable[DataFilter]) -> SubFn:
        async def wrapper(data: Mdata) -> Any:
            for f in filters:
                f_out = await f(data)
                if not f_out:
                    return
            # all filters passed - call the actual subfn
            return await subfn(data)
        return wrapper
