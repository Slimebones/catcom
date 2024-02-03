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
import typing
from asyncio import Task
from asyncio.queues import Queue
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, ClassVar, Coroutine, Self, TypeVar

from aiohttp.web import WebSocketResponse as Websocket
from fcode import FcodeCore, code
from pydantic import BaseModel
from pykit.err import AlreadyProcessedErr, InpErr
from pykit.log import log
from pykit.rnd import RandomUtils
from pykit.singleton import Singleton

if TYPE_CHECKING:
    from aiohttp.http import WSMessage as Wsmsg

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

class BusCfg(BaseModel):
    is_invoked_action_unhandled_errs_logged: bool = False

class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    @abs
    """

    MustExcludeChildNoneFieldsOnSerialization: ClassVar[bool] = True
    """
    If set to True, all child-defined fields set to None will be excluded
    from the final serialization to reduce raw msg size.
    """

    msid: str = ""

    connid: int | None = None
    """
    From which conn the msg is originated.

    Only actual for the server. If set to None, it means that the msg is inner.
    Otherwise it is always set to connid.
    """

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

        # we should do this check before key deletion setup
        if "connid" in res and res["connid"] is not None:
            # connids must exist only inside server bus, it's probably an err
            # if a msg is tried to be serialized with connid, but we will
            # throw a warning for now, and ofcourse del the field
            log.warn(
                "connids must exist only inside server bus, but it is tried"
                f" to serialize msg {self} with connid != None"
            )
            del res["connid"]

        is_msid_found = False
        keys_to_del: list[str] = []
        for k, v in res.items():
            if k == "msid":
                is_msid_found = True
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

        return cls(**data)

class Evt(Msg):
    """
    @abs
    """
    rsid: str
    """
    In response to which request the event has been sent.
    """

class Req(Msg):
    """
    @abs
    """

@code("pyrxcat.sub-req")
class _SubReq(Req):
    """
    Used to manage subscriptions over the net.
    """
    targetMcodeid: int

@code("pyrxcat.unsub-req")
class _UnsubReq(Req):
    targetMcodeid: int

TEvt = TypeVar("TEvt", bound=Evt)
TReq = TypeVar("TReq", bound=Req)
PubAction = Callable[[TReq, TEvt], Awaitable[None]]

@code("pyrxcat.ok-evt")
class OkEvt(Evt):
    """
    Confirm that a req processed successfully.

    This evt should have a rsid defined, otherwise it is pointless since
    it is too general.
    """

@code("pyrxcat.err-evt")
class ErrEvt(Evt):
    """
    Represents any err that can be thrown.

    Only Server-endpoints can throw errs to the bus.
    """
    errcodeid: int | None = None
    errmsg: str

    isThrownByRaction: bool | None = None
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


@code("pyrxcat.initd-client-evt")
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

class PubOpts(BaseModel):
    must_send_to_net: bool = True
    """
    Will send to net if True and code is defined for the msg passed.
    """

MsgFilter = Callable[[Msg], Awaitable[bool]]

class SubOpts(BaseModel):
    must_receive_last_msg: bool = True
    filters: list[MsgFilter] = []
    """
    All filters should succeed before msg being passed to a subscriber.
    """

class BusConnType:
    Inner = 0
    Tcp = 1
    Udp = 2
    Ws = 3

SubAction = tuple[Callable[[Msg], Awaitable], SubOpts]

class Bus(Singleton):
    """
    Server bus implementation.
    """
    MsgQueueMaxSize = 10000

    def __init__(self):
        self._is_initd = False

    async def init(self):
        self._cfg = BusCfg(is_invoked_action_unhandled_errs_logged=True)
        FcodeCore.defcode("rxcat.fallback-err", Exception)
        # only server is able to index mcodes, client is not able to send
        # theirs mcodes on conn, so the server must know client codes at boot
        #
        # active and legacy codes are bundled together under the same id, so
        # we use a dict here
        self._McodeToMcodeid: dict[str, int] = {}
        self._ErrcodeToErrcodeid: dict[str, int] = {}

        self._IndexedActiveMcodes: list[str] = []
        self._IndexedActiveErrcodes: list[str] = []

        self._IndexedMcodes: list[list[str]] = []
        self._IndexedErrcodes: list[list[str]] = []

        self._init_mcodes()
        self._init_errcodes()
        self._fallback_errcodeid: int = \
            self._ErrcodeToErrcodeid["rxcat.fallback-err"]

        self._mcodeid_to_connids: dict[int, set[int]] = {}
        """
        Map of connids for each required mcodeid.
        """

        # todo: check id overflow
        self._next_available_conn_id: int = 0
        self._next_available_sub_id: int = 0

        self._connid_to_conn: dict[int, Websocket] = {}
        self._conn_id_to_inp_out_tasks: \
            dict[int, tuple[Task, Task]] = {}

        self._subid_to_mtype: dict[int, type[Msg]] = {}
        self._subid_to_subaction: dict[int, SubAction] = {}
        self._mtype_to_subactions: \
            dict[type[Msg], list[SubAction]] = {}
        self._type_to_last_msg: dict[type[Msg], Msg] = {}

        # network in and out unprocessed yet raw msgs
        self._net_inp_connid_and_wsmsg_queue: Queue[tuple[int, Wsmsg]] = \
            Queue(self.MsgQueueMaxSize)
        self._net_out_connids_and_rawmsg_queue: Queue[
            tuple[list[int], dict]
        ] = Queue(self.MsgQueueMaxSize)

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

        self._rsid_to_req_and_pubaction: dict[
            str, tuple[Req, PubAction]
        ] = {}

        self._is_initd = True

        # accept sub/unsub from the client
        await self.sub(_SubReq, self._on_sub)
        await self.sub(_UnsubReq, self._on_unsub)

    async def _on_sub(self, req: _SubReq):
        if req.connid is None:
            log.err("sub reqs must originate only from conns => skip")
            return
        if req.targetMcodeid not in self._mcodeid_to_connids:
            self._mcodeid_to_connids[req.targetMcodeid] = set()
        connids = self._mcodeid_to_connids[req.targetMcodeid]
        if req.connid in connids:
            log.err(
                f"req {req} with connid {req.connid} has been attached"
                " already => skip"
            )
            return
        connids.add(req.connid)

    async def _on_unsub(self, req: _UnsubReq):
        if req.connid is None:
            log.err("unsub reqs must originate only from conns => skip")
            return
        if req.targetMcodeid not in self._mcodeid_to_connids:
            log.err(
                f"for unsub req {req} target mcodeid {req.targetMcodeid}"
                " is not found => skip"
            )
            return
        connids = self._mcodeid_to_connids[req.targetMcodeid]
        if req.connid not in connids:
            log.err(
                f"unsub req {req} connid not in connids for the given"
                f" mcodeid {req.targetMcodeid} => skip"
            )
            return
        connids.remove(req.connid)

    @property
    def is_initd(self) -> bool:
        return self._is_initd

    async def throw_err_evt(
        self,
        err: Exception,
        triggered_msg: Msg | None = None,
        pub_opts: PubOpts = PubOpts(),
        *,
        is_thrown_by_pubaction: bool | None = None
    ):
        """
        Pubs ThrownErrEvt.

        If given err has no code attached, the default "rxcat.fallback-err" is
        used.

        Args:
            err:
                Err to throw as evt.
            triggered_msg:
                Msg which caused an error. If the msg is Evt with rsid
                defined, it will be send back to the requestor.
            pub_opts:
                Extra pub opts to send to Bus.pub method.
        """
        errcodeid: int | None = self.try_get_errcodeid_for_errtype(type(err))
        errmsg: str = ", ".join(err.args)

        rsid: str = "__system__"
        if isinstance(triggered_msg, Evt):
            rsid = triggered_msg.rsid

        evt = ErrEvt(
            errcodeid=errcodeid,
            errmsg=errmsg,
            rsid=rsid,
            isThrownByRaction=is_thrown_by_pubaction
        )

        if errcodeid is None:
            errcodeid = self._fallback_errcodeid

        await self.pub(evt, None, pub_opts)

    def try_get_mcodeid_for_mcode(self, mcode: str) -> int | None:
        res = self._McodeToMcodeid.get(mcode, -1)
        if res == -1:
            return None
        return res

    def try_get_errcodeid_for_errcode(self, errcode: str) -> int | None:
        res = self._ErrcodeToErrcodeid.get(errcode, -1)
        if res == -1:
            return None
        return res

    def _init_mcodes(self):
        collections = FcodeCore.try_get_all_codes(Msg)
        assert collections, "must have at least one mcode defined"
        self._IndexedMcodes = collections

        for id, mcodes in enumerate(collections):
            self._IndexedActiveMcodes.append(mcodes[0])
            for mcode in mcodes:
                self._McodeToMcodeid[mcode] = id

    def _init_errcodes(self):
        collections = FcodeCore.try_get_all_codes(Exception)
        assert collections, "must have at least one errcode defined"
        self._IndexedErrcodes = collections

        for id, errcodes in enumerate(collections):
            self._IndexedActiveErrcodes.append(errcodes[0])
            for errcode in errcodes:
                self._ErrcodeToErrcodeid[errcode] = id

    async def postinit(self):
        # restrict any further code defines since we start sending code data
        # to clients
        FcodeCore.deflock = True

        if self._initd_client_evt_mcodeid is None:
            mcodeid = self.try_get_mcodeid_for_mtype(InitdClientEvt)
            assert mcodeid is not None
            self._initd_client_evt_mcodeid = mcodeid

        if not self._preserialized_initd_client_evt:
            self._preserialized_initd_client_evt = InitdClientEvt(
                indexedMcodes=self._IndexedMcodes,
                indexedErrcodes=self._IndexedErrcodes,
                rsid="__system__"
            ).serialize_json(self._initd_client_evt_mcodeid)

    @classmethod
    async def destroy(cls):
        """
        Should be used only on server close or test interchanging.
        """
        bus = Bus.ie()
        if not bus.is_initd:
            return

        if not bus._is_initd:  # noqa: SLF001
            return

        if bus._net_inp_queue_processor:  # noqa: SLF001
            bus._net_inp_queue_processor.cancel()  # noqa: SLF001
        if bus._net_out_queue_processor:  # noqa: SLF001
            bus._net_out_queue_processor.cancel()  # noqa: SLF001

        Bus.try_discard()

    async def conn(self, conn: Websocket) -> None:
        if not self._connid_to_conn:
            await self.postinit()

        connid = self._next_available_conn_id
        self._connid_to_conn[connid] = conn
        self._next_available_conn_id += 1
        log.info(
            f"accept new conn {conn}, assign id {connid}",
            2
        )

        try:
            # for now set initd out of order, just by putting preserialized
            # obj directly
            await conn.send_json(self._preserialized_initd_client_evt)
            await self._read_ws(connid, conn)
        finally:
            assert connid in self._connid_to_conn
            del self._connid_to_conn[connid]

    async def _read_ws(self, connid: int, conn: Websocket):
        async for wsmsg in conn:
            self._net_inp_connid_and_wsmsg_queue.put_nowait((connid, wsmsg))

    async def _process_net_inp_queue(self) -> None:
        while True:
            connid, wsmsg = await self._net_inp_connid_and_wsmsg_queue.get()
            assert connid >= 0

            try:
                rawmsg: dict = wsmsg.json()
            except Exception:
                log.err(f"unable to parse ws msg {wsmsg}")
                continue

            msid: str | None = rawmsg.get("msid", None)
            if not msid:
                log.err("msg without msid => skip silently")
                continue

            # for future rsid navigation
            # rsid: str | None = raw_msg.get("lmsid", None)

            mcodeid: int | None = rawmsg.get("mcodeid", None)
            if not mcodeid:
                await self.throw_err_evt(
                    ValueError(
                        f"got msg {rawmsg} with undefined mcodeid"
                    )
                )
                continue
            if mcodeid < 0:
                await self.throw_err_evt(
                    ValueError(
                        f"invalid mcodeid {mcodeid}"
                    )
                )
                continue
            if mcodeid > len(self._IndexedActiveMcodes) - 1:
                await self.throw_err_evt(ValueError(
                    f"unrecognized mcodeid {mcodeid}"
                ))
                continue

            t: type[Msg] | None = \
                FcodeCore.try_get_type_for_any_code(
                    self._IndexedActiveMcodes[mcodeid]
                )
            assert t is not None, "if mcodeid found, mtype must be found"

            t = typing.cast(type[Msg], t)
            rawmsg["connid"] = connid
            msg = t.deserialize_json(rawmsg)
            # publish to inner bus with no duplicate net resending
            await self.pub(msg, None, PubOpts(must_send_to_net=False))

    async def _process_net_out_queue(self) -> None:
        while True:
            connids, rawmsg = \
                await self._net_out_connids_and_rawmsg_queue.get()

            coros: list[Coroutine] = [
                self._connid_to_conn[connid].send_json(rawmsg)
                for connid in connids
            ]
            await asyncio.gather(*coros)

    async def sub(
        self,
        mtype: type[TMsg],
        action: Callable[[TMsg], Awaitable],
        opts: SubOpts = SubOpts(),
    ) -> int:
        subid: int = self._next_available_sub_id
        self._next_available_sub_id += 1
        subaction = (typing.cast(Callable, action), opts)

        if mtype not in self._mtype_to_subactions:
            self._mtype_to_subactions[mtype] = []
        self._mtype_to_subactions[mtype].append(subaction)
        self._subid_to_subaction[subid] = subaction
        self._subid_to_mtype[subid] = mtype

        if opts.must_receive_last_msg and mtype in self._type_to_last_msg:
            last_msg = self._type_to_last_msg[mtype]
            await self._try_invoke_subaction(
                subaction,
                last_msg
            )

        mcodeid = self.try_get_mcodeid_for_mtype(mtype)
        if mcodeid:
            await self.pub(_SubReq(targetMcodeid=mcodeid))

        return subid

    async def unsub(self, subid: int):
        if subid not in self._subid_to_mtype:
            raise ValueError(f"sub with id {subid} not found")

        assert self._subid_to_mtype[subid] in self._mtype_to_subactions

        msg_type = self._subid_to_mtype[subid]

        mcodeid = self.try_get_mcodeid_for_mtype(msg_type)
        if mcodeid:
            await self.pub(_UnsubReq(targetMcodeid=mcodeid))

        assert subid in self._subid_to_mtype, "all maps must be synced"
        assert subid in self._subid_to_subaction, "all maps must be synced"
        del self._subid_to_mtype[subid]
        del self._subid_to_subaction[subid]
        del self._mtype_to_subactions[msg_type]

    async def unsub_many(
        self,
        id: list[int],
    ) -> None:
        for i in id:
            await self.unsub(i)

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

        msg_type = type(msg)
        self._type_to_last_msg[msg_type] = msg

        # SEND ORDER
        #
        #   1. Net (only if has mcodeid and this is in the required list)
        #   2. Inner (always for every registered subaction)
        #   3. As a response (only if this msg type has the associated paction)

        if opts.must_send_to_net:
            mcodeid: int | None = self.try_get_mcodeid_for_mtype(msg_type)
            if mcodeid is not None:
                connids: list[int] = self._mcodeid_to_connids.get(mcodeid, [])
                if connids:
                    log.info(
                        f"send {msg} over the net to connids {connids}",
                        2
                    )
                    rawmsg = msg.serialize_json(mcodeid)
                    self._net_out_connids_and_rawmsg_queue.put_nowait(
                        (connids, rawmsg)
                    )

        if msg_type in self._mtype_to_subactions:
            for subaction in self._mtype_to_subactions[msg_type]:
                await self._try_invoke_subaction(subaction, msg)

        if isinstance(msg, Evt):
            await self._send_evt_as_response(msg)

    async def _send_evt_as_response(self, evt: Evt):
        req_and_pubaction = self._rsid_to_req_and_pubaction.get(
            evt.rsid,
            None
        )

        if isinstance(evt, ErrEvt) and evt.isThrownByRaction:
            # skip pubaction errs to avoid infinite msg loop
            return

        if req_and_pubaction is not None:
            req = req_and_pubaction[0]
            pubaction = req_and_pubaction[1]
            await self._try_invoke_pubaction(pubaction, req, evt)

            # pubaction for now never deld by the bus, despite the result
            # (errs will be continiously thrown) - the pubaction should
            # use "try_close_pubaction" by themself

    async def try_close_pubaction(self, rsid: str) -> bool:
        if rsid not in self._rsid_to_req_and_pubaction:
            return False
        del self._rsid_to_req_and_pubaction[rsid]
        return True

    def __action_catch(self, err: Exception):
        log.catch(err)

    async def _try_invoke_pubaction(
        self,
        pubaction: PubAction,
        req: Req,
        evt: Evt
    ) -> bool:
        try:
            await pubaction(req, evt)
        except Exception as err:
            if self._cfg.is_invoked_action_unhandled_errs_logged:
                self.__action_catch(err)
            # technically, the msg which caused the err is evt, since on evt
            # the pubaction is finally called
            await self.throw_err_evt(err, evt, is_thrown_by_pubaction=True)
            return False
        return True

    async def _try_invoke_subaction(
        self,
        subaction: SubAction,
        msg: Msg
    ) -> bool:
        filters = subaction[1].filters
        for filter in filters:
            this_filter_f = await filter(msg)
            if not this_filter_f:
                return False

        try:
            await subaction[0](msg)
        except Exception as err:
            if self._cfg.is_invoked_action_unhandled_errs_logged:
                self.__action_catch(err)
            await self.throw_err_evt(err, msg)
            return False

        return True

    def try_get_mcodeid_for_mtype(self, mtype: type[Msg]) -> int | None:
        mcode: str | None = \
            FcodeCore.try_get_active_code_for_type(mtype)
        if mcode:
            mcodeid: int = self._McodeToMcodeid.get(mcode, -1)
            assert mcodeid != -1, "must find mcodeid for mcode"
            return mcodeid
        return None

    def try_get_mcode_for_mcodeid(self, mcodeid: int) -> str | None:
        for k, v in self._McodeToMcodeid.items():
            if v == mcodeid:
                return k
        return None

    def try_get_errcodeid_for_errtype(
        self, errtype: type[Exception]
    ) -> int | None:
        errcode: str | None = \
            FcodeCore.try_get_active_code_for_type(errtype)
        if errcode:
            errcodeid: int = self._ErrcodeToErrcodeid.get(errcode, -1)
            assert errcodeid != -1, "must find mcodeid for mcode"
            return errcodeid
        return None

