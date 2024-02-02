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
from typing import TYPE_CHECKING, ClassVar, Self, TypeVar

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

TEvt = TypeVar("TEvt", bound=Evt)
TReq = TypeVar("TReq", bound=Req)
Raction = Callable[[TReq, TEvt], Awaitable[None]]

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
    Errs can be thrown by req-listening action or by req+evt listening raction.

    In the second case, we should set this flag to True to avoid infinite
    msg loop, where after raction fail, the err evt is generated with the
    same req sid, and again is sent to the same raction which caused this err.

    If this flag is set, the bus will prevent raction trigger, for this err
    evt, but won't disable the raction.
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


class SubOpts(BaseModel):
    must_receive_last_msg: bool = True


class BusConnType:
    Inner = 0
    Tcp = 1
    Udp = 2
    Ws = 3


class Bus(Singleton):
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

        # todo: check id overflow
        self._next_available_conn_id: int = 0
        self._next_available_sub_id: int = 0

        self._id_to_conn: dict[int, Websocket] = {}
        self._conn_id_to_inp_out_tasks: \
            dict[int, tuple[Task, Task]] = {}

        self._sub_id_to_msg_type: dict[int, type[Msg]] = {}
        self._msg_type_to_actions: \
            dict[type[Msg], list[Callable[[Msg], Awaitable]]] = {}
        self._type_to_last_msg: dict[type[Msg], Msg] = {}

        # network in and out unprocessed yet raw msgs
        self._net_inp_wsmsg_queue: Queue = Queue(self.MsgQueueMaxSize)
        self._net_out_raw_msg_queue: Queue = Queue(self.MsgQueueMaxSize)

        self._net_inp_raw_msg_queue_processor: Task | None = None
        self._net_out_raw_msg_queue_processor: Task | None = None

        self._preserialized_initd_client_evt: dict = {}
        self._initd_client_evt_mcodeid: int | None = None

        if not self._net_inp_raw_msg_queue_processor:
            self._net_inp_raw_msg_queue_processor = asyncio.create_task(
                self._process_net_inp_raw_msg_queue()
            )
        if not self._net_out_raw_msg_queue_processor:
            self._net_out_raw_msg_queue_processor = asyncio.create_task(
                self._process_net_out_raw_msg_queue()
            )

        self._rsid_to_req_and_raction: dict[
            str, tuple[Req, Raction]
        ] = {}

        self._is_initd = True

    @property
    def is_initd(self) -> bool:
        return self._is_initd

    async def throw_err_evt(
        self,
        err: Exception,
        triggered_msg: Msg | None = None,
        pub_opts: PubOpts = PubOpts(),
        *,
        is_thrown_by_raction: bool | None = None
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
            isThrownByRaction=is_thrown_by_raction
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

        if bus._net_inp_raw_msg_queue_processor:  # noqa: SLF001
            bus._net_inp_raw_msg_queue_processor.cancel()  # noqa: SLF001
        if bus._net_out_raw_msg_queue_processor:  # noqa: SLF001
            bus._net_out_raw_msg_queue_processor.cancel()  # noqa: SLF001

        Bus.try_discard()

    async def conn(self, ws: Websocket) -> None:
        if not self._id_to_conn:
            await self.postinit()

        conn_id = self._next_available_conn_id
        self._id_to_conn[conn_id] = ws
        self._next_available_conn_id += 1

        try:
            # for now set initd out of order, just by putting preserialized
            # obj directly
            await ws.send_json(self._preserialized_initd_client_evt)
            await self._read_ws(ws)
        finally:
            assert conn_id in self._id_to_conn
            del self._id_to_conn[conn_id]

    async def _read_ws(self, ws: Websocket):
        async for wsmsg in ws:
            self._net_inp_wsmsg_queue.put_nowait(wsmsg)

    async def _process_net_inp_raw_msg_queue(self) -> None:
        while True:
            wsmsg: Wsmsg = await self._net_inp_wsmsg_queue.get()
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
            msg = t.deserialize_json(rawmsg)
            # publish to inner bus with no duplicate net resending
            await self.pub(msg, None, PubOpts(must_send_to_net=False))

    async def _process_net_out_raw_msg_queue(self) -> None:
        while True:
            raw_msg: dict = await self._net_out_raw_msg_queue.get()

            for conn in self._id_to_conn.values():
                await conn.send_json(raw_msg)

    async def sub(
        self,
        msg_type: type[TMsg],
        action: Callable[[TMsg], Awaitable],
        opts: SubOpts = SubOpts(),
    ) -> int:
        sub_id: int = self._next_available_sub_id
        self._next_available_sub_id += 1

        if msg_type not in self._msg_type_to_actions:
            self._msg_type_to_actions[msg_type] = []
        self._msg_type_to_actions[msg_type].append(action)  # type: ignore

        if opts.must_receive_last_msg and msg_type in self._type_to_last_msg:
            await self._try_invoke_action(
                action,  # type: ignore
                self._type_to_last_msg[msg_type]
            )

        return sub_id

    async def unsub(self, id: int):
        if id not in self._sub_id_to_msg_type:
            raise ValueError(f"sub with id {id} not found")

        assert self._sub_id_to_msg_type[id] in self._msg_type_to_actions

        msg_type = self._sub_id_to_msg_type[id]

        del self._sub_id_to_msg_type[id]
        # todo: also hold subid to action to not delete the whole action set
        log.warn(
            "unsub works uncorrectly now - deletes all actions for type"
        )
        del self._msg_type_to_actions[msg_type]

    async def unsub_many(
        self,
        id: list[int],
    ) -> None:
        for i in id:
            await self.unsub(i)

    async def pub(
        self,
        msg: Msg,
        raction: Raction | None = None,
        opts: PubOpts = PubOpts(),
    ):
        if raction is not None and not isinstance(msg, Req):
            raise InpErr(f"for defined raction, {msg} should be req")

        if (
            isinstance(msg, Req)
            and raction is not None
        ):
            if msg.msid in self._rsid_to_req_and_raction:
                raise AlreadyProcessedErr(f"{msg} for pubr")
            self._rsid_to_req_and_raction[msg.msid] = (msg, raction)

        msg_type = type(msg)
        self._type_to_last_msg[msg_type] = msg

        # SEND ORDER
        #
        #   1. Net
        #   2. Inner
        #   3. As response

        # todo:
        #    mcodeid is attached afterwards since not all messages contain it.
        #
        #    Is a msg has a mcodeid, it will be send over all conns despite the
        #    need.
        #
        #    The target servers later might specify which msg types to send
        #    from the client, but for now we don't care about upload
        #    bandwidths.
        #
        #    By impling handshake, the problem should be solved, since we
        #    would be able to translate only net-required codes.
        if opts.must_send_to_net:
            mcodeid: int | None = self.try_get_mcodeid_for_mtype(msg_type)
            if mcodeid is not None:
                self._net_out_raw_msg_queue.put_nowait(
                    msg.serialize_json(mcodeid)
                )

        if msg_type in self._msg_type_to_actions:
            for action in self._msg_type_to_actions[msg_type]:
                await self._try_invoke_action(action, msg)

        if isinstance(msg, Evt):
            await self._send_evt_as_response(msg)

    async def _send_evt_as_response(self, evt: Evt):
        req_and_raction = self._rsid_to_req_and_raction.get(
            evt.rsid,
            None
        )

        if isinstance(evt, ErrEvt) and evt.isThrownByRaction:
            # skip raction errs to avoid infinite msg loop
            return

        if req_and_raction is not None:
            req = req_and_raction[0]
            raction = req_and_raction[1]
            await self._try_invoke_raction(raction, req, evt)

            # raction for now never deld by the bus, despite the result
            # (errs will be continiously thrown) - the raction should
            # use "try_close_raction" by themself

    async def try_close_raction(self, rsid: str) -> bool:
        if rsid not in self._rsid_to_req_and_raction:
            return False
        del self._rsid_to_req_and_raction[rsid]
        return True

    def __action_catch(self, err: Exception):
        log.catch(err)

    async def _try_invoke_raction(
        self,
        raction: Raction,
        req: Req,
        evt: Evt
    ) -> bool:
        try:
            await raction(req, evt)
        except Exception as err:
            if self._cfg.is_invoked_action_unhandled_errs_logged:
                self.__action_catch(err)
            # technically, the msg which caused the err is evt, since on evt
            # the raction is finally called
            await self.throw_err_evt(err, evt, is_thrown_by_raction=True)
            return False
        return True

    async def _try_invoke_action(
        self,
        action: Callable[[Msg], Awaitable[None]],
        msg: Msg
    ) -> bool:
        try:
            await action(msg)
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

