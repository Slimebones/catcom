from typing import Self, TypeVar

from pydantic import BaseModel
from pykit.fcode import code
from pykit.log import log
from pykit.rand import RandomUtils


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
            if k.startswith("inner__"):
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
    errtype: str

    inner__err: Exception | None = None
    """
    Err that only exists on the inner bus and won't be serialized.
    """

    inner__is_thrown_by_pubaction: bool | None = None
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
