from typing import Any, Self, TypeVar

from pydantic import BaseModel
from pykit.fcode import code
from pykit.log import log
from pykit.rand import RandomUtils

from rxcat._err import ErrDto


class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    Fields prefixed with "skipnet__" won't pass net serialization process.

    @abs
    """
    msid: str = ""

    skipnet__connsid: str | None = None
    """
    From which conn the msg is originated.

    Only actual for the server. If set to None, it means that the msg is inner.
    Otherwise it is always set to connsid.
    """

    skipnet__target_connsids: list[str] = []
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

    def serialize_for_net(self, mcodeid: int) -> dict:
        res: dict = self.model_dump()

        # DEL SERVER MSG FIELDS
        #       we should do these before key deletion setup
        if "skipnet__connsid" in res and res["skipnet__connsid"] is not None:
            # connsids must exist only inside server bus, it's probably an err
            # if a msg is tried to be serialized with connsid, but we will
            # throw a warning for now, and ofcourse del the field
            log.warn(
                "connsids must exist only inside server bus, but it is tried"
                f" to serialize msg {self} with connsid != None"
            )

        is_msid_found = False
        keys_to_del: list[str] = []
        for k, v in res.items():
            if k == "msid":
                is_msid_found = True
                continue
            # all inner keys are deleted from the final serialization
            if (
                    v is None
                    or k.startswith(("internal__", "skipnet__"))):
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
    def deserialize_from_net(cls, data: dict) -> Self:
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
        return [self.skipnet__connsid] \
            if self.skipnet__connsid is not None else []

class Evt(Msg):
    """
    @abs
    """
    rsid: str | None
    """
    In response to which request the event has been sent.
    """

    skipnet__is_continious: bool | None = None
    """
    Whether receiving bus should delete pubfn entry after call pubfn
    with this evt. If true, the entry is not deleted.
    """

    def as_res_from_req(self, req: Req) -> Self:
        self.rsid = req.msid
        self.skipnet__target_connsids = req.get_res_connsids()
        return self

TMsg = TypeVar("TMsg", bound=Msg)
TEvt = TypeVar("TEvt", bound=Evt)
TReq = TypeVar("TReq", bound=Req)

@code("rxcat__ok_evt")
class OkEvt(Evt):
    """
    Confirm that a req processed successfully.

    This evt should have a rsid defined, otherwise it is pointless since
    it is too general.
    """

@code("rxcat__err_evt")
class ErrEvt(Evt):
    """
    Represents any err that can be thrown.

    Only Server-endpoints can throw errs to the bus.
    """
    err: ErrDto

    skipnet__err: Exception | None = None
    """
    Err that only exists on the inner bus and won't be serialized.
    """

    internal__is_thrown_by_pubfn: bool | None = None
    """
    Errs can be thrown by req-listening action or by req+evt listening
    pubfn.

    In the second case, we should set this flag to True to avoid infinite
    msg loop, where after pubfn fail, the err evt is generated with the
    same req sid, and again is sent to the same pubfn which caused this
    err.

    If this flag is set, the bus will prevent pubfn trigger, for this err
    evt, but won't disable the pubfn.
    """

@code("rxcat__welcome_evt")
class WelcomeEvt(Evt):
    """
    Welcome evt sent to every connected client.

    Contains information required to survive in harsh rx environment.
    """

    indexed_mcodes: list[list[str]]
    """
    Collection of active and legacy mcodes, indexed by their mcodeid,
    first mcode under each index's list is an active one.
    """

    indexed_errcodes: list[list[str]]
    """
    Collection of active and legacy errcodes, indexed by their
    errcodeid, first errcode under each index's list is an active one.
    """

    # ... here an additional info how to behave properly on the bus can be sent

@code("rxcat__register_req")
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
