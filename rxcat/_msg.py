from inspect import isfunction
from typing import Any, Self, TypeVar

from pydantic import BaseModel
from pykit.err import ValErr
from pykit.fcode import code
from pykit.log import log
from pykit.obj import get_fully_qualified_name
from pykit.rand import RandomUtils
from pykit.res import Res
from result import Err, Ok

from rxcat._err import ErrDto

TMdata = TypeVar("TMdata")
Mdata = Any
"""
Any custom data bus user interested in. Must be serializable and implement
`code() -> str` method.

To represent basic types without the need of modifying them with code
signature, use CodedMsgData.
"""
class CodedMsgData(BaseModel):
    """
    Msg data coupled with identification code.
    """
    code: str
    data: Mdata

def get_mdata_code(data: Mdata) -> Res[str]:
    if isinstance(data, CodedMsgData):
        code = data.code
    else:
        codefn = getattr(data, "code", None)
        if codefn is None:
            return Err(ValErr(
                f"msg data {data} must define \"code() -> str\" method"))
        if not isfunction(codefn):
            return Err(ValErr(
                f"msg data {data} \"code\" attribute must be function,"
                f" got {codefn}"))
        try:
            code = codefn()
        except Exception as err:
            log.catch(err)
            return Err(ValErr(
                f"err {get_fully_qualified_name(err)} occured during"
                f" msg data {data} {codefn} method call #~stacktrace"))

    if not isinstance(code, str):
        return Err(ValErr(f"msg data {data} code {code} must be str"))
    if code == "":
        return Err(ValErr(f"msg data {data} has empty code"))
    for i, c in enumerate(code):
        if i == 0 and not c.isalpha():
            return Err(ValErr(
                f"msg data {data} code {code} must start with alpha"))
        if not c.isalnum() and c != "_":
            return Err(ValErr(
                f"msg data {data} code {code} can contain only alnum"
                " characters or underscore"))
    if len(code) > 256:
        return Err(ValErr(f"msg data {data} code {code} exceeds maxlen 256"))

    return Ok(code)

class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    Fields prefixed with "skip__" won't pass net serialization process.

    Msgs are internal to rxcat implementation. The bus user is only interested
    in the actual data he is operating on, and which connections they are
    operating with. And the Msg is just an underlying container for that.
    """
    msid: str = ""
    lsid: str | None = None
    """
    Linked message's sid.

    Used to send this message back to the owner of the message with this lsid.
    """

    skip__connsid: str | None = None
    """
    From which conn the msg is originated.

    Only actual for the server. If set to None, it means that the msg is inner.
    Otherwise it is always set to connsid.
    """

    skip__target_connsids: list[str] = []
    """
    To which connsids the published msg should be addressed.
    """

    data: Mdata

    skip__err: Exception | None = None
    """
    Err that only exists on the inner bus and won't be serialized.

    Only filled for error-carrying messages.
    """

    internal__is_thrown_by_lsubfn: bool | None = None
    """
    Errs can be thrown by req-listening action or by req+evt listening
    pubfn.

    Only filled for error-carrying messages.

    In the second case, we should set this flag to True to avoid infinite
    msg loop, where after pubfn fail, the err evt is generated with the
    same req sid, and again is sent to the same pubfn which caused this
    err.

    If this flag is set, the bus will prevent pubfn trigger, for this err
    evt, but won't disable the pubfn.
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

    @property
    def connsid(self) -> str | None:
        return self.skip__connsid

    @property
    def target_connsids(self) -> list[str]:
        return self.skip__target_connsids.copy()

    # todo: use orwynn indication funcs for serialize/deserialize methods

    def serialize_for_net(self, mcodeid: int) -> dict:
        res: dict = self.model_dump()

        # DEL SERVER MSG FIELDS
        #       we should do these before key deletion setup
        if "skip__connsid" in res and res["skip__connsid"] is not None:
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
                    or k.startswith(("internal__", "skip__"))):
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

        if "lsid" not in data:
            data["lsid"] = None

        return cls(**data)

TMsg = TypeVar("TMsg", bound=Msg)

class ok:
    """
    Confirm that a req processed successfully.

    This evt should have a lsid defined, otherwise it is pointless since
    it is too general.
    """

    def code(self) -> str:
        return "rxcat__ok"

class Welcome(BaseModel):
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

    def code(self) -> str:
        return "rxcat__welcome"

class Register(BaseModel):
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

    def code(self) -> str:
        return "rxcat__register"
