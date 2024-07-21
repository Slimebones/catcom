from inspect import isfunction
from typing import Any, Iterable, Self, TypeVar

from pydantic import BaseModel
from pykit.code import Code
from pykit.err import NotFoundErr, ValErr
from pykit.log import log
from pykit.res import Res
from pykit.res import Err, Ok
from pykit.uuid import uuid4

TMdata = TypeVar("TMdata")
Mdata = Any
"""
Any custom data bus user interested in. Must be serializable and implement
`code() -> str` method.

To represent basic types without the need of modifying them with code
signature, use CodedMsgData.
"""

class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    Fields prefixed with "skip__" won't pass net serialization process.

    Msgs are internal to rxcat implementation. The bus user is only interested
    in the actual data he is operating on, and which connections they are
    operating with. And the Msg is just an underlying container for that.
    """
    sid: str = ""
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

    # since we won't change data type for an existing message, we keep
    # code with the data
    skip__datacode: str
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
            data["msid"] = uuid4()
        super().__init__(**data)

    def __hash__(self) -> int:
        assert self.sid
        return hash(self.sid)

    @property
    def connsid(self) -> str | None:
        return self.skip__connsid

    @property
    def target_connsids(self) -> list[str]:
        return self.skip__target_connsids.copy()

    # todo: use orwynn indication funcs for serialize/deserialize methods

    async def serialize_to_net(self) -> Res[dict]:
        final = self.model_dump()

        codeid_res = await Code.get_registered_codeid(self.skip__datacode)
        if isinstance(codeid_res, Err):
            return codeid_res
        final["codeid"] = codeid_res.okval

        # del server msg fields - we should do these before key deletion setup
        if "skip__connsid" in final and final["skip__connsid"] is not None:
            # connsids must exist only inside server bus, it's probably an err
            # if a msg is tried to be serialized with connsid, but we will
            # throw a warning for now, and ofcourse del the field
            log.warn(
                "connsids must exist only inside server bus, but it is tried"
                f" to serialize msg {self} with connsid != None => ignore"
            )

        is_msid_found = False
        keys_to_del: list[str] = []
        for k, v in final.items():
            if k == "sid":
                is_msid_found = True
                continue
            # all internal or skipped keys are deleted from the final
            # serialization
            if (
                    v is None
                    or k.startswith(("internal__", "skip__"))):
                keys_to_del.append(k)

        if not is_msid_found:
            raise ValueError(f"no msid field for raw msg {final}")
        for k in keys_to_del:
            del final[k]

        return Ok(final)

    @classmethod
    async def deserialize_from_net(cls, data: dict) -> Res[Self]:
        """Recovers model of this class using dictionary."""
        # parse mdata separately according to it's registered type
        custom = data.get("data", None)
        if "codeid" not in data:
            return Err(ValErr(f"data {data} must have \"codeid\" field"))
        codeid = data["codeid"]
        del data["codeid"]
        if not isinstance(codeid, int):
            return Err(ValErr(
                f"invalid type of codeid {codeid}, expected int"))
        code_res = await Code.get_registered_code_by_id(codeid)
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval
        if not Code.has_code(code):
            return Err(ValErr(f"unregistered code {code}"))
        data["skip__code"] = code
        custom_type = Code._code_to_type[code]
        deserialize_custom = getattr(custom_type, "deserialize", None)
        if issubclass(custom_type, BaseModel):
            if not isinstance(custom, dict):
                return Err(ValErr(
                    "if custom type is BaseModel, data must be dict,"
                    f" got {data}"))
            custom = custom_type(**custom)
        elif deserialize_custom is not None:
            custom = deserialize_custom(custom)
        else:
            custom = custom_type(custom)

        if "lsid" not in data:
            data["lsid"] = None

        data = data.copy()
        # don't do redundant serialization of Any type
        if "data" in data:
            del data["data"]
        model = cls.model_validate(data.copy())
        model.data = custom
        return Ok(model)

TMsg = TypeVar("TMsg", bound=Msg)

# lowercase to not conflict with result.Ok
class ok:
    @staticmethod
    def code() -> str:
        return "rxcat__ok"

class Welcome(BaseModel):
    """
    Welcome evt sent to every connected client.
    """
    codes: list[str]

    @staticmethod
    def code() -> str:
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

    @staticmethod
    def code() -> str:
        return "rxcat__register"
