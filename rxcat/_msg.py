from inspect import isfunction
from typing import Any, Iterable, Self, TypeVar

from pydantic import BaseModel
from pykit.err import NotFoundErr, ValErr
from pykit.fcode import code
from pykit.log import log
from pykit.obj import get_fully_qualified_name
from pykit.rand import RandomUtils
from pykit.res import Res
from rxcat._lock import Lock
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

class Code:
    """
    Manages attached to various objects str codes.
    """
    _code_to_type: dict[str, type] = {}
    _codes: list[str] = []
    _lock: Lock = Lock()

    @classmethod
    async def get_codes(cls) -> Res[list[str]]:
        await cls._lock.wait()
        return Ok(cls._codes.copy())

    @classmethod
    async def get_code(cls, t: type) -> Res[str]:
        await cls._lock.wait()
        for c, t_ in cls._code_to_type.items():
            if t_ is t:
                return Ok(c)
        return Err(ValErr(f"code {code} is not registered"))

    @classmethod
    async def get_codeid(cls, code: str) -> Res[int]:
        await cls._lock.wait()
        if code not in cls._codes:
            return Err(ValErr(f"code {code} is not registered"))
        return Ok(cls._codes.index(code))

    @classmethod
    async def get_type(cls, code: str) -> Res[type]:
        await cls._lock.wait()
        if code not in cls._code_to_type:
            return Err(ValErr(f"code {code} is not registered"))
        return Ok(cls._code_to_type[code])

    @classmethod
    async def upd(
            cls,
            types: Iterable[type],
            order: list[str] | None = None) -> Res[None]:
        async with cls._lock:
            for t in types:
                code_res = cls.get_from_type(t)
                if isinstance(code_res, Err):
                    log.err(
                        f"cannot get code {code}: {code_res.err_value}"
                        " => skip")
                    continue
                code = code_res.ok_value

                validate_res = cls.validate(code)
                if isinstance(validate_res, Err):
                    log.err(
                        f"code {code} is not valid:"
                        f" {validate_res.err_value} => skip")
                    continue

                cls._code_to_type[code] = t

            cls._codes = list(cls._code_to_type.keys())
            if order:
                order_res = cls._order(order)
                if isinstance(order_res, Err):
                    return order_res

            return Ok(None)

    @classmethod
    def _order(cls, order: list[str]) -> Res[None]:
        sorted_codes: list[str] = []
        for o in order:
            if o not in cls._codes:
                log.warn(f"unrecornized order code {o} => skip")
                continue
            cls._codes.remove(o)
            sorted_codes.append(o)
        # bring rest of the codes
        for c in cls._codes:
            sorted_codes.append(c)

        cls._codes = sorted_codes
        return Ok(None)

    @classmethod
    def validate(cls, code: str) -> Res[None]:
        if not isinstance(code, str):
            return Err(ValErr(f"code {code} must be str"))
        if code == "":
            return Err(ValErr(f"empty code"))
        for i, c in enumerate(code):
            if i == 0 and not c.isalpha():
                return Err(ValErr(
                    f"code {code} must start with alpha"))
            if not c.isalnum() and c != "_":
                return Err(ValErr(
                    f"code {code} can contain only alnum"
                    " characters or underscore"))
        if len(code) > 256:
            return Err(ValErr(f"code {code} exceeds maxlen 256"))
        return Ok(None)

    @classmethod
    def get_from_type(cls, t: type) -> Res[str]:
        if isinstance(t, CodedMsgData):
            code = t.code
        else:
            codefn = getattr(t, "code", None)
            if codefn is None:
                return Err(ValErr(
                    f"msg data {t} must define \"code() -> str\" method"))
            if not isfunction(codefn):
                return Err(ValErr(
                    f"msg data {t} \"code\" attribute must be function,"
                    f" got {codefn}"))
            try:
                code = codefn()
            except Exception as err:
                log.catch(err)
                return Err(ValErr(
                    f"err {get_fully_qualified_name(err)} occured during"
                    f" msg data {t} {codefn} method call #~stacktrace"))

        validate_res = cls.validate(code)
        if isinstance(validate_res, Err):
            return validate_res

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

    # since we won't change data type for an existing message, we keep
    # code with the data
    code: str
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

    def serialize_for_net(self) -> Res[dict]:
        res = self.model_dump()

        # del server msg fields - we should do these before key deletion setup
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
            # all internal or skipped keys are deleted from the final
            # serialization
            if (
                    v is None
                    or k.startswith(("internal__", "skip__"))):
                keys_to_del.append(k)

        if not is_msid_found:
            raise ValueError(f"no msid field for raw msg {res}")
        for k in keys_to_del:
            del res[k]

        return Ok(res)

    @classmethod
    def deserialize_from_net(cls, data: dict) -> Res[Self]:
        """Recovers model of this class using dictionary."""
        # parse mdata separately according to it's registered type
        custom = data.get("data", None)
        if "code" not in data:
            return Err(ValErr(f"data {data} must have \"code\" field"))
        code = data["code"]
        if code not in Code._code_to_type:
            return Err(ValErr(f"unregistered code {code}"))
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
    def code(self) -> str:
        return "rxcat__ok"

class Welcome(BaseModel):
    """
    Welcome evt sent to every connected client.
    """
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
