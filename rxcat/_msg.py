from typing import Any, Callable, Self, TypeVar

from pydantic import BaseModel
from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import create_err_dto
from pykit.log import log
from pykit.res import Err, Ok, Res, resultify
from pykit.uuid import uuid4

TMdata_contra = TypeVar("TMdata_contra", contravariant=True)
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

    skip__target_connsids: list[str] | None = None
    """
    To which connsids the published msg should be addressed.
    """

    # since we won't change data type for an existing message, we keep
    # code with the data. Also it's placed here and not in ``data`` to not
    # interfere with custom fields, and for easier access
    skip__datacode: str
    """
    Code of msg's data.
    """
    data: Mdata

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        if "sid" not in data:
            data["sid"] = uuid4()
        super().__init__(**data)

    def __hash__(self) -> int:
        assert self.sid
        return hash(self.sid)

    # todo: use orwynn indication funcs for serialize/deserialize methods

    async def serialize_to_net(self) -> Res[dict]:
        final = self.model_dump()

        data = final["data"]
        # serialize exception to errdto
        if isinstance(data, Exception):
            err_dto_res = create_err_dto(data)
            if isinstance(err_dto_res, Err):
                return err_dto_res
            data = err_dto_res.okval

        codeid_res = await Code.get_regd_codeid(self.skip__datacode)
        if isinstance(codeid_res, Err):
            return codeid_res
        final["datacodeid"] = codeid_res.okval

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
            raise ValueError(f"no sid field for rmsg {final}")
        for k in keys_to_del:
            del final[k]

        return Ok(final)

    @classmethod
    async def _parse_rmsg_code(cls, rmsg: dict) -> Res[str]:
        if "datacodeid" not in rmsg:
            return Err(ValErr(f"data {rmsg} must have \"codeid\" field"))
        codeid = rmsg["datacodeid"]
        del rmsg["datacodeid"]
        if not isinstance(codeid, int):
            return Err(ValErr(
                f"invalid type of codeid {codeid}, expected int"))

        code_res = await Code.get_regd_code_by_id(codeid)
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval
        if not Code.has_code(code):
            return Err(ValErr(f"unregd code {code}"))

        return Ok(code)

    @classmethod
    async def _parse_rmsg_data(cls, rmsg: dict) -> Res[Mdata]:
        data = rmsg.get("data", None)

        code_res = await cls._parse_rmsg_code(rmsg)
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval

        rmsg["skip__datacode"] = code

        custom_type_res = await Code.get_regd_type(code)
        if isinstance(custom_type_res, Err):
            return custom_type_res
        custom_type = custom_type_res.okval

        deserialize_custom = getattr(custom_type, "deserialize", None)
        final_deserialize_fn: Callable[[], Any]
        if issubclass(custom_type, BaseModel):
            # for case of rmsg with empty data field, we'll try to initialize
            # the type without any fields (empty dict)
            if data is None:
                data = {}
            elif not isinstance(data, dict):
                return Err(ValErr(
                    f"if custom type ({custom_type}) is a BaseModel, data"
                    f" {data} must be a dict, got type {type(data)}"))
            final_deserialize_fn = lambda: custom_type(**data)
        elif deserialize_custom is not None:
            final_deserialize_fn = lambda: deserialize_custom(data)
        else:
            # for arbitrary types: just pass data as init first arg
            final_deserialize_fn = lambda: custom_type(data)

        return resultify(final_deserialize_fn)

    @classmethod
    async def deserialize_from_net(cls, rmsg: dict) -> Res[Self]:
        """Recovers model of this class using dictionary."""
        # parse mdata separately according to it's regd type
        data_res = await cls._parse_rmsg_data(rmsg)
        if isinstance(data_res, Err):
            return data_res
        data = data_res.okval

        if "lsid" not in rmsg:
            rmsg["lsid"] = None

        rmsg = rmsg.copy()
        # don't do redundant serialization of Any type
        rmsg["data"] = None
        model = cls.model_validate(rmsg.copy())
        model.data = data
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

class Reg(BaseModel):
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
        return "rxcat__reg"
