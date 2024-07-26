from typing import Any, Callable, Self, TypeVar

from pydantic import BaseModel
from pykit.code import Code
from pykit.err import ValErr
from pykit.err_utils import create_err_dto
from pykit.log import log
from pykit.res import Err, Ok, Res, resultify
from pykit.uuid import uuid4

TMbody_contra = TypeVar("TMbody_contra", contravariant=True)
Mbody = Any
"""
Any custom body bus user interested in. Must be serializable and implement
`code() -> str` method.
"""

class Msg(BaseModel):
    """
    Basic unit flowing in the bus.

    Note that any field set to None won't be serialized.

    Fields prefixed with "skip__" won't pass net serialization process.

    Msgs are internal to rxcat implementation. The bus user is only interested
    in the actual body he is operating on, and which connections they are
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

    # since we won't change body type for an existing message, we keep
    # code with the body. Also it's placed here and not in ``body`` to not
    # interfere with custom fields, and for easier access
    skip__bodycode: str
    """
    Code of msg's body.
    """
    body: Mbody

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **body):
        if "sid" not in body:
            body["sid"] = uuid4()
        super().__init__(**body)

    def __hash__(self) -> int:
        assert self.sid
        return hash(self.sid)

    # todo: use orwynn indication funcs for serialize/deserialize methods

    async def serialize_to_net(self) -> Res[dict]:
        final = self.model_dump()

        body = final["body"]
        # serialize exception to errdto
        if isinstance(body, Exception):
            err_dto_res = await create_err_dto(body)
            if isinstance(err_dto_res, Err):
                return err_dto_res
            body = err_dto_res.okval.model_dump(exclude={"stacktrace"})

        codeid_res = await Code.get_regd_codeid(self.skip__bodycode)
        if isinstance(codeid_res, Err):
            return codeid_res
        final["bodycodeid"] = codeid_res.okval

        if "skip__connsid" in final and final["skip__connsid"] is not None:
            # connsids must exist only inside server bus, it's probably an err
            # if a msg is tried to be serialized with connsid, but we will
            # throw a warning for now, and ofcourse del the field
            log.warn(
                "connsids must exist only inside server bus, but it is tried"
                f" to serialize msg {self} with connsid != None => ignore"
            )

        keys_to_del = self._get_keys_to_del_from_serialized(final)

        for k in keys_to_del:
            del final[k]

        final["body"] = body
        if not body and "body" in final:
            del final["body"]
        return Ok(final)

    @classmethod
    async def _parse_rmsg_code(cls, rmsg: dict) -> Res[str]:
        if "bodycodeid" not in rmsg:
            return Err(ValErr(f"msg {rmsg} must have \"codeid\" field"))
        codeid = rmsg["bodycodeid"]
        del rmsg["bodycodeid"]
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
    def _get_keys_to_del_from_serialized(cls, body: dict) -> list[str]:
        keys_to_del: list[str] = []
        is_msid_found = False
        for k, v in body.items():
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
            raise ValueError(f"no sid field for rmsg {body}")
        return keys_to_del

    @classmethod
    async def _parse_rmsg_body(cls, rmsg: dict) -> Res[Mbody]:
        body = rmsg.get("body", None)

        code_res = await cls._parse_rmsg_code(rmsg)
        if isinstance(code_res, Err):
            return code_res
        code = code_res.okval

        rmsg["skip__bodycode"] = code

        custom_type_res = await Code.get_regd_type_by_code(code)
        if isinstance(custom_type_res, Err):
            return custom_type_res
        custom_type = custom_type_res.okval

        deserialize_custom = getattr(custom_type, "deserialize", None)
        final_deserialize_fn: Callable[[], Any]
        if issubclass(custom_type, BaseModel):
            # for case of rmsg with empty body field, we'll try to initialize
            # the type without any fields (empty dict)
            if body is None:
                body = {}
            elif not isinstance(body, dict):
                return Err(ValErr(
                    f"if custom type ({custom_type}) is a BaseModel, body"
                    f" {body} must be a dict, got type {type(body)}"))
            final_deserialize_fn = lambda: custom_type(**body)
        elif deserialize_custom is not None:
            final_deserialize_fn = lambda: deserialize_custom(body)
        else:
            # for arbitrary types: just pass body as init first arg
            final_deserialize_fn = lambda: custom_type(body)

        return resultify(final_deserialize_fn)

    @classmethod
    async def deserialize_from_net(cls, rmsg: dict) -> Res[Self]:
        """Recovers model of this class using dictionary."""
        # parse body separately according to it's regd type
        body_res = await cls._parse_rmsg_body(rmsg)
        if isinstance(body_res, Err):
            return body_res
        body = body_res.okval

        if "lsid" not in rmsg:
            rmsg["lsid"] = None

        rmsg = rmsg.copy()
        # don't do redundant serialization of Any type
        rmsg["body"] = None
        model = cls.model_validate(rmsg.copy())
        model.body = body
        return Ok(model)

TMsg = TypeVar("TMsg", bound=Msg)

# lowercase to not conflict with result.Ok
class ok:
    @staticmethod
    def code() -> str:
        return "rxcat::ok"

class Welcome(BaseModel):
    """
    Welcome evt sent to every connected client.
    """
    codes: list[str]

    @staticmethod
    def code() -> str:
        return "rxcat::welcome"
