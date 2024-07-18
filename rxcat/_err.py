from typing import Self
from pydantic import BaseModel
from pykit.obj import get_fully_qualified_name
from rxcat._code import CodeStorage


class ErrDto(BaseModel):
    """
    Represents an error as data transfer object.
    """
    codeid: int | None = None
    """
    Is none for errors without assigned fcode.
    """
    name: str
    msg: str

    @classmethod
    def create(cls, err: Exception) -> Self:
        codeid = CodeStorage.try_get_errcodeid_for_errtype(type(err))
        name = get_fully_qualified_name(err)
        msg = ", ".join([str(a) for a in err.args])
        return cls(codeid=codeid, msg=msg, name=name)
