import traceback
from typing import Self

from pydantic import BaseModel
from pykit.obj import get_fully_qualified_name
from pykit.res import Res
from result import Ok


class ErrDto(BaseModel):
    """
    Represents an error as data transfer object.

    "stacktrace" used to comply with other languages structures, for Python
    it's actually a traceback.
    """
    name: str
    msg: str
    stacktrace: str | None = None

    @classmethod
    def create(cls, err: Exception) -> Res[Self]:
        name = get_fully_qualified_name(err)
        msg = ", ".join([str(a) for a in err.args])
        stacktrace = None
        tb = err.__traceback__
        if tb:
            extracted_list = traceback.extract_tb(tb)
            stacktrace = ""
            for item in traceback.StackSummary.from_list(
                    extracted_list).format():
                stacktrace += item
        return Ok(cls(msg=msg, name=name, stacktrace=stacktrace))

    @staticmethod
    def code() -> str:
        return "err"
