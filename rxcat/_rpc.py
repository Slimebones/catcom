"""
Remote procedure call support for rxcat.

Core messages - RpcReq and RpcEvt. Each of it has identification "key", in
format "<fn_code>::<token>". Token is UUID4 string to differentiate which
exact call should receive the result.
"""
from typing import Any, Awaitable, Callable, TypeVar

from pykit.fcode import code
from rxcat._msg import Evt, Req


class RpcReq(Req):
    key: str
    kwargs: dict
    """
    Any parseable kwargs passed to rpc fn.
    """

class RpcEvt(Evt):
    key: str
    retval: Any
    """
    Returned val can be anything parseable, including exceptions.
    """

RpcFn = Callable[[dict], Awaitable[Any]]
TRpcFn = TypeVar("TRpcFn", bound=RpcFn)
def server_rpc(code: str):
    def inner(target: TRpcFn) -> TRpcFn:
        return target
    return inner
