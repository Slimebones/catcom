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

Rpcfn = Callable[[dict], Awaitable[Any]]
TRpcfn = TypeVar("TRpcfn", bound=Rpcfn)
def server_rpc(code: str):
    def inner(target: TRpcfn) -> TRpcfn:
        return target
    return inner
