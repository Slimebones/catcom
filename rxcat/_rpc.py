"""
Remote procedure call support for rxcat.

Core messages - RpcReq and RpcEvt. Each of it has identification "key", in
format "<fn_code>::<token>". Token is UUID4 string to differentiate which
exact call should receive the result.
"""
from typing import Any, Awaitable, Callable, Protocol, TypeVar

from pykit.fcode import code
from pykit.res import Res
from rxcat import ServerBus
from rxcat._msg import Evt, Req


@code("rxcat_rpc_req")
class RpcReq(Req):
    key: str
    kwargs: dict
    """
    Any parseable kwargs passed to rpc fn.
    """

@code("rxcat_rpc_evt")
class RpcEvt(Evt):
    key: str
    val: Any
    """
    Returned value can be anything parseable, including exceptions.
    """

class Rpcfn(Protocol):
    async def __call__(self, **kwargs: Any) -> Res[Any]: ...
TRpcfn = TypeVar("TRpcfn", bound=Rpcfn)
def server_rpc(code: str):
    def inner(target: TRpcfn) -> TRpcfn:
        ServerBus.register_rpc(code, target)
        return target
    return inner
