from pykit.code import Code
from pykit.res import Res, Err


async def get_registered_codeid_by_type(t: type) -> Res[int]:
    code_res = await Code.get_registered_code_by_type(t)
    if isinstance(code_res, Err):
        return code_res
    code = code_res.okval
    return await Code.get_registered_codeid(code)
