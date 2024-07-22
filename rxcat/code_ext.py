from pykit.code import Code
from pykit.res import Err, Res


async def get_regd_codeid_by_type(t: type) -> Res[int]:
    code_res = await Code.get_regd_code_by_type(t)
    if isinstance(code_res, Err):
        return code_res
    code = code_res.okval
    return await Code.get_regd_codeid(code)
