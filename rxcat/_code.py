from typing import ClassVar

from pykit.err import NotFoundErr
from pykit.fcode import FcodeCore
from pykit.res import Res
from result import Err, Ok

from rxcat._msg import Msg


class CodeStorage:
    """
    Stores initialized object codes.
    """
    # active and legacy codes are bundled together under the same id, so
    # we use dict here
    mcode_to_mcodeid: ClassVar[dict[str, int]] = {}
    errcode_to_errcodeid: ClassVar[dict[str, int]] = {}

    indexed_active_mcodes: ClassVar[list[str]] = []
    indexed_active_errcodes: ClassVar[list[str]] = []

    indexed_mcodes: ClassVar[list[list[str]]] = []
    indexed_errcodes: ClassVar[list[list[str]]] = []
    fallback_errcodeid: ClassVar[int]

    @classmethod
    def update(cls):
        cls._update_mcodes()
        cls._update_errcodes()
        cls.fallback_errcodeid = cls.errcode_to_errcodeid[
            "rxcat_fallback_err"]

    @classmethod
    def _update_mcodes(cls):
        collections = FcodeCore.try_get_all_codes(Msg)
        assert collections, "must have at least one mcode defined"

        # put RegisterReq to index 0, according to the protocol
        register_req_start_index = -1
        for i, c in enumerate(collections):
            if c[0] == "rxcat_register_req":
                register_req_start_index = i
        assert register_req_start_index >= 0, "RegisterReq must be found"
        # replace whatever is at index 0, and put register req there
        collections[0], collections[register_req_start_index] = \
            collections[register_req_start_index], collections[0]

        cls.indexed_mcodes = collections
        for id, mcodes in enumerate(collections):
            cls.indexed_active_mcodes.append(mcodes[0])
            for mcode in mcodes:
                cls.mcode_to_mcodeid[mcode] = id

    @classmethod
    def _update_errcodes(cls):
        collections = FcodeCore.try_get_all_codes(Exception)
        assert collections, "must have at least one errcode defined"
        cls.indexed_errcodes = collections

        for id, errcodes in enumerate(collections):
            cls.indexed_active_errcodes.append(errcodes[0])
            for errcode in errcodes:
                cls.errcode_to_errcodeid[errcode] = id

    @classmethod
    def try_get_mcodeid_for_mcode(cls, mcode: str) -> int | None:
        res = cls.mcode_to_mcodeid.get(mcode, -1)
        if res == -1:
            return None
        return res

    @classmethod
    def try_get_errcodeid_for_errcode(cls, errcode: str) -> int | None:
        res = cls.errcode_to_errcodeid.get(errcode, -1)
        if res == -1:
            return None
        return res

    @classmethod
    def get_errcode_for_errcodeid(cls, errcodeid: int) -> Res[str]:
        for k, v in cls.errcode_to_errcodeid.items():
            if v == errcodeid:
                return Ok(k)
        return Err(NotFoundErr(f"errcode for errcodeid {errcodeid}"))

    @classmethod
    def try_get_mcodeid_for_mtype(cls, mtype: type[Msg]) -> int | None:
        mcode: str | None = \
            FcodeCore.try_get_active_code_for_type(mtype)
        if mcode:
            mcodeid: int = cls.mcode_to_mcodeid.get(mcode, -1)
            assert mcodeid != -1, "must find mcodeid for mcode"
            return mcodeid
        return None

    @classmethod
    def try_get_mcode_for_mtype(cls, mtype: type[Msg]) -> str | None:
        return FcodeCore.try_get_active_code_for_type(mtype)

    @classmethod
    def try_get_mcode_for_mcodeid(cls, mcodeid: int) -> str | None:
        for k, v in cls.mcode_to_mcodeid.items():
            if v == mcodeid:
                return k
        return None

    @classmethod
    def try_get_errcodeid_for_errtype(
        cls, errtype: type[Exception]
    ) -> int | None:
        errcode: str | None = \
            FcodeCore.try_get_active_code_for_type(errtype)
        if errcode:
            errcodeid: int = cls.errcode_to_errcodeid.get(errcode, -1)
            assert errcodeid != -1, "must find mcodeid for mcode"
            return errcodeid
        return None

    @classmethod
    def try_get_errcode_for_errtype(
        cls, errtype: type[Exception]
    ) -> str | None:
        return FcodeCore.try_get_active_code_for_type(errtype)
