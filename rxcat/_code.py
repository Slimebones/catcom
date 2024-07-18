from typing import ClassVar
from pykit.fcode import FcodeCore
from rxcat._msg import Msg


class CodeStorage:
    """
    Stores initialized object codes.
    """
    # active and legacy codes are bundled together under the same id, so
    # we use dict here
    _MCODE_TO_MCODEID: ClassVar[dict[str, int]] = {}
    _ERRCODE_TO_ERRCODEID: ClassVar[dict[str, int]] = {}

    _INDEXED_ACTIVE_MCODES: ClassVar[list[str]] = []
    _INDEXED_ACTIVE_ERRCODES: ClassVar[list[str]] = []

    INDEXED_MCODES: ClassVar[list[list[str]]] = []
    INDEXED_ERRCODES: ClassVar[list[list[str]]] = []

    @classmethod
    def update(cls):
        cls._update_mcodes()
        cls._update_errcodes()

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

        cls.INDEXED_MCODES = collections
        for id, mcodes in enumerate(collections):
            cls._INDEXED_ACTIVE_MCODES.append(mcodes[0])
            for mcode in mcodes:
                cls._MCODE_TO_MCODEID[mcode] = id

    @classmethod
    def _update_errcodes(cls):
        collections = FcodeCore.try_get_all_codes(Exception)
        assert collections, "must have at least one errcode defined"
        cls.INDEXED_ERRCODES = collections

        for id, errcodes in enumerate(collections):
            cls._INDEXED_ACTIVE_ERRCODES.append(errcodes[0])
            for errcode in errcodes:
                cls._ERRCODE_TO_ERRCODEID[errcode] = id

    @classmethod
    def try_get_mcodeid_for_mtype(cls, mtype: type[Msg]) -> int | None:
        mcode: str | None = \
            FcodeCore.try_get_active_code_for_type(mtype)
        if mcode:
            mcodeid: int = cls._MCODE_TO_MCODEID.get(mcode, -1)
            assert mcodeid != -1, "must find mcodeid for mcode"
            return mcodeid
        return None

    @classmethod
    def try_get_mcode_for_mtype(cls, mtype: type[Msg]) -> str | None:
        return FcodeCore.try_get_active_code_for_type(mtype)

    @classmethod
    def try_get_mcode_for_mcodeid(cls, mcodeid: int) -> str | None:
        for k, v in cls._MCODE_TO_MCODEID.items():
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
            errcodeid: int = cls._ERRCODE_TO_ERRCODEID.get(errcode, -1)
            assert errcodeid != -1, "must find mcodeid for mcode"
            return errcodeid
        return None

    @classmethod
    def try_get_errcode_for_errtype(
        cls, errtype: type[Exception]
    ) -> str | None:
        return FcodeCore.try_get_active_code_for_type(errtype)
