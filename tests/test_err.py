from pykit.err import ValErr

from rxcat._err import ErrDto


def test_err():
    def throw():
        raise ValErr("hello")

    try:
        throw()
    except ValErr as err:
        dto = ErrDto.create(err, 1)

    assert dto.codeid == 1
    assert dto.name == "pykit.err.ValErr"
    assert dto.msg == "hello"
    assert isinstance(dto.stacktrace, str)
