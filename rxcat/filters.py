from rxcat import ServerBus

__all__ = [
    "disable_subfn_lsid"
]


def disable_subfn_lsid(_):
    ServerBus.ie().set_ctx_subfn_lsid(None)
