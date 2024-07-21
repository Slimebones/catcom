from rxcat import ServerBus


def disable_subfn_lsid(_):
    ServerBus.ie().set_ctx_subfn_lsid(None)
