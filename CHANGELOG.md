# 0.3.0 - UNRELEASED

- updated pykit@0.8.0
- listed public exports
- renamed throw_err_evt() -> throw()
- added ServerBusCfg.are_errs_catchlogged which allows to catch+reraise thrown to the bus errs
- removed ServerBusCfg.is_invoked_action_unhandled_errs_logged in favor of ServerBusCfg.are_errs_catchlogged
- ServerBusCfg: renamed register -> register_fn
- setup ContextVar for subaction subscriber call
- added support for custom subaction context functions
- added ErrEvt.errtype which is set to fully qualified string representation of an error type
- added SEND/RECV callbacks defined as cfg.on_send and cfg.on_recv
- added Rpc support
- added support of multiple Transport layers and their Conn objects
- added ErrDto.stacktrace
- renamed InitdClientEvt to WelcomeEvt, changed the code to "rxcat__welcome_evt"
- changed code "err-evt" to "rxcat__err_evt"
- changed code "ok-evt" to "rxcat__ok_evt"
- changed RegisterReq code to "rxcat__register_req"
- renamed msg fields - all to snake case; introduce "skipnet__" prefix to not serialize field to the net
- renamed Msg.serialize_json to Msg.serialize_for_net; renamed Msg.deserialize_json to Msg.deserialize_from_net
- make officially public ErrEvt.skipnet__err which holds the original err object

# 0.2.3

- updated pykit

# 0.2.2

- updated pykit

# 0.2.1

- put RegisterReq to index 0

# 0.2.0

- connections now use sids instead of ids as well as subscriptions
- added connection management as public API
- added client registration
- subscribers can now optionally return `Res[Msg | list[Msg] | None]`
  instead of throwing an error, or to return message or list of messages to
  be published
- pub actions also can optionally return `Res[None]` instead of throwing an
  error
- internal bus stability improvements

# 0.1.1

- dep update

# 0.1.0

- protocol is ready!
