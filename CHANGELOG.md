# 1.0.10

- separate concept of Bmsg and Msg
- rename Conn -> Con
- export SubOpts, PubOpts

# 1.0.9

- change pykit to ryz

# 1.0.8

- rename to Yon

# 1.0.7

- msg: make ok be a base model
- msg/serialize: exclude zero-len objects from serialization

# 1.0.6

- rpc: use msg sid instead of key's attached uuid
- rpc: allow reg_rpc to accept custom rpc key

# 1.0.5

- rpc: remove "srpc__" prefix from rpc key

# 1.0.4

- rpc: now uses "::" key-token separator

# 1.0.3

- rpc/SrpcSend: rename "args" to "body"

# 1.0.2

- allow and use moduled codes

# 1.0.1

- rpc functions now accept body: BaseModel instead of args: BaseModel

# 1.0.0

Complete rebuild to data-oriented instead of msg-oriented. Some of features (but not all of them) are listed below.

- updated pykit@0.8.0
- listed public exports
- renamed throw_err_evt() -> throw()
- added ServerBusCfg.trace_errs_on_pub which allows to catch+reraise thrown to the bus errs
- removed ServerBusCfg.is_invoked_action_unhandled_errs_logged in favor of ServerBusCfg.trace_errs_on_pub
- ServerBusCfg: renamed reg -> reg_fn
- setup ContextVar for subfn subscriber call
- added support for custom subfn context functions
- added ErrEvt.errtype which is set to fully qualified string representation of an error type
- added SEND/RECV callbacks defined as cfg.on_send and cfg.on_recv
- added Rpc support
- added support of multiple Transport layers and their Conn objects
- added ErrDto.stacktrace
- renamed InitdClientEvt to WelcomeEvt, changed the code to "rxcat__welcome_evt"
- changed code "err-evt" to "rxcat__err_evt"
- changed code "ok-evt" to "rxcat__ok_evt"
- changed RegReq code to "rxcat__reg_req"
- renamed msg fields - all to snake case; introduce "skip__" prefix to not serialize field to the net
- renamed Msg.serialize_json to Msg.serialize_for_net; renamed Msg.deserialize_json to Msg.deserialize_from_net
- make officially public ErrEvt.skip__err which holds the original err object
- renamed SubAction to SubFn; renamed PubAction to PubFn
- added sub and rpc custom context managers support
- added Msg.connsid and Msg.target_connsids properties

# 0.2.3

- updated pykit

# 0.2.2

- updated pykit

# 0.2.1

- put RegReq to index 0

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
