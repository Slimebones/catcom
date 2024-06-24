# 0.2.0 - UNRELEASED

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
