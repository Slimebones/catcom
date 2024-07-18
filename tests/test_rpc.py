



# async def test_rpc(server_bus: ServerBus):
#     evt = asyncio.Event()

#     async def update_email(data: dict) -> Res[int]:
#         username = data["username"]
#         email = data["email"]
#         if username == "throw":
#             return Err(Exception())
#         assert username == "test_username"
#         assert email == "test_email"
#         return Ok(0)

#     async def on_send(connsids: set[str], rawmsg: dict):
#         evt.set()

#     ServerBus.register_rpc("update_email", update_email)
#     server_bus._cfg.on_send = on_send
#     # TODO:
#     #   here find a way to specify target connsids or _pub_to_net won't work
#     await server_bus.inner__accept_net_msg(RpcReq(
#         key="update_email:" + RandomUtils.makeid(),
#         kwargs={"username": "test_username", "email": "test_email"}))
#     await asyncio.wait_for(evt.wait(), 1)
