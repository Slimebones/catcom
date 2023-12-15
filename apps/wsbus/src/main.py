import argparse
import asyncio

from orwynn.apiversion import ApiVersion
from orwynn.boot import Boot
from orwynn.bootscript import Bootscript
from orwynn.http import LogMiddleware
from orwynn.module import Module
from orwynn.server import Server, ServerEngine


async def run_server(
    boot: Boot,
) -> None:
    """
    Run server with options provided from command line.

    Temporary method until Orwynn implements CLI and/or similar runner.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Orwynn Server Runner",
    )

    parser.add_argument("host", type=str, help="host to serve on")
    parser.add_argument("port", type=int, help="port to serve on")

    namespace: argparse.Namespace = parser.parse_args()

    await Server(
        engine=ServerEngine.Uvicorn,
        boot=boot,
    ).serve(
        host=namespace.host,
        port=namespace.port,
    )


def create_root_module() -> Module:
    return Module(
        "/",
        Providers=[],
        Controllers=[],
        imports=[],
    )


async def create_boot(
    bootscripts: list[Bootscript] | None = None,
) -> Boot:
    return await Boot.create(
        create_root_module(),
        global_middleware={
            LogMiddleware: ["*"]
        },
        bootscripts=bootscripts,
    )


async def main() -> None:
    await run_server(await create_boot())


if __name__ == "__main__":
    asyncio.run(main())
