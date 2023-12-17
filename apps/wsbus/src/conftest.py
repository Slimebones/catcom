import os
from contextlib import suppress
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from orwynn import Worker
from orwynn.app import App
from orwynn.boot import Boot
from orwynn.di.di import Di
from orwynn.sql import SQLUtils
from orwynn.testing import Client

from src.main import create_boot


@pytest.fixture(autouse=True)
def run_around_tests():
    os.environ["ORWYNN_MODE"] = "test"

    yield

    # Ensure that workers created in previous test does not migrate in the
    # next one
    _discard_workers()


def _discard_workers(W: type[Worker] = Worker):
    for NestedW in W.__subclasses__():
        _discard_workers(NestedW)
    W.discard(should_validate=False)
    with suppress(KeyError):
        del os.environ["ORWYNN_MODE"]
    with suppress(KeyError):
        del os.environ["ORWYNN_ROOT_DIR"]
    with suppress(KeyError):
        del os.environ["ORWYNN_APPRC_PATH"]


@pytest_asyncio.fixture
async def boot() -> AsyncGenerator[Boot, None]:
    boot: Boot = await create_boot()
    yield boot


@pytest.fixture
def app(boot: Boot) -> App:
    return boot.app


@pytest.fixture
def client(app: App) -> Client:
    return app.client


@pytest.fixture
def di(boot) -> Di:
    return Di.ie()
