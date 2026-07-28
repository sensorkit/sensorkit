# SPDX-License-Identifier: Apache-2.0
import contextlib
import os
from enum import StrEnum

import pytest
import pytest_asyncio

from sensorkit.api import Service
from sensorkit.core.client import SensorKit


class BackendKind(StrEnum):
    FAKE = "fake"
    NATS = "nats"


backend_impl = BackendKind(os.getenv("SK_TEST_BACKEND", "fake"))


def skip_if_no_testcontainers():
    pytest.importorskip("docker", reason="docker unavailable")
    pytest.importorskip("testcontainers", reason="testcontainers unavailable")

    import docker
    from docker.errors import DockerException

    try:
        docker.from_env()
    except DockerException:
        pytest.skip("could not find docker daemon")


@contextlib.asynccontextmanager
async def get_backend(kind: BackendKind):
    from sensorkit.backend.base import Backend

    match kind:
        case BackendKind.FAKE:
            from sensorkit.backend.fake import FakeBackendImpl

            yield Backend(impl=await FakeBackendImpl.create())
        case BackendKind.NATS:
            skip_if_no_testcontainers()

            from testcontainers.nats import NatsContainer

            from sensorkit.backend.nats import NATSBackendImpl

            with NatsContainer().with_command("-js") as container:
                uri = container.nats_uri().replace("localhost", "127.0.0.1")
                yield Backend(impl=await NATSBackendImpl.create(uri))


@pytest_asyncio.fixture(
    params=[
        BackendKind.FAKE,
        pytest.param(BackendKind.NATS, marks=pytest.mark.docker),
    ]
)
async def _backend(request):
    """Backend fixture for internal backend tests.

    This yields all registered backends.
    """
    async with get_backend(request.param.value) as backend:
        yield backend


@pytest_asyncio.fixture
async def backend():
    """Backend fixture for general tests.

    Returns the backend configured for testing (default=fake).
    """
    async with get_backend(backend_impl) as backend:
        yield backend


@pytest_asyncio.fixture
async def kit(backend):
    """Return a SensorKit instance backed by the `backend` fixture."""
    yield SensorKit(backend)


@pytest_asyncio.fixture
async def service_context(kit):
    """Return a registered service context on top of the `kit` fixture."""
    yield await kit.register_service("testservice", "0.1.0")


@pytest_asyncio.fixture
async def service(kit):
    svc = Service("test", "0.1.0")
    svc.client = kit

    yield svc

    with contextlib.suppress(Exception):
        await svc.stop()
