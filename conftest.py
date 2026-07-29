# SPDX-License-Identifier: Apache-2.0
"""Root pytest configuration shared by the core and module test suites."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import os
from typing import Any, Literal, cast

import pytest
import pytest_asyncio

from sensorkit.api import Service
from sensorkit.backend.base import Subject
from sensorkit.common.keyword import Keyword
from sensorkit.core.client import SensorKit
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.data.graph import DataGraph
from sensorkit.data.local import AppSink, AppSource

type BackendKind = Literal["fake", "nats"]

localonly = os.getenv("ENV", "").lower() == "local"
backend_impl = cast(BackendKind, os.getenv("SK_TEST_BACKEND", "fake"))


@functools.cache
def docker_available() -> bool:
    """Whether a Docker daemon able to run Linux containers is reachable."""
    try:
        import testcontainers  # noqa: F401
        from docker.errors import DockerException

        import docker
    except ImportError:
        return False

    try:
        # We require Linux images for testcontainers.
        return docker.from_env().info().get("OSType") == "linux"
    except (DockerException, OSError):
        return False


def pytest_collection_modifyitems(items):
    """Skip tests whose environment this run cannot provide.

    `localonly` covers hardware, a running backend, or another site-local
    service. `docker` covers containers brought up by testcontainers.
    """
    skip_localonly = pytest.mark.skip(reason="local only test; enable by setting `ENV=local`")
    skip_docker = pytest.mark.skip(reason="requires a Docker daemon running Linux containers")

    for item in items:
        if not localonly and "localonly" in item.keywords:
            item.add_marker(skip_localonly)

        if "docker" in item.keywords and not docker_available():
            item.add_marker(skip_docker)


def skip_if_no_testcontainers():
    pytest.importorskip("docker", reason="docker unavailable")
    pytest.importorskip("testcontainers", reason="testcontainers unavailable")

    from docker.errors import DockerException

    import docker

    try:
        docker.from_env()
    except DockerException:
        pytest.skip("could not find docker daemon")


@contextlib.asynccontextmanager
async def get_backend(kind: BackendKind):
    from sensorkit.backend.base import Backend

    match kind:
        case "fake":
            from sensorkit.backend.fake import FakeBackendImpl

            yield Backend(impl=await FakeBackendImpl.create())
        case "nats":
            skip_if_no_testcontainers()

            from testcontainers.nats import NatsContainer

            from sensorkit.backend.nats import NATSBackendImpl

            with NatsContainer().with_command("-js") as container:
                uri = container.nats_uri().replace("localhost", "127.0.0.1")
                yield Backend(impl=await NATSBackendImpl.create(uri))
        case _:
            raise ValueError(f"Unknown backend kind: {kind}")


@pytest.fixture
def make_backend():
    """Return the async context manager that brings up a backend of the requested kind."""
    return get_backend


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


TEST_ENTITY = "testentity"
"""Name of the entity created by the `entity_impl` fixture and its typed variants."""


@contextlib.asynccontextmanager
async def registered_impl[T: EntityImpl](service_context, impl_type: type[T], name: str):
    """Register an entity implementation and make it the current execution context.

    Entering the context is what a live service does around every hook and command handler, so
    module code calling `sk.device()` or `sk.program()` resolves to this implementation.
    """
    impl = await service_context.register_impl(
        impl_type.for_service_context(service_context, name)
    )

    try:
        with impl.enter_context():
            yield impl
    finally:
        # Detaching stops the entity's lease renewal and background tasks. Left running, they
        # accumulate across a session and progressively slow every later test.
        with contextlib.suppress(Exception):
            await service_context.shutdown()


@pytest_asyncio.fixture
async def entity_impl(service_context):
    """A live generic EntityImpl on the fake backend, installed as the current entity context."""
    async with registered_impl(service_context, EntityImpl, TEST_ENTITY) as impl:
        yield impl


@pytest_asyncio.fixture
async def device_impl(service_context):
    """A live DeviceImpl on the fake backend, installed as the current device context."""
    async with registered_impl(service_context, DeviceImpl, TEST_ENTITY) as impl:
        yield impl


@pytest_asyncio.fixture
async def program_impl(service_context):
    """A live ProgramImpl on the fake backend, installed as the current program context."""
    async with registered_impl(service_context, ProgramImpl, TEST_ENTITY) as impl:
        yield impl


@pytest_asyncio.fixture
async def controller_impl(service_context):
    """A live ControllerImpl on the fake backend, installed as the current controller context."""
    async with registered_impl(service_context, ControllerImpl, TEST_ENTITY) as impl:
        yield impl


@pytest_asyncio.fixture
async def data_graph(device_impl):
    """Install and start a source-to-sink DataGraph on the `device_impl` entity.

    Device code that writes frames through `sk.device().data_graph()` reaches the returned
    graph, so what it wrote is readable from `graph.app_sink().consume()`.
    """
    await device_impl.kv_put_model(
        DataGraph(nodes={"source": AppSource(output=["sink"]), "sink": AppSink()})
    )

    graph = await device_impl.data_graph()

    yield graph

    await graph.stop()


class Recorder:
    """Records the keywords an entity publishes, in order.

    Replaces assertions against a mocked `publish` with assertions against the stream the entity
    actually wrote to.
    """

    def __init__(self):
        self.records: list[tuple[Subject, Any]] = []
        self._arrived = asyncio.Event()

    def append(self, subject: Subject, model: Any):
        """Record one published keyword."""
        self.records.append((subject, model))
        self._arrived.set()

    def all(self) -> list[Any]:
        """Every published keyword, oldest first."""
        return [model for _, model in self.records]

    def of[M: Keyword](self, keyword: type[M]) -> list[M]:
        """Every published instance of the given keyword type, oldest first."""
        return [model for model in self.all() if isinstance(model, keyword)]

    def latest[M: Keyword](self, keyword: type[M]) -> M:
        """The most recently published instance of the given keyword type.

        Raises:
            AssertionError: if the keyword was never published.
        """
        published = self.of(keyword)
        assert published, f"{keyword.__name__} was never published"
        return published[-1]

    def keys(self) -> set[str]:
        """The distinct keyword keys published so far."""
        return {subject.prop for subject, _ in self.records}

    async def wait_for[M: Keyword](self, keyword: type[M], *, count: int = 1, timeout=2.0) -> M:
        """Wait until the keyword has been published `count` times and return the latest.

        Raises:
            TimeoutError: if the keyword is not published often enough in time.
        """
        async with asyncio.timeout(timeout):
            while len(self.of(keyword)) < count:
                self._arrived.clear()
                await self._arrived.wait()

        return self.latest(keyword)


@pytest_asyncio.fixture
async def recorder(kit):
    """Return a factory that starts recording the keywords an entity publishes.

    Recording starts when the factory is called, so call it before the code under test publishes.
    """
    tasks: list[asyncio.Task] = []

    async def record(entity: str = TEST_ENTITY) -> Recorder:
        rec = Recorder()
        monitor = await kit.entity(entity).monitor_all()
        started = asyncio.Event()

        async def pump():
            started.set()

            async for subject, model in monitor:
                rec.append(subject, model)

        tasks.append(asyncio.create_task(pump()))
        await started.wait()

        return rec

    yield record

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
