"""
Tests for dual UDL endpoint support (poll from one, upload imagery to another).

Equivalent of the machina module's test_dual_instance_publisher.py: tests use
two real UDLx-enabled MACHINA instances (not mocks) to verify:
- Backward compatibility (single-endpoint mode, api.upload=None)
- Split mode: CollectRequests polled from instance A, SkyImagery uploaded to B
- CollectResponses always go to the polling endpoint
- Multi-frame sets land entirely on the upload endpoint with correct grouping

Instance URLs default to localhost:30080 (A) and localhost:31080 (B) and can
be overridden with UDLX_A_BASE_URL / UDLX_B_BASE_URL.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from unifieddatalibrary import AsyncUnifieddatalibrary, omit
from unifieddatalibrary.types import CollectRequestFull

from sensorkit.astro.common import SitePosition
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.udl.models import UDLAPIConfig, UDLConfig, UDLEndpointConfig
from sensorkit.udl.program import UDLProgram

UDLX_A_BASE_URL = os.getenv("UDLX_A_BASE_URL", "http://localhost:30080")
UDLX_B_BASE_URL = os.getenv("UDLX_B_BASE_URL", "http://localhost:31080")

TEST_SOURCE = "UDLX-TEST"

# UDLx requires these as filters on time-keyed datatypes
EPOCH_FILTER = ">2020-01-01T00:00:00.000000Z"


def _instance_available(base_url: str) -> bool:
    try:
        return httpx.get(f"{base_url}/healthz", timeout=2.0).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(
    not (_instance_available(UDLX_A_BASE_URL) and _instance_available(UDLX_B_BASE_URL)),
    reason=f"requires live UDLx instances at {UDLX_A_BASE_URL} and {UDLX_B_BASE_URL}",
)


# ── Query helpers ──


async def get_skyimagery_from_udlx(base_url: str, id_sensor: str) -> list[dict[str, Any]]:
    """Query SkyImagery records from a UDLx instance by idSensor."""
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        resp = await client.get(
            "/udl/skyimagery",
            params={"idSensor": id_sensor, "expStartTime": EPOCH_FILTER},
        )
        if resp.status_code == 200:
            content = resp.json()
            print(
                f"Query {base_url}/udl/skyimagery for idSensor={id_sensor}: "
                f"{len(content)} results"
            )
            return content
        print(f"Query failed with status {resp.status_code}: {resp.text}")
        return []


async def get_collectresponses_from_udlx(
    base_url: str, id_request: str
) -> list[dict[str, Any]]:
    """Query CollectResponse records from a UDLx instance by idRequest."""
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        resp = await client.get(
            "/udl/collectresponse",
            params={"idRequest": id_request, "createdAt": EPOCH_FILTER},
        )
        if resp.status_code == 200:
            content = resp.json()
            print(
                f"Query {base_url}/udl/collectresponse for idRequest={id_request}: "
                f"{len(content)} results"
            )
            return content
        print(f"Query failed with status {resp.status_code}: {resp.text}")
        return []


# ── Data-graph / queue stand-ins ──


class _MultiFrameBinding:
    """Stands in for the bound sk program: provides data_graph() -> app_sink()."""

    def __init__(self, context: dict[str, Any], num_frames: int = 3) -> None:
        self.context = context
        self.frames = [
            (Context({**self.context, "frame_num": i}), f"frame{i}_data".encode("utf-8"))
            for i in range(num_frames)
        ]

    async def data_graph(self):
        return _MultiFrameGraph(self.frames)


class _MultiFrameGraph:
    def __init__(self, frames: list[tuple[dict[str, Any], bytes]]):
        self._frames = frames

    def app_sink(self):
        return _MultiFrameSink(self._frames)


class _MultiFrameSink:
    def __init__(self, frames: list[tuple[dict[str, Any], bytes]]):
        self._frames = frames

    async def consume(self):
        for context, data in self._frames:
            yield context, data


class _StubQueue:
    """Minimal TaskQueue stand-in for polling tests (no offer windows)."""

    def __init__(self) -> None:
        self.pushed: list[CollectRequestFull] = []

    async def push_task(self, request: CollectRequestFull) -> None:
        self.pushed.append(request)

    def iter(self):
        return iter(self.pushed)


# ── Program builders ──


def make_collect_request(id_sensor: str, num_frames: int = 3) -> CollectRequestFull:
    """CollectRequest as the program would have received it from polling."""
    return CollectRequestFull.model_validate(
        {
            "id": str(uuid.uuid1()),
            "classificationMarking": "U",
            "dataMode": "TEST",
            "source": TEST_SOURCE,
            "type": "DIRECTED_SEARCH",
            "startTime": datetime.now(UTC).isoformat(),
            "endTime": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            "origSensorId": id_sensor,
            "idSensor": id_sensor,
            "satNo": 51850,
            "numFrames": num_frames,
            "integrationTime": 1500,
        }
    )


def make_program(
    base_url: str,
    id_sensor: str,
    collect_request: CollectRequestFull,
    binding: _MultiFrameBinding,
    upload_base_url: str | None = None,
) -> UDLProgram:
    """
    Build a UDLProgram wired the way program_init() leaves it, pointed at
    live UDLx instances (which accept any username/password).

    With upload_base_url set, CollectRequests are polled from base_url while
    SkyImagery is uploaded to upload_base_url — the dual-endpoint split.
    """
    program = UDLProgram()
    program.config = UDLConfig(
        controller="controller1",
        api=UDLAPIConfig(
            base_url=base_url,
            id_sensor=id_sensor,
            source=TEST_SOURCE,
            upload=(
                UDLEndpointConfig(base_url=upload_base_url)
                if upload_base_url
                else None
            ),
        ),
    )
    program.client = AsyncUnifieddatalibrary(
        username="test",
        password="test",
        base_url=base_url,
        timeout=program.config.api.timeout,
    )
    program._udl_username = "test"
    program._udl_password = "test"
    if upload_base_url:
        program.upload_client = AsyncUnifieddatalibrary(
            username="test",
            password="test",
            base_url=upload_base_url,
            timeout=program.config.api.upload.timeout,
        )
        program._upload_username = "test"
        program._upload_password = "test"
    else:
        program.upload_client = program.client
        program._upload_username = program._udl_username
        program._upload_password = program._udl_password
    program._site = SitePosition(
        latitude_degrees=36.06789,
        longitude_degrees=-115.121193,
        altitude_km=2.0,
    )
    program.tasks = {collect_request.id: collect_request}
    program.program = binding  # _publish_loop only needs data_graph()
    program.queue = _StubQueue()
    return program


async def _close_clients(program: UDLProgram) -> None:
    """Close a program's SDK client(s) (the single-endpoint case aliases them)."""
    if program.upload_client is not program.client:
        await program.upload_client.close()
    await program.client.close()


# ── Fixtures ──


@pytest.fixture
def udlx_a_base_url() -> str:
    """Base URL for UDLx instance A (polling endpoint)"""
    return UDLX_A_BASE_URL


@pytest.fixture
def udlx_b_base_url() -> str:
    """Base URL for UDLx instance B (upload endpoint)"""
    return UDLX_B_BASE_URL


@pytest.fixture
def id_sensor(udlx_a_base_url: str, udlx_b_base_url: str):
    """
    Unique sensor registered in BOTH instances for the duration of a test.

    Registering in both instances ensures an absence of SkyImagery in one
    proves "no upload happened", not "upload was rejected by the idSensor
    foreign key".

    Cleans up CollectResponses, CollectRequests, SkyImagery, and the sensor
    from both instances afterwards.
    """
    sensor_id = f"UDLX-TEST-{uuid.uuid4().hex[:8]}"
    sensor = {
        "classificationMarking": "U",
        "dataMode": "TEST",
        "source": TEST_SOURCE,
        "idSensor": sensor_id,
        "sensorName": sensor_id,
    }

    urls = [udlx_a_base_url, udlx_b_base_url]
    for base_url in urls:
        resp = httpx.post(f"{base_url}/udl/sensor", json=sensor, timeout=30.0)
        assert resp.status_code == 200, (
            f"Failed to register sensor in {base_url}: {resp.status_code} {resp.text}"
        )

    yield sensor_id

    # Cleanup: entities referencing the sensor first (FK), then the sensor
    for base_url in urls:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            responses = client.get(
                "/udl/collectresponse",
                params={"idSensor": sensor_id, "createdAt": EPOCH_FILTER},
            )
            if responses.status_code == 200:
                for record in responses.json():
                    client.delete(f"/udl/collectresponse/{record['id']}")

            requests = client.get(
                "/udl/collectrequest",
                params={"origSensorId": sensor_id, "startTime": EPOCH_FILTER},
            )
            if requests.status_code == 200:
                for record in requests.json():
                    client.delete(f"/udl/collectrequest/{record['id']}")

            imagery = client.get(
                "/udl/skyimagery",
                params={"idSensor": sensor_id, "expStartTime": EPOCH_FILTER},
            )
            if imagery.status_code == 200:
                for record in imagery.json():
                    client.delete(f"/udl/skyimagery/{record['id']}")

            client.delete(f"/udl/sensor/{sensor_id}")


@pytest.fixture
def collect_request(id_sensor: str) -> CollectRequestFull:
    """A CollectRequest already correlated to the program (preloaded into tasks)."""
    return make_collect_request(id_sensor)


@pytest.fixture
def publisher_context(collect_request: CollectRequestFull) -> Context:
    """Context emitted by the data graph for each frame."""
    context = Context(
        {
            "task_id": collect_request.id,
            "frame_num": 0,
            "image_width": 100,
            "image_height": 50,
            "date_obs": datetime.now(UTC).isoformat(),
            "exptime": 1.5,
            "bits_per_pixel": 16,
        }
    )
    context.set(FileInfo(path="test_frame.fits"))
    return context


@pytest_asyncio.fixture
async def publisher_program(
    request: pytest.FixtureRequest,
    id_sensor: str,
    collect_request: CollectRequestFull,
    publisher_context: dict[str, Any],
    udlx_a_base_url: str,
    udlx_b_base_url: str,
):
    """A UDLProgram preloaded with one task and a finite data graph, ready to publish.

    Indirect params (a dict, all keys optional):
      split:      route SkyImagery to instance B via api.upload (default False)
      num_frames: frames the data graph emits (default 1)

    Closes the SDK client(s) on teardown.
    """
    params: dict[str, Any] = getattr(request, "param", {})
    split = params.get("split", False)
    num_frames = params.get("num_frames", 1)

    program = make_program(
        base_url=udlx_a_base_url,
        id_sensor=id_sensor,
        collect_request=collect_request,
        binding=_MultiFrameBinding(context=publisher_context, num_frames=num_frames),
        upload_base_url=udlx_b_base_url if split else None,
    )
    try:
        yield program
    finally:
        await _close_clients(program)


@pytest_asyncio.fixture
async def seeded_collect_request(
    id_sensor: str, udlx_a_base_url: str
) -> CollectRequestFull:
    """A CollectRequest tasked into instance A for the poller to discover.

    Cleaned up by the id_sensor fixture (it deletes CollectRequests by origSensorId).
    """
    seeded = make_collect_request(id_sensor)
    payload = seeded.model_dump(by_alias=True, exclude_none=True, mode="json")
    async with httpx.AsyncClient(base_url=udlx_a_base_url, timeout=30.0) as client:
        resp = await client.post("/udl/collectrequest", json=payload)
        assert resp.status_code == 200, f"Failed to seed CollectRequest: {resp.text}"
    return seeded


@pytest_asyncio.fixture
async def polling_program(
    seeded_collect_request: CollectRequestFull,
    id_sensor: str,
    udlx_a_base_url: str,
    udlx_b_base_url: str,
):
    """Split-mode program that must DISCOVER its task by polling instance A.

    tasks start empty — the seeded CollectRequest is waiting in instance A — so
    the test exercises the real poll path. Closes SDK client(s) on teardown.
    """
    context = Context(
        {
            "task_id": seeded_collect_request.id,
            "frame_num": 0,
            "date_obs": datetime.now(UTC).isoformat(),
            "exptime": 1.5,
        }
    )
    context.set(FileInfo(path="polled_frame.fits"))
    program = make_program(
        base_url=udlx_a_base_url,
        id_sensor=id_sensor,
        collect_request=seeded_collect_request,
        binding=_MultiFrameBinding(context=context, num_frames=1),
        upload_base_url=udlx_b_base_url,
    )
    program.tasks = {}  # poller must discover the task, not find it preloaded
    try:
        yield program
    finally:
        await _close_clients(program)


@pytest.fixture
def config(
    _mock_sk_program,
    monkeypatch: pytest.MonkeyPatch,
    udlx_a_base_url: str,
) -> UDLConfig:
    """No-certs, no-username/password UDLConfig wired into the mocked sk.program().

    Clears the credential env vars and points env_file at a nonexistent path so
    the program resolves to unauthenticated mode, and makes the mocked
    sk.program().kv_get_model(UDLConfig) return this config (other models raise,
    mimicking "no saved state"). monkeypatch restores the environment on teardown.
    """
    monkeypatch.delenv("UDL_USERNAME", raising=False)
    monkeypatch.delenv("UDL_PASSWORD", raising=False)

    config = UDLConfig(
        controller="controller1",
        api=UDLAPIConfig(
            base_url=udlx_a_base_url,
            id_sensor="UDLX-TEST-NOAUTH",
            source=TEST_SOURCE,
            env_file="/nonexistent/.env",
        ),
    )

    async def _kv_get_model(model):
        if model is UDLConfig:
            return config
        raise Exception("no saved state")

    _mock_sk_program.kv_get_model = AsyncMock(side_effect=_kv_get_model)
    return config


@pytest_asyncio.fixture
async def program(config: UDLConfig):
    """UDLProgram brought up via the real program_init() and torn down after.

    program_init() is the credential-resolution path under test; building it
    here means a regression (RuntimeError on missing creds) surfaces as a setup
    failure. program_deinit() cancels the poller/publisher and closes clients.
    """
    program = UDLProgram()
    await program.program_init()
    try:
        yield program
    finally:
        await program.program_deinit()


# ── Tests ──


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_program", [{"split": False, "num_frames": 1}], indirect=True)
async def test_single_endpoint_upload_no_split(
    publisher_program: UDLProgram,
    collect_request: CollectRequestFull,
    id_sensor: str,
    udlx_a_base_url: str,
    udlx_b_base_url: str,
) -> None:
    """
    BEHAVIOR: When api.upload is None, system behaves exactly as before.
    SkyImagery is uploaded to the same endpoint that is polled; instance B
    receives nothing.

    This test verifies backward compatibility.
    """
    assert publisher_program.upload_client is publisher_program.client, (
        "Without api.upload the upload client should alias the primary client"
    )
    await publisher_program._publish_loop()

    # Assert: SkyImagery exists in instance A
    imagery_a = await get_skyimagery_from_udlx(udlx_a_base_url, id_sensor)
    assert len(imagery_a) == 1, "Expected 1 SkyImagery in instance A"

    record = imagery_a[0]
    assert record["idSensor"] == id_sensor
    assert record["source"] == TEST_SOURCE
    assert record["classificationMarking"] == "U"
    assert record["satNo"] == collect_request.sat_no
    assert record["sequenceId"] == 1, "sequenceId must be 1-based"
    assert record["frameWidthPixels"] == 100
    assert record["frameHeightPixels"] == 50

    # Assert: NO SkyImagery in instance B
    imagery_b = await get_skyimagery_from_udlx(udlx_b_base_url, id_sensor)
    assert len(imagery_b) == 0, "Expected 0 SkyImagery in instance B (single-endpoint mode)"


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_program", [{"split": True, "num_frames": 1}], indirect=True)
async def test_split_uploads_imagery_to_b_only(
    publisher_program: UDLProgram,
    id_sensor: str,
    udlx_a_base_url: str,
    udlx_b_base_url: str,
) -> None:
    """
    BEHAVIOR: With api.upload.base_url pointing at instance B, SkyImagery is
    uploaded ONLY to B while the primary endpoint (A) receives none.

    This is the core poll-here/upload-there split.
    """
    await publisher_program._publish_loop()

    # Assert: SkyImagery exists ONLY in instance B
    imagery_b = await get_skyimagery_from_udlx(udlx_b_base_url, id_sensor)
    imagery_a = await get_skyimagery_from_udlx(udlx_a_base_url, id_sensor)

    assert len(imagery_b) == 1, "Expected 1 SkyImagery in instance B (upload endpoint)"
    assert len(imagery_a) == 0, "Expected 0 SkyImagery in instance A (polling endpoint)"

    assert imagery_b[0]["idSensor"] == id_sensor
    assert imagery_b[0]["sequenceId"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_program", [{"split": True, "num_frames": 3}], indirect=True)
async def test_split_multi_frame_upload(
    publisher_program: UDLProgram,
    collect_request: CollectRequestFull,
    id_sensor: str,
    udlx_a_base_url: str,
    udlx_b_base_url: str,
) -> None:
    """
    BEHAVIOR: In split mode a multi-frame collect produces one SkyImagery per
    frame, ALL on the upload endpoint, grouped by imageSetId ==
    CollectRequest.id with 1-based sequenceIds.
    """
    await publisher_program._publish_loop()

    # Assert: 3 SkyImagery in instance B
    imagery_b = await get_skyimagery_from_udlx(udlx_b_base_url, id_sensor)
    assert len(imagery_b) == 3, "Expected 3 SkyImagery in instance B"

    # All frames grouped into one set keyed by the CollectRequest ID
    assert all(img["imageSetId"] == collect_request.id for img in imagery_b), (
        "All frames should share imageSetId == CollectRequest.id"
    )
    assert all(img["imageSetLength"] == 3 for img in imagery_b)

    # sequenceId is 1-based and unique per frame
    assert sorted(img["sequenceId"] for img in imagery_b) == [1, 2, 3], (
        "Expected 1-based sequenceIds 1..3"
    )

    # Assert: NO SkyImagery in instance A
    imagery_a = await get_skyimagery_from_udlx(udlx_a_base_url, id_sensor)
    assert len(imagery_a) == 0, "Expected 0 SkyImagery in instance A (polling endpoint)"


@pytest.mark.asyncio
async def test_poll_from_a_respond_to_a_publish_to_b(
    polling_program: UDLProgram,
    seeded_collect_request: CollectRequestFull,
    id_sensor: str,
    udlx_a_base_url: str,
    udlx_b_base_url: str,
) -> None:
    """
    BEHAVIOR: Full split-mode lifecycle against live instances:
    1. A CollectRequest tasked into instance A is picked up by the poller
    2. The ACCEPTED CollectResponse goes to A (the polling endpoint), not B
    3. Published SkyImagery goes to B (the upload endpoint), not A
    """
    # Poll instance A exactly as the background loop does
    await polling_program._poll_collect_requests()

    assert seeded_collect_request.id in polling_program.tasks, (
        "Poller should have picked up the seeded request"
    )
    assert len(polling_program.queue.pushed) == 1, "Request should be queued for execution"

    # ACCEPTED CollectResponse landed in A only
    responses_a = await get_collectresponses_from_udlx(udlx_a_base_url, seeded_collect_request.id)
    responses_b = await get_collectresponses_from_udlx(udlx_b_base_url, seeded_collect_request.id)
    assert len(responses_a) == 1, "Expected ACCEPTED CollectResponse in instance A"
    assert responses_a[0]["status"] == "ACCEPTED"
    assert responses_a[0]["idSensor"] == id_sensor
    assert len(responses_b) == 0, "CollectResponses must stay on the polling endpoint"

    # Publish imagery for the polled task
    await polling_program._publish_loop()

    imagery_b = await get_skyimagery_from_udlx(udlx_b_base_url, id_sensor)
    imagery_a = await get_skyimagery_from_udlx(udlx_a_base_url, id_sensor)
    assert len(imagery_b) == 1, "Expected 1 SkyImagery in instance B (upload endpoint)"
    assert len(imagery_a) == 0, "Expected 0 SkyImagery in instance A (polling endpoint)"


@pytest.mark.asyncio
async def test_program_init_without_credentials(program: UDLProgram) -> None:
    """
    BEHAVIOR: With no certs and no username/password configured, the program
    initializes and talks to a local UDL-like endpoint unauthenticated.

    Drives the real program_init() (via the `program` fixture, which the other
    tests bypass) so this exercises the credential-resolution path end-to-end.
    The UDLx instances accept unauthenticated requests, so an immediate poll
    must succeed.
    """
    assert program.client is not None
    assert program.upload_client is program.client, (
        "No api.upload → upload client aliases primary client"
    )
    # With no credentials, init must resolve the per-request omit so the SDK
    # allows an Authorization-less request.
    assert program._client_headers == {"Authorization": omit}

    # An unauthenticated poll against the live instance must succeed. Use the
    # same per-request headers the program applies in _poll_collect_requests;
    # without the omit the SDK raises before sending.
    await program.client.collect_requests.list(
        start_time=f"<{datetime.now(UTC):%Y-%m-%dT%H:%M:%S.%f}Z",
        extra_query={
            "origSensorId": "UDLX-TEST-NOAUTH",
            "endTime": f">{datetime.now(UTC):%Y-%m-%dT%H:%M:%S.%f}Z",
        },
        extra_headers=program._client_headers,
    )
