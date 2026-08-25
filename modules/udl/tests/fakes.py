# SPDX-License-Identifier: Apache-2.0
"""Stand-ins for the UDL SDK: request builders and a recording client.

`CollectRequestFull` and friends are ordinary pydantic models, so the builders here return real
instances — the SDK validates them exactly as it validates what comes off the wire, including the
camelCase aliases the program round-trips through KV. `FakeUDLClient` replaces the network: it
records every call the program makes and serves whatever the test staged.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from unifieddatalibrary.types import CollectRequestFull

ISS_LINE1 = "1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002"
ISS_LINE2 = "2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000"


def make_collect_request(
    *,
    id="test-request-001",
    classification_marking="U",
    data_mode="TEST",
    source="TEST_SOURCE",
    origin=None,
    id_sensor="SENSOR-01",
    orig_sensor_id="SENSOR-01",
    sat_no=25544,
    orig_object_id="25544",
    start_time=None,
    end_time=None,
    integration_time=4000,
    num_frames=3,
    task_id="task-001",
    id_plan=None,
    external_id=None,
    **extra,
) -> CollectRequestFull:
    """Build a CollectRequest with the fields the program reads.

    `integration_time` is milliseconds, as UDL reports it. Extra keyword arguments are passed
    through under their SDK aliases, so a caller can attach an `elset`, `stateVector`, `ra`/`dec`,
    or anything else the schema allows.
    """
    start_time = start_time or datetime.now(UTC)
    end_time = end_time if end_time is not None else start_time + timedelta(minutes=10)

    return CollectRequestFull.model_validate(
        {
            "id": id,
            "classificationMarking": classification_marking,
            "dataMode": data_mode,
            "source": source,
            "origin": origin,
            "type": "DIRECTED_SEARCH",
            "idSensor": id_sensor,
            "origSensorId": orig_sensor_id,
            "satNo": sat_no,
            "origObjectId": orig_object_id,
            "startTime": start_time,
            "endTime": end_time,
            "integrationTime": integration_time,
            "numFrames": num_frames,
            "taskId": task_id,
            "idPlan": id_plan,
            "externalId": external_id,
            **extra,
        }
    )


def tle_request(sat_no=25544, **overrides) -> CollectRequestFull:
    """A CollectRequest carrying an Elset — the target type the program prefers."""
    return make_collect_request(
        sat_no=sat_no,
        elset={
            "classificationMarking": "U",
            "dataMode": "TEST",
            "source": "TEST_SOURCE",
            "epoch": datetime.now(UTC),
            "satNo": sat_no,
            "line1": ISS_LINE1,
            "line2": ISS_LINE2,
        },
        **overrides,
    )


def state_vector_request(**overrides) -> CollectRequestFull:
    """A CollectRequest carrying a StateVector, in UDL's km and km/s."""
    return make_collect_request(
        stateVector={
            "classificationMarking": "U",
            "dataMode": "TEST",
            "source": "TEST_SOURCE",
            "epoch": datetime.now(UTC),
            "xpos": 6778.0,
            "ypos": 0.0,
            "zpos": 0.0,
            "xvel": 0.0,
            "yvel": 7.5,
            "zvel": 0.0,
            "referenceFrame": "J2000",
        },
        **overrides,
    )


def radec_request(ra=180.0, dec=45.0, **overrides) -> CollectRequestFull:
    """A CollectRequest carrying only a fixed RA/Dec pointing."""
    return make_collect_request(ra=ra, dec=dec, **overrides)


@dataclass
class CollectRequestPage:
    """One page of CollectRequests, as the SDK's list() returns it."""

    items: list[CollectRequestFull] = field(default_factory=list)


@dataclass
class FakeCollectRequests:
    """The SDK's collect_requests resource."""

    page: CollectRequestPage = field(default_factory=CollectRequestPage)
    queries: list[dict[str, Any]] = field(default_factory=list)

    async def list(self, **kwargs) -> CollectRequestPage:
        self.queries.append(kwargs)
        return self.page


@dataclass
class FakeCollectResponses:
    """The SDK's collect_responses resource; `created` holds one dict per posted response."""

    created: list[dict[str, Any]] = field(default_factory=list)

    async def create(self, **kwargs) -> None:
        self.created.append(kwargs)

    def statuses(self) -> list[str]:
        """The status of every response posted, oldest first."""
        return [call["status"] for call in self.created]


@dataclass
class FakeEOObservations:
    """The SDK's observations.eo_observations resource; `batches` holds one list
    per unvalidated_publish (the filedrop bulk-drop the module uses)."""

    batches: list[list[dict[str, Any]]] = field(default_factory=list)

    async def unvalidated_publish(self, *, body, **kwargs) -> None:
        self.batches.append(body)


@dataclass
class FakeObservations:
    eo_observations: FakeEOObservations = field(default_factory=FakeEOObservations)


async def poll_once(program):
    """Drive the program's poll loop through one pass, then cancel it."""
    baseline = len(program.client.collect_requests.queries)
    poller = asyncio.create_task(program._poll_loop())
    try:
        async with asyncio.timeout(2.0):
            while len(program.client.collect_requests.queries) == baseline:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)  # let the pass finish handling the page
    finally:
        poller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poller


@dataclass
class FakeUpload:
    """Stands in for SkyImageryPublisher._upload, recording raw ZIP bytes.

    Same `fail` scripting as FakeSkyImagery: a list of exceptions/Nones for
    per-upload outcomes, or a single exception to fail every upload.
    """

    uploads: list[bytes] = field(default_factory=list)
    fail: Any = None

    async def __call__(self, zip_bytes: bytes) -> None:
        outcome = self.fail.pop(0) if isinstance(self.fail, list) else self.fail
        if outcome is not None:
            raise outcome

        self.uploads.append(zip_bytes)


@dataclass
class FakeSkyImagery:
    """The SDK's sky_imagery resource.

    `uploads` holds the raw ZIP bytes of every accepted upload. Set `fail` to a list of exceptions
    (or Nones) to script per-upload outcomes, or to a single exception to fail every upload.
    """

    uploads: list[bytes] = field(default_factory=list)
    fail: Any = None

    async def upload_zip(self, *, file: bytes, **kwargs) -> None:
        outcome = self.fail.pop(0) if isinstance(self.fail, list) else self.fail
        if outcome is not None:
            raise outcome

        self.uploads.append(file)


@dataclass
class FakeUDLClient:
    """Stands in for AsyncUnifieddatalibrary, recording what the program sends."""

    collect_requests: FakeCollectRequests = field(default_factory=FakeCollectRequests)
    collect_responses: FakeCollectResponses = field(default_factory=FakeCollectResponses)
    observations: FakeObservations = field(default_factory=FakeObservations)
    sky_imagery: FakeSkyImagery = field(default_factory=FakeSkyImagery)
    closed: bool = False

    async def close(self) -> None:
        self.closed = True
