# SPDX-License-Identifier: Apache-2.0
"""Test SkyImagery metadata generation against the UDL schema."""

import io
import json
import zipfile
from datetime import UTC, datetime

import pytest

from sensorkit.astro.common import SitePosition
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.udl.models import (
    PublishConfig,
    ResponseStatus,
    SkyImageryPublishConfig,
    UDLAPIConfig,
    UDLConfig,
)
from sensorkit.udl.program import UDLProgram, _PublishProgress
from sensorkit.udl.publishers import SkyImageryPublisher

from .fakes import FakeUDLClient, FakeUpload, tle_request

SITE = SitePosition(
    latitude_degrees=41.9168354,
    longitude_degrees=-84.0290721,
    altitude_km=0.05,
)

# A UDL-compliant host: uploads raw-POST to {base_url}/filedrop/udl-skyimagery; the fixture
# stubs the publisher's _upload with a recorder. TestResolveUploadUrl covers URL derivation.
COMPLIANT_BASE_URL = "https://udl-compliant.example.mil"


def frame_context(name="test.fits", **fields):
    """Build a DataGraph-style context carrying a FileInfo keyword."""
    context = Context(fields)
    context.set(FileInfo(path=name))
    return context


def uploaded_metadata(program, index=-1):
    """The SkyImagery metadata JSON from an uploaded ZIP, newest by default."""
    zip_bytes = program._sky_imagery._upload.uploads[index]

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        (name,) = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
        return json.loads(zf.read(name))


@pytest.fixture
def program(program_impl):
    config = UDLConfig(
        controller="controller1",
        api=UDLAPIConfig(
            base_url=COMPLIANT_BASE_URL,
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        ),
        publish=PublishConfig(sky_imagery=SkyImageryPublishConfig()),
    )
    p = UDLProgram()
    p.config = config
    p.program = program_impl
    p._site = SITE
    p.client = FakeUDLClient()
    p.upload_client = p.client
    p._sky_imagery = SkyImageryPublisher(p)
    p._sky_imagery._upload = FakeUpload()
    return p


async def publish_frame(program, request, **context_fields):
    """Hand one collected frame to the program, as the publish loop does.

    Provides every context key SkyImageryPublisher.publish requires; tests
    override via context_fields.
    """
    program.tasks[request.id] = request
    context_fields.setdefault("exptime", 4.0)
    context_fields.setdefault("image_width", 2048)
    context_fields.setdefault("image_height", 2048)
    context_fields.setdefault("bits_per_pixel", 16)
    context = frame_context(
        task_id=request.id,
        frame_num=0,
        date_obs="2026-03-21T07:18:47.082000",
        **context_fields,
    )
    await program._handle_frame(context, b"\x00" * 100)


class TestSkyImageryMetadata:
    @pytest.mark.asyncio
    async def test_metadata_has_required_fields(self, program):
        """SkyImagery_Ingest requires classificationMarking, imageType, expStartTime, source, dataMode."""
        request = tle_request(classification_marking="U", data_mode="REAL", source="TEST_SOURCE")
        await publish_frame(
            program,
            request,
            exptime=4.0,
            image_width=8120,
            image_height=8120,
            bits_per_pixel=16,
        )

        metadata = uploaded_metadata(program)
        assert {
            "classificationMarking",
            "imageType",
            "expStartTime",
            "source",
            "dataMode",
        } <= metadata.keys()

    @pytest.mark.asyncio
    async def test_metadata_uses_config_source(self, program):
        await publish_frame(program, tle_request())
        assert uploaded_metadata(program)["source"] == "TEST_SOURCE"

    @pytest.mark.asyncio
    async def test_metadata_uses_config_id_sensor(self, program):
        await publish_frame(program, tle_request())
        assert uploaded_metadata(program)["idSensor"] == "SENSOR-01"

    @pytest.mark.asyncio
    async def test_metadata_sensor_ids(self, program):
        """idSensor and origSensorId both carry the configured sensor id, as on
        CollectResponses and EOObservations; origObjectId is not stamped (satNo suffices)."""
        await publish_frame(program, tle_request())

        metadata = uploaded_metadata(program)
        assert metadata["origSensorId"] == "SENSOR-01"
        assert "origObjectId" not in metadata

    @pytest.mark.asyncio
    async def test_sequence_id_starts_at_one(self, program):
        """sequenceId must be >= 1, though SensorKit numbers frames from 0."""
        await publish_frame(program, tle_request())
        assert uploaded_metadata(program)["sequenceId"] == 1

    @pytest.mark.asyncio
    async def test_metadata_includes_sensor_location(self, program):
        await publish_frame(program, tle_request())

        metadata = uploaded_metadata(program)
        assert metadata["senlat"] == SITE.latitude_degrees
        assert metadata["senlon"] == SITE.longitude_degrees
        assert metadata["senalt"] == SITE.altitude_km

    @pytest.mark.asyncio
    async def test_metadata_omits_sensor_location_without_site(self, program):
        """The controller may not carry a SitePosition; UDL accepts imagery without one."""
        program._site = None
        await publish_frame(program, tle_request())

        metadata = uploaded_metadata(program)
        assert "senlat" not in metadata

    @pytest.mark.asyncio
    async def test_metadata_matches_example(self, program):
        """Verify metadata structure matches the known-good example."""
        request = tle_request(classification_marking="U", sat_no=39120, data_mode="REAL")
        await publish_frame(
            program,
            request,
            exptime=5.344074,
            image_width=8120,
            image_height=8120,
            bits_per_pixel=16,
        )

        metadata = uploaded_metadata(program)
        assert metadata["classificationMarking"] == "U"
        assert metadata["idSensor"] == "SENSOR-01"
        assert metadata["satNo"] == 39120
        assert metadata["senlat"] == SITE.latitude_degrees
        assert metadata["senlon"] == SITE.longitude_degrees
        assert metadata["senalt"] == SITE.altitude_km
        assert metadata["imageSetLength"] == 3
        assert metadata["sequenceId"] == 1
        assert metadata["frameWidthPixels"] == 8120
        assert metadata["frameHeightPixels"] == 8120
        assert metadata["source"] == "TEST_SOURCE"
        assert metadata["dataMode"] == "REAL"
        assert metadata["imageType"] == "FITS"
        # Multi-frame set (num_frames=3) → imageSetId associates the frames.
        assert metadata["imageSetId"] == request.id

    @pytest.mark.asyncio
    async def test_imageset_id_omitted_for_single_image(self, program):
        """Per UDL: a single-image set (num_frames=1) doesn't need an imageSetId."""
        await publish_frame(program, tle_request(num_frames=1))

        metadata = uploaded_metadata(program)
        assert metadata["imageSetLength"] == 1
        assert "imageSetId" not in metadata

    @pytest.mark.asyncio
    async def test_imageset_id_present_for_multi_image(self, program):
        """A multi-frame set keeps imageSetId so the frames can be grouped."""
        request = tle_request(num_frames=5)
        await publish_frame(program, request)

        metadata = uploaded_metadata(program)
        assert metadata["imageSetLength"] == 5
        assert metadata["imageSetId"] == request.id


class TestUntaskedFrames:
    """Frames with no CollectRequest publish only with configured provenance."""

    CONTEXT = dict(
        task_id="not-a-known-task",
        frame_num=0,
        date_obs="2026-03-21T07:18:47.082000",
        exptime=4.0,
        image_width=2048,
        image_height=2048,
        bits_per_pixel=16,
        frame_count=1,
    )

    @pytest.mark.asyncio
    async def test_untasked_frame_dropped_without_provenance(self, program):
        await program._handle_frame(frame_context(**self.CONTEXT), b"\x00" * 100)
        assert program._sky_imagery._upload.uploads == []

    @pytest.mark.asyncio
    async def test_untasked_frame_uses_configured_provenance(self, program):
        cfg = program.config.publish.sky_imagery
        cfg.classification_marking = "U"
        cfg.data_mode = "TEST"
        cfg.origin = "TEST_ORG"

        await program._handle_frame(frame_context(**self.CONTEXT), b"\x00" * 100)

        metadata = uploaded_metadata(program)
        assert metadata["classificationMarking"] == "U"
        assert metadata["dataMode"] == "TEST"
        assert metadata["origin"] == "TEST_ORG"
        assert "idRequest" not in metadata
        assert "satNo" not in metadata


class TestResolveUploadUrl:
    """UDL proper serves the imagery filedrop on a dedicated subdomain, so the
    known UDL hosts map there; any other base_url is presumed to serve the
    UDL-compliant route itself.
    """

    def resolve(self, program):
        return program._sky_imagery._get_upload_url(program.config.api)

    def test_default_base_url_uses_production_imagery(self, program):
        program.config.api.base_url = None
        assert (
            self.resolve(program)
            == "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_test_base_url_uses_test_imagery(self, program):
        program.config.api.base_url = "https://test.unifieddatalibrary.com"
        assert (
            self.resolve(program)
            == "https://imagery-test.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_explicit_prod_base_url_uses_production_imagery(self, program):
        program.config.api.base_url = "https://unifieddatalibrary.com/"
        assert (
            self.resolve(program)
            == "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_other_host_carries_the_filedrop_route(self, program):
        """Custom UDL-compliant endpoints serve the route on their own host."""
        assert self.resolve(program) == f"{COMPLIANT_BASE_URL}/filedrop/udl-skyimagery"


class TestCompletedResponse:
    """COMPLETED is sent from the publisher once a collect's imagery set has been
    delivered. It fires on set completion *by count* (not on the final frame's
    upload succeeding), so a partially-delivered set still reports COMPLETED; it
    is suppressed only when nothing was delivered.
    """

    WINDOW = (
        datetime(2026, 3, 21, 7, 18, 47, tzinfo=UTC),
        datetime(2026, 3, 21, 7, 19, 12, tzinfo=UTC),
    )

    @classmethod
    def arm(cls, program, *, num_frames, upload_fails=None, stash_window=True):
        """Register a request and, optionally, the COLLECTED execution window.

        The window is what the task factory stashes on the success path, so leaving it out models
        a collect that never succeeded. `upload_fails` scripts per-upload outcomes.
        """
        request = tle_request(num_frames=num_frames)
        program.tasks[request.id] = request
        program._sky_imagery._upload.fail = upload_fails

        if stash_window:
            program.state.publish_progress[request.id] = _PublishProgress(window=cls.WINDOW)

        return request

    @staticmethod
    async def publish_frames(program, request, count):
        for frame_num in range(count):
            # Distinct per-frame filenames, as FileNameTemplate produces; the
            # frame consumer dedups repeated deliveries of the same file.
            context = frame_context(
                name=f"{request.id}_{frame_num}.fits",
                task_id=request.id,
                frame_num=frame_num,
                date_obs="2026-03-21T07:18:47.082000",
                exptime=4.0,
                image_width=2048,
                image_height=2048,
                bits_per_pixel=16,
            )
            await program._handle_frame(context, b"\x00" * 100)

    @pytest.mark.asyncio
    async def test_no_completed_before_set_finishes(self, program):
        request = self.arm(program, num_frames=3)
        await self.publish_frames(program, request, 2)  # only 2 of 3 frames in
        assert program.client.collect_responses.statuses() == []

    @pytest.mark.asyncio
    async def test_completed_after_full_set_carries_execution_window(self, program):
        request = self.arm(program, num_frames=2)
        await self.publish_frames(program, request, 2)

        (sent,) = program.client.collect_responses.created
        assert sent["status"] == ResponseStatus.COMPLETED.value
        assert sent["actual_start_time"] == "2026-03-21T07:18:47.000000Z"
        assert sent["actual_end_time"] == "2026-03-21T07:19:12.000000Z"

    @pytest.mark.asyncio
    async def test_completed_sent_when_final_frame_upload_fails(self, program):
        """4/5 deliver, the 5th upload fails → still COMPLETED."""
        fails = [None, None, None, None, RuntimeError("boom")]
        request = self.arm(program, num_frames=5, upload_fails=fails)
        await self.publish_frames(program, request, 5)

        assert program.client.collect_responses.statuses() == [ResponseStatus.COMPLETED.value]

    @pytest.mark.asyncio
    async def test_no_completed_when_all_uploads_fail(self, program):
        request = self.arm(
            program, num_frames=2, upload_fails=RuntimeError("boom"), stash_window=False
        )
        await self.publish_frames(program, request, 2)

        assert program.client.collect_responses.statuses() == []

    @pytest.mark.asyncio
    async def test_no_completed_without_collected_window(self, program):
        """No stashed window → the task didn't collect successfully → no COMPLETED."""
        request = self.arm(program, num_frames=1, stash_window=False)
        await self.publish_frames(program, request, 1)

        assert program.client.collect_responses.statuses() == []

    @pytest.mark.asyncio
    async def test_completed_sent_once_per_set(self, program):
        """A stray extra frame after the set finished must not re-send COMPLETED."""
        request = self.arm(program, num_frames=2)
        await self.publish_frames(program, request, 2)
        # A duplicate/late frame for the same task arrives after finalization.
        await self.publish_frames(program, request, 1)

        assert program.client.collect_responses.statuses() == [ResponseStatus.COMPLETED.value]

    @pytest.mark.asyncio
    async def test_completed_without_imagery_publisher(self, program):
        """With imagery publishing disabled, seeing the full set is completion."""
        request = self.arm(program, num_frames=2)
        program._sky_imagery = None
        await self.publish_frames(program, request, 2)

        assert program.client.collect_responses.statuses() == [ResponseStatus.COMPLETED.value]

    @pytest.mark.asyncio
    async def test_no_completed_without_imagery_until_set_seen(self, program):
        """Imagery disabled: COMPLETED still waits for the full set."""
        request = self.arm(program, num_frames=3)
        program._sky_imagery = None
        await self.publish_frames(program, request, 2)

        assert program.client.collect_responses.statuses() == []
