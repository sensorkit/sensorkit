"""Test SkyImagery metadata generation against the UDL schema."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockCollectRequest

from sensorkit.udl.models import ResponseStatus

from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.udl.models import UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram, _PublishProgress


def _frame_context(name="test.fits", **fields):
    """Build a DataGraph-style context carrying a FileInfo keyword."""
    context = Context(fields)
    context.set(FileInfo(path=name))
    return context


@pytest.fixture
def program():
    config = UDLConfig(
        entity="udl_program",
        controller="controller1",
        api=UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        ),
    )
    p = UDLProgram()
    p.config = config
    p.program = MagicMock()
    p.program.entity = "udl_program"
    # Mock site position
    p._site = MagicMock()
    p._site.latitude_degrees = 41.9168354
    p._site.longitude_degrees = -84.0290721
    p._site.altitude_km = 0.05
    return p


class TestSkyImageryMetadata:
    @pytest.mark.asyncio
    async def test_metadata_has_required_fields(self, program):
        """SkyImagery_Ingest requires: classificationMarking, imageType, expStartTime, source, dataMode."""
        request = MockCollectRequest.with_tle(
            classification_marking="U",
            data_mode="REAL",
            source="TEST_SOURCE",
        )
        program.tasks["test-request-001"] = request

        # Mock the SDK upload
        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
            exptime=4.0,
            image_width=8120,
            image_height=8120,
            bits_per_pixel=16,
        )
        data = b"\x00" * 100

        await program._publish_imagery(context, data)

        # Extract the metadata from the ZIP that was uploaded
        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            assert len(metadata_files) == 1
            metadata = json.loads(zf.read(metadata_files[0]))

        # Required fields per UDL schema
        assert "classificationMarking" in metadata
        assert "imageType" in metadata
        assert "expStartTime" in metadata
        assert "source" in metadata
        assert "dataMode" in metadata

    @pytest.mark.asyncio
    async def test_metadata_uses_config_source(self, program):
        """source should come from config, not hardcoded."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["source"] == "TEST_SOURCE"

    @pytest.mark.asyncio
    async def test_metadata_uses_config_id_sensor(self, program):
        """idSensor should come from config."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["idSensor"] == "SENSOR-01"

    @pytest.mark.asyncio
    async def test_metadata_no_orig_sensor_id(self, program):
        """origSensorId should NOT be in SkyImagery metadata."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert "origSensorId" not in metadata

    @pytest.mark.asyncio
    async def test_metadata_no_orig_object_id(self, program):
        """origObjectId should NOT be in SkyImagery metadata (satNo is sufficient)."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert "origObjectId" not in metadata

    @pytest.mark.asyncio
    async def test_sequence_id_starts_at_one(self, program):
        """sequenceId must be >= 1 per UDL feedback."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,  # 0-indexed from SensorKit
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["sequenceId"] == 1

    @pytest.mark.asyncio
    async def test_metadata_includes_sensor_location(self, program):
        """senlat/senlon/senalt should be present when site position is available."""
        request = MockCollectRequest.with_tle()
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["senlat"] == 41.9168354
        assert metadata["senlon"] == -84.0290721
        assert metadata["senalt"] == 0.05

    @pytest.mark.asyncio
    async def test_metadata_matches_example(self, program):
        """Verify metadata structure matches the known-good example."""
        request = MockCollectRequest.with_tle(
            classification_marking="U",
            sat_no=39120,
            data_mode="REAL",
        )
        program.tasks["test-request-001"] = request
        program.config.api.id_sensor = "SENSOR-01"
        program.config.api.source = "TEST_SOURCE"

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            name="3869a317-24f6-11f1-b697-3a7c7693f667.fits",
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
            exptime=5.344074,
            image_width=8120,
            image_height=8120,
            bits_per_pixel=16,
        )

        await program._publish_imagery(context, b"\x00" * 527480640)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        # Match the example JSON structure
        assert metadata["classificationMarking"] == "U"
        assert metadata["idSensor"] == "SENSOR-01"
        assert metadata["satNo"] == 39120
        assert metadata["senlat"] == 41.9168354
        assert metadata["senlon"] == -84.0290721
        assert metadata["senalt"] == 0.05
        assert metadata["imageSetLength"] == 3
        assert metadata["sequenceId"] == 1
        assert metadata["frameWidthPixels"] == 8120
        assert metadata["frameHeightPixels"] == 8120
        assert metadata["source"] == "TEST_SOURCE"
        assert metadata["dataMode"] == "REAL"
        assert metadata["imageType"] == "FITS"
        # Multi-frame set (num_frames=3) → imageSetId associates the frames.
        assert metadata["imageSetId"] == "test-request-001"
        assert "origSensorId" not in metadata
        assert "origObjectId" not in metadata

    @pytest.mark.asyncio
    async def test_imageset_id_omitted_for_single_image(self, program):
        """Per UDL: a single-image set (num_frames=1) doesn't need an imageSetId."""
        request = MockCollectRequest.with_tle(num_frames=1)
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["imageSetLength"] == 1
        assert "imageSetId" not in metadata

    @pytest.mark.asyncio
    async def test_imageset_id_present_for_multi_image(self, program):
        """A multi-frame set keeps imageSetId so the frames can be grouped."""
        request = MockCollectRequest.with_tle(num_frames=5)
        program.tasks["test-request-001"] = request

        program.client = MagicMock()
        program._upload_skyimagery_zip = AsyncMock()

        context = _frame_context(
            task_id="test-request-001",
            frame_num=0,
            date_obs="2026-03-21T07:18:47.082000",
        )

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program._upload_skyimagery_zip.call_args
        zip_bytes = call_args.args[0] if call_args.args else call_args.kwargs.get("zip_bytes")

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            metadata_files = [n for n in zf.namelist() if n.endswith("_skyimagery.json")]
            metadata = json.loads(zf.read(metadata_files[0]))

        assert metadata["imageSetLength"] == 5
        assert metadata["imageSetId"] == "test-request-001"


class TestImageryFiledropURL:
    """The SDK's upload_zip() targets the wrong host when base_url is
    overridden; _imagery_filedrop_url() derives the dedicated imagery subdomain.
    """

    def test_default_base_url_uses_production_imagery(self, program):
        program.config.api.base_url = None
        assert (
            program._imagery_filedrop_url()
            == "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_test_base_url_uses_test_imagery(self, program):
        program.config.api.base_url = "https://test.unifieddatalibrary.com"
        assert (
            program._imagery_filedrop_url()
            == "https://imagery-test.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_explicit_prod_base_url_uses_production_imagery(self, program):
        program.config.api.base_url = "https://unifieddatalibrary.com/"
        assert (
            program._imagery_filedrop_url()
            == "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        )

    def test_unknown_host_returns_none(self, program):
        """Unknown hosts (e.g. MACHINA) → fall back to the SDK."""
        program.config.api.base_url = "https://machina.example.mil"
        assert program._imagery_filedrop_url() is None


class TestCompletedResponse:
    """COMPLETED is sent from the publisher once a collect's imagery set has been
    delivered. It fires on set completion *by count* (not on the final frame's
    upload succeeding), so a partially-delivered set still reports COMPLETED; it
    is suppressed only when nothing was delivered.
    """

    TASK_ID = "test-request-001"
    WINDOW = (
        datetime(2026, 3, 21, 7, 18, 47, tzinfo=UTC),
        datetime(2026, 3, 21, 7, 19, 12, tzinfo=UTC),
    )

    @classmethod
    def _arm(cls, program, *, num_frames, upload=None, stash_window=True):
        """Wire a request, mocked client/upload, and (optionally) the COLLECTED
        execution window so the publisher can finalize the set."""
        request = MockCollectRequest.with_tle(id=cls.TASK_ID, num_frames=num_frames)
        program.tasks[cls.TASK_ID] = request
        program.client = MagicMock()
        program.client.collect_responses.create = AsyncMock()
        program._upload_skyimagery_zip = upload or AsyncMock()
        if stash_window:
            program._publish_progress[cls.TASK_ID] = _PublishProgress(window=cls.WINDOW)
        return request

    @classmethod
    async def _publish_frames(cls, program, num_frames):
        for frame_num in range(num_frames):
            context = _frame_context(
                task_id=cls.TASK_ID,
                frame_num=frame_num,
                date_obs="2026-03-21T07:18:47.082000",
            )
            await program._publish_imagery(context, b"\x00" * 100)

    @pytest.mark.asyncio
    async def test_no_completed_before_set_finishes(self, program):
        self._arm(program, num_frames=3)
        await self._publish_frames(program, 2)  # only 2 of 3 frames in
        program.client.collect_responses.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_completed_after_full_set_carries_execution_window(self, program):
        self._arm(program, num_frames=2)
        await self._publish_frames(program, 2)

        kwargs = program.client.collect_responses.create.call_args.kwargs
        assert kwargs["status"] == ResponseStatus.COMPLETED.value
        assert kwargs["actual_start_time"] == "2026-03-21T07:18:47.000000Z"
        assert kwargs["actual_end_time"] == "2026-03-21T07:19:12.000000Z"

    @pytest.mark.asyncio
    async def test_completed_sent_when_final_frame_upload_fails(self, program):
        """4/5 deliver, the 5th upload fails → still COMPLETED (the case RS raised)."""
        upload = AsyncMock(side_effect=[None, None, None, None, RuntimeError("boom")])
        self._arm(program, num_frames=5, upload=upload)
        await self._publish_frames(program, 5)

        kwargs = program.client.collect_responses.create.call_args.kwargs
        assert kwargs["status"] == ResponseStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_no_completed_when_all_uploads_fail(self, program):
        upload = AsyncMock(side_effect=RuntimeError("boom"))
        self._arm(program, num_frames=2, upload=upload, stash_window=False)
        await self._publish_frames(program, 2)
        program.client.collect_responses.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_completed_without_collected_window(self, program):
        """No stashed window → the task didn't collect successfully → no COMPLETED."""
        self._arm(program, num_frames=1, stash_window=False)
        await self._publish_frames(program, 1)
        program.client.collect_responses.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_completed_sent_once_per_set(self, program):
        """A stray extra frame after the set finished must not re-send COMPLETED."""
        self._arm(program, num_frames=2)
        await self._publish_frames(program, 2)
        # A duplicate/late frame for the same task arrives after finalization.
        await self._publish_frames(program, 1)
        assert program.client.collect_responses.create.call_count == 1
