"""Test SkyImagery metadata generation against the UDL schema."""

import json
import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from sensorkit.udl.models import UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram

from conftest import MockCollectRequest


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
    p = UDLProgram(config)
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "etime": 4.0,
            "image_width": 8120,
            "image_height": 8120,
            "bits_per_pixel": 16,
            "file_name": "test.fits",
        }
        data = b"\x00" * 100

        await program._publish_imagery(context, data)

        # Extract the metadata from the ZIP that was uploaded
        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,  # 0-indexed from SensorKit
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "file_name": "test.fits",
        }

        await program._publish_imagery(context, b"\x00" * 100)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        program.client.sky_imagery = MagicMock()
        program.client.sky_imagery.upload_zip = AsyncMock()

        context = {
            "task_id": "test-request-001",
            "frame_num": 0,
            "date_obs": "2026-03-21T07:18:47.082000",
            "etime": 5.344074,
            "image_width": 8120,
            "image_height": 8120,
            "bits_per_pixel": 16,
            "file_name": "3869a317-24f6-11f1-b697-3a7c7693f667.fits",
        }

        await program._publish_imagery(context, b"\x00" * 527480640)

        call_args = program.client.sky_imagery.upload_zip.call_args
        zip_bytes = call_args.kwargs.get("file") or call_args.args[0]

        import io, zipfile
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
        assert "origSensorId" not in metadata
        assert "origObjectId" not in metadata
