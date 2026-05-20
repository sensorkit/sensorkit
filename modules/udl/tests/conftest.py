from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_sk_program():
    """Mock sk.program() so program methods work without a real service."""
    mock_program = MagicMock()
    mock_program.kv_put_model = AsyncMock()
    mock_program.kv_get_model = AsyncMock(side_effect=Exception("no saved state"))
    mock_program.entity = "udl_program"
    mock_program.data_graph = AsyncMock(return_value=None)
    mock_program.clear_offers = MagicMock()
    mock_program.add_offer = MagicMock()
    mock_program.publish_offers = AsyncMock()
    mock_program.task_group = MagicMock()
    mock_program.task_group.create_task = MagicMock()

    mock_sensorkit = MagicMock()
    mock_controller = MagicMock()
    mock_controller.kv_get_model = AsyncMock(return_value=MagicMock(
        latitude_degrees=32.0,
        longitude_degrees=-110.0,
        altitude_km=0.7,
    ))
    mock_sensorkit.controller.return_value = mock_controller
    mock_program.sensorkit.return_value = mock_sensorkit

    with patch("sensorkit.api.program", return_value=mock_program):
        yield mock_program


class MockCollectRequest:
    """Factory for creating mock CollectRequestFull-like objects."""

    @staticmethod
    def create(
        id="test-request-001",
        classification_marking="U",
        data_mode="TEST",
        source="TEST_SOURCE",
        id_sensor="SENSOR-01",
        orig_sensor_id="SENSOR-01",
        sat_no=25544,
        orig_object_id="25544",
        start_time=None,
        end_time=None,
        integration_time=4000,  # milliseconds
        num_frames=3,
        task_id="task-001",
        id_plan=None,
        external_id=None,
        elset=None,
        state_vector=None,
        ra=None,
        dec=None,
    ):
        from datetime import UTC, datetime, timedelta

        if start_time is None:
            start_time = datetime.now(UTC)
        if end_time is None:
            end_time = start_time + timedelta(minutes=10)

        mock = MagicMock()
        mock.id = id
        mock.classification_marking = classification_marking
        mock.data_mode = data_mode
        mock.source = source
        mock.id_sensor = id_sensor
        mock.orig_sensor_id = orig_sensor_id
        mock.sat_no = sat_no
        mock.orig_object_id = orig_object_id
        mock.start_time = start_time
        mock.end_time = end_time
        mock.integration_time = integration_time
        mock.num_frames = num_frames
        mock.task_id = task_id
        mock.id_plan = id_plan
        mock.external_id = external_id
        mock.elset = elset
        mock.state_vector = state_vector
        mock.ra = ra
        mock.dec = dec
        mock.model_dump = MagicMock(return_value={"id": id})
        return mock

    @staticmethod
    def with_tle(sat_no=25544, **kwargs):
        elset = MagicMock()
        elset.sat_no = sat_no
        elset.line1 = "1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002"
        elset.line2 = "2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000"
        return MockCollectRequest.create(sat_no=sat_no, elset=elset, **kwargs)

    @staticmethod
    def with_state_vector(**kwargs):
        sv = MagicMock()
        sv.xpos = 6778.0  # km
        sv.ypos = 0.0
        sv.zpos = 0.0
        sv.xvel = 0.0  # km/s
        sv.yvel = 7.5
        sv.zvel = 0.0
        sv.epoch = None
        sv.reference_frame = "J2000"
        return MockCollectRequest.create(state_vector=sv, **kwargs)

    @staticmethod
    def with_radec(ra=180.0, dec=45.0, **kwargs):
        return MockCollectRequest.create(ra=ra, dec=dec, **kwargs)
