import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

ISS_TLE = TLE(
    line0="0 25544",
    line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
    line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
)


def make_task(
    task_id=None,
    end_time=None,
    integration_time=10.0,
    frame_count=3,
    filter_name=None,
    binning=1,
    target=None,
):
    """Create a StandardCollectTask with sensible defaults."""
    return StandardCollectTask(
        task_id=task_id or uuid.uuid1(),
        controller_id="controller1",
        target=target or TLETarget(tle=ISS_TLE),
        end_time=end_time or (datetime.now(UTC) + timedelta(minutes=10)),
        camera_params=CameraParameterSet(
            integration_time_seconds=integration_time,
            frame_count=frame_count,
            filter_name=filter_name,
            binning_x=binning,
            binning_y=binning,
        ),
    )


@pytest.fixture
def mock_program_binding():
    """Mock for the program binding used by TaskQueue."""
    mock = MagicMock()
    mock.clear_offers = MagicMock()
    mock.add_offer = MagicMock()
    mock.publish_offers = AsyncMock()
    return mock
