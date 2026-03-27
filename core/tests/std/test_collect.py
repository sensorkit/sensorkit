import uuid
from datetime import datetime, timedelta, timezone

from sensorkit.astro.target import TLE, TLETarget
from sensorkit.std.collect import CameraParameterSet, FrameType, StandardCollectTask


def test_collect_task():
    StandardCollectTask(
        task_id=uuid.uuid1(),
        controller_id="sample_id_123",
        target=TLETarget(
            tle=TLE(
                line0="0 SPACE STATION",
                line1="1 25544U 98067A   25015.39697524  .00024300  00000-0  43163-3 0  9997",
                line2="2 25544  51.6408 343.8792 0001934 100.3261   3.0329 15.50054085491466",
            ),
        ),
        camera_params=CameraParameterSet(
            integration_time_seconds=10,
            frame_count=12,
            binning_x=2,
            binning_y=2,
            gain=1.0,
            frame_type=FrameType.LIGHT,
        ),
        end_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )
