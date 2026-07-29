# SPDX-License-Identifier: Apache-2.0
"""Shared test data for the Otto suite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.otto.models import CollectConfig, OttoConfig, PublishConfig, TaskConfig
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

ISS_TLE = TLE(
    line0="0 25544",
    line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
    line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
)


def make_config(**overrides) -> OttoConfig:
    """Create an OttoConfig with sensible defaults.

    `TaskConfig` requires at least one target source, so the default names a satellite; tests
    that task a different source override `task` wholesale or edit the returned config.
    """
    defaults = dict(
        controller="testcontroller",
        task=TaskConfig(tles=["25544"]),
        collect=CollectConfig(track_mode="rate"),
        publish=PublishConfig(),
    )
    return OttoConfig(**(defaults | overrides))


def make_task(
    end_time=None,
    integration_time=10.0,
    frame_count=3,
    filter_name=None,
    binning=1,
    target=None,
) -> StandardCollectTask:
    """Create a StandardCollectTask with sensible defaults."""
    return StandardCollectTask(
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
