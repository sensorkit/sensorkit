# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto's data models."""

from sensorkit.otto.models import (
    CollectConfig,
    TaskConfig,
)
from sensorkit.otto.program import OttoState, TLECache


class TestOttoState:
    def test_defaults(self):
        state = OttoState()
        assert state.whitelist == []
        assert state.graylist == []
        assert state.blacklist == []

    def test_serialization(self):
        state = OttoState(
            whitelist=["25544", "42738"],
            graylist=["39120"],
            blacklist=["12345"],
        )
        data = state.model_dump()
        restored = OttoState.model_validate(data)
        assert restored.whitelist == ["25544", "42738"]
        assert restored.graylist == ["39120"]
        assert restored.blacklist == ["12345"]


class TestTaskConfig:
    def test_defaults(self):
        config = TaskConfig(objects=["25544"])
        assert config.objects == ["25544"]
        assert config.tle_update_interval_hours == 24
        assert config.graylist_interval_minutes == 300
        assert config.end_time_deadband_seconds == 60

    def test_multiple_objects(self):
        config = TaskConfig(objects=["25544", "42738", "39120"])
        assert len(config.objects) == 3


class TestCollectConfig:
    def test_defaults(self):
        config = CollectConfig(track_mode="rate")
        assert config.altitude_min == 20.0
        assert config.dither is False
        assert config.num_frames == 3

    def test_custom(self):
        config = CollectConfig(
            track_mode="rate_sidereal",
            altitude_min=30.0,
            filters=["Lum", "R", "G", "B"],
            exposure_min=5,
            exposure_max=15,
            exposure_delta=5,
            binning=[1, 2],
            num_frames=5,
        )
        assert config.track_mode == "rate_sidereal"
        assert len(config.filters) == 4
        assert config.num_frames == 5


class TestTLECache:
    def test_serialization(self):
        from datetime import UTC, datetime

        cache = TLECache(
            tles={
                "25544": {
                    "line0": "0 25544",
                    "line1": "1 25544U ...",
                    "line2": "2 25544 ...",
                }
            },
            dt=datetime(2026, 4, 17, tzinfo=UTC),
        )
        data = cache.model_dump(mode="json")
        restored = TLECache.model_validate(data)
        assert "25544" in restored.tles
        assert restored.tles["25544"]["line0"] == "0 25544"
