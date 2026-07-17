# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto's data models."""

import pytest
from pydantic import ValidationError

from sensorkit.otto.models import (
    CollectConfig,
    PublishConfig,
    TaskConfig,
    UDLPublishConfig,
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

    def test_orbits_only(self):
        config = TaskConfig(orbits=["GEO", "HEO"])
        assert config.objects == []
        assert config.orbits == ["GEO", "HEO"]

    def test_objects_and_orbits(self):
        config = TaskConfig(objects=["25544"], orbits=["GEO"])
        assert config.objects == ["25544"]
        assert config.orbits == ["GEO"]

    def test_requires_objects_or_orbits(self):
        with pytest.raises(ValidationError):
            TaskConfig()

    def test_invalid_orbit_rejected(self):
        with pytest.raises(ValidationError):
            TaskConfig(orbits=["XEO"])


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

    def test_sidereal_track_mode(self):
        config = CollectConfig(track_mode="sidereal")
        assert config.track_mode == "sidereal"

    def test_invalid_track_mode_rejected(self):
        with pytest.raises(ValidationError):
            CollectConfig(track_mode="ratee")


class TestPublishConfig:
    def test_defaults(self):
        config = PublishConfig()
        assert config.upload is False
        assert config.env_file == ".env"
        assert config.gdrive is None
        assert config.dropbox is None
        assert config.udl is None

    def test_udl_requires_id_sensor_and_source(self):
        with pytest.raises(ValidationError):
            UDLPublishConfig(source="DAO")
        with pytest.raises(ValidationError):
            UDLPublishConfig(id_sensor="SENSOR-01")


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
