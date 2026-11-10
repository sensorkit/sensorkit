# SPDX-License-Identifier: Apache-2.0
"""Instrument selection in Burr configuration and collect tasks."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from burr.core.config import AppConfig, PlottingConfig
from burr.models.observation import ExposureSpec, ObservationRequest, RADecTarget
from burr.models.run import BurrRun
from burr.models.site import SiteMetadata
from burr.models.tracking import Rates, TrackingMode
from pydantic import ValidationError

from sensorkit.astro.target import ICRSTarget, RateTarget
from sensorkit.burr.models import CollectConfig
from sensorkit.burr.tasks import BurrContext, build_sk_tasks

OFF = TrackingMode.OFF
RATE = TrackingMode.RATE
SIDEREAL = TrackingMode.SIDEREAL

APP_CONFIG = AppConfig(
    stage="test",
    plotting=PlottingConfig(debug=False, review=False),
    site=SiteMetadata(
        name="ctrl", latitude=32.0, longitude=-110.0, altitude_km=0.7, minimum_altitude_degrees=0.0
    ),
)

# Expected tasks per exposure list the target type, frame count, and sidereal frame indices.
PATTERNS = {
    "sidereal": ([SIDEREAL] * 3, [("ICRSTarget", 3, [])]),
    "off": ([OFF] * 2, [("ICRSTarget", 2, [])]),
    "rate": ([RATE] * 2, [("RateTarget", 2, [])]),
    "rate_then_sidereal": ([RATE, SIDEREAL, SIDEREAL], [("RateTarget", 3, [1, 2])]),
    "sidereal_then_rate": ([SIDEREAL, RATE, RATE], [("ICRSTarget", 1, []), ("RateTarget", 2, [])]),
}


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    return BurrRun.from_config(
        config=APP_CONFIG,
        observation_date=datetime(2026, 11, 10, tzinfo=UTC),
        run_id="test",
        output_directory=tmp_path_factory.mktemp("burr"),
    )


def observation(modes: list[TrackingMode]) -> ObservationRequest:
    return ObservationRequest(
        task_id="request-1",
        source_name="coverage",
        target=RADecTarget(right_ascension_deg=10.0, declination_deg=20.0),
        exposure_seconds=[1.0, 2.5],
        exposure_map=[ExposureSpec(tracking_mode=mode) for mode in modes],
        rates=Rates(right_ascension_rate_arcsec_s=36.0, declination_rate_arcsec_s=-72.0),
        metadata={"target_name": "field 7"},
    )


def test_collect_instrument_is_optional_and_exact():
    assert CollectConfig().instrument is None
    assert CollectConfig.model_validate({"instrument": None}).instrument is None
    assert CollectConfig(instrument=" cam east ").instrument == " cam east "

    with pytest.raises(ValidationError, match="at least 1 character"):
        CollectConfig(instrument="")


@pytest.mark.parametrize("pattern", PATTERNS)
@pytest.mark.parametrize("instrument", [None, " cam east "])
def test_every_task_carries_the_configured_instrument(run, pattern, instrument):
    modes, expected = PATTERNS[pattern]
    collect = CollectConfig(instrument=instrument, filter_name="r", gain=2.0, binning=2)

    submissions = list(build_sk_tasks(run, APP_CONFIG, observation(modes), collect=collect))

    assert len(submissions) == 2 * len(expected)

    for index, submission in enumerate(submissions):
        task = submission.task
        (params,) = task.exposures
        target_type, frame_count, sidereal_frames = expected[index % len(expected)]

        assert params.instrument == instrument
        assert (params.filter_name, params.gain, params.binning_x, params.binning_y) == (
            "r",
            2.0,
            2,
            2,
        )
        assert params.integration_time_seconds == [1.0, 2.5][index // len(expected)]
        assert (type(task.target).__name__, params.frame_count) == (target_type, frame_count)
        assert task.sidereal_frames == sidereal_frames
        assert task.end_time == run.lighting_schedule.schedule_end_utc
        assert submission.expiry_time == run.lighting_schedule.schedule_end_utc

        match task.target:
            case ICRSTarget():
                assert (task.target.coords.ra, task.target.coords.dec) == (10.0, 20.0)
            case RateTarget():
                assert (task.target.rates.ra, task.target.rates.dec) == (0.01, -0.02)
                assert (task.target.initial_coords.ra, task.target.initial_coords.dec) == (
                    10.0,
                    20.0,
                )

    contexts = [submission.context.get(BurrContext) for submission in submissions]

    assert {(context.targ, context.mode) for context in contexts} == {("field_7", "coverage")}
    # One sequence identifies all tasks from an exposure.
    assert len({context.seq for context in contexts}) == 2
    assert len({context.seq for context in contexts[: len(expected)]}) == 1


def test_omitted_collect_selects_automatically(run):
    submissions = list(build_sk_tasks(run, APP_CONFIG, observation([SIDEREAL, RATE])))

    assert [submission.task.exposures[0].instrument for submission in submissions] == [None] * 4
