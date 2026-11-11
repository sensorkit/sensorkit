# SPDX-License-Identifier: Apache-2.0
"""Instrument selection in Burr configuration and collect tasks."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime

import pytest
from burr.core.config import AppConfig, PlottingConfig
from burr.models.observation import ExposureSpec, ObservationRequest, RADecTarget
from burr.models.run import BurrRun
from burr.models.site import SiteMetadata
from burr.models.tracking import Rates, TrackingMode
from pydantic import ValidationError

from sensorkit.astro.target import ICRSTarget, RateTarget
from sensorkit.burr.models import CameraSettings, CollectConfig
from sensorkit.burr.program import BurrConfig
from sensorkit.burr.tasks import BurrContext, build_sk_tasks
from sensorkit.config.parser import PARSER_VERSION, parse_config
from sensorkit.core.entity import InstrumentInfo
from sensorkit.core.task import TaskInfo
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.std import FrameType
from sensorkit.std.collect import Collect

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

SHARED = {"filter_name": "r", "gain": 100, "binning": 1}

MAPPED = {
    **SHARED,
    "instruments": {
        "cam-east": {},
        "cam-west": {"filter_name": "g", "gain": 200},
        " Cam West ": {"filter_name": None, "binning": None},
    },
}

# Expected instrument settings include inherited values and explicit null overrides.
CONFIGS = {
    "automatic": (SHARED, [(None, CameraSettings(filter_name="r", gain=100, binning=1))]),
    "named": (
        {**SHARED, "instrument": " cam east "},
        [(" cam east ", CameraSettings(filter_name="r", gain=100, binning=1))],
    ),
    "mapped": (
        MAPPED,
        [
            ("cam-east", CameraSettings(filter_name="r", gain=100, binning=1)),
            ("cam-west", CameraSettings(filter_name="g", gain=200, binning=1)),
            (" Cam West ", CameraSettings(gain=100)),
        ],
    ),
}

# Names cover case, separators, whitespace, and non-ASCII characters.
FILE_INSTRUMENTS = ["cam-east", "CAM-EAST", "cam/east", "cam\\east", " ", "cam-é"]
AUTOMATIC_INSTRUMENT = "assigned/cam"

SOURCES = ["twilight_flats", "lunar_background", "photometric_standards", "coverage", "calsats"]


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    return BurrRun.from_config(
        config=APP_CONFIG,
        observation_date=datetime(2026, 11, 10, tzinfo=UTC),
        run_id="test",
        output_directory=tmp_path_factory.mktemp("burr"),
    )


def observation(modes: list[TrackingMode], source: str = "coverage") -> ObservationRequest:
    return ObservationRequest(
        task_id="request-1",
        source_name=source,
        target=RADecTarget(right_ascension_deg=10.0, declination_deg=20.0),
        exposure_seconds=[1.0, 2.5],
        exposure_map=[ExposureSpec(tracking_mode=mode) for mode in modes],
        rates=Rates(right_ascension_rate_arcsec_s=36.0, declination_rate_arcsec_s=-72.0),
        metadata={"target_name": "field 7"},
    )


def requested(params) -> tuple[str | None, CameraSettings]:
    """Read the instrument selection and camera settings from a parameter set."""
    assert params.binning_x == params.binning_y

    return params.instrument, CameraSettings(
        filter_name=params.filter_name,
        readout_mode=params.readout_mode,
        gain=params.gain,
        binning=params.binning_x,
    )


def file_names(submissions) -> list[str]:
    """Resolve frame filenames with an assigned instrument for every sequence."""
    names = []

    for submission in submissions:
        info = TaskInfo(task=submission.task, task_id=uuid.uuid4(), controller_id="ctrl")

        for params in submission.task.exposures:
            instrument = InstrumentInfo(name=params.instrument or AUTOMATIC_INSTRUMENT)

            for frame in range(params.frame_count):
                collect = Collect(target=submission.task.target, params=params, frame_number=frame)
                context = Context(submission.context, info, collect, instrument)
                names.append(context.resolve(context[FileNameTemplate].template, as_type=str))

    return names


def test_collect_instrument_is_optional_and_exact():
    assert CollectConfig().instrument is None
    assert CollectConfig.model_validate({"instrument": None}).instrument is None
    assert CollectConfig(instrument=" cam east ").instrument == " cam east "
    assert CollectConfig.model_validate({"instruments": None}).instruments is None

    with pytest.raises(ValidationError, match="at least 1 character"):
        CollectConfig(instrument="")


def test_configured_instruments_keep_authored_order_and_exact_names():
    names = ["cam-west", " Cam West ", "cam-east", "CAM-EAST"]
    collect = CollectConfig.model_validate({"instruments": {name: {} for name in names}})

    assert [name for name, _ in collect.instrument_settings()] == names


@pytest.mark.parametrize(
    ("collect", "error"),
    [
        ({"instrument": "cam-east", "instruments": {"cam-west": {}}}, "not both"),
        ({"instruments": {}}, "at least 1 item"),
        ({"instruments": {"": {}}}, "at least 1 character"),
        ({"instruments": {"cam-east": {"gain": "high"}}}, "valid number"),
    ],
    ids=["both", "empty", "empty_name", "invalid_override"],
)
def test_invalid_instrument_configuration_is_rejected(collect, error):
    with pytest.raises(ValidationError, match=error):
        CollectConfig.model_validate(collect)


@pytest.mark.parametrize("config", CONFIGS)
def test_instrument_settings_survive_model_round_trips(config):
    authored, expected = CONFIGS[config]
    collect = CollectConfig.model_validate(authored)

    assert collect.instrument_settings() == expected
    assert CollectConfig.model_validate(collect.model_dump()).instrument_settings() == expected
    assert (
        CollectConfig.model_validate_json(collect.model_dump_json()).instrument_settings()
        == expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("config", CONFIGS)
async def test_instrument_settings_survive_a_kv_round_trip(kit, config):
    authored, expected = CONFIGS[config]
    section = {
        "id": "burr",
        "controller": "ctrl",
        "plotting": {"debug": False, "review": False},
        "collect": authored,
    }
    unified = {"version": PARSER_VERSION, "burr": [section]}

    (model,) = parse_config(unified).resolve_dynamic_sections().entity_kv["burr"]
    await kit.entity("burr").kv_put_model(model)
    loaded = await kit.program("burr").kv_get_model(BurrConfig)

    assert loaded.collect.instrument_settings() == expected


@pytest.mark.parametrize("pattern", PATTERNS)
@pytest.mark.parametrize("config", CONFIGS)
def test_every_task_requests_each_configured_instrument(run, pattern, config):
    modes, expected = PATTERNS[pattern]
    authored, instrument_settings = CONFIGS[config]
    collect = CollectConfig.model_validate(authored)

    submissions = list(build_sk_tasks(run, APP_CONFIG, observation(modes), collect=collect))

    assert len(submissions) == 2 * len(expected)

    for index, submission in enumerate(submissions):
        task = submission.task
        target_type, frame_count, sidereal_frames = expected[index % len(expected)]
        exposure = [1.0, 2.5][index // len(expected)]

        # A mapping emits a list; singular and automatic selections emit one parameter set.
        assert isinstance(task.camera_params, list) == (config == "mapped")
        assert [requested(params) for params in task.exposures] == instrument_settings
        assert {(p.integration_time_seconds, p.frame_count) for p in task.exposures} == {
            (exposure, frame_count)
        }
        assert type(task.target).__name__ == target_type
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


@pytest.mark.parametrize("config", CONFIGS)
def test_unsupported_tracking_pattern_is_rejected(run, config):
    collect = CollectConfig.model_validate(CONFIGS[config][0])
    request = observation([SIDEREAL, RATE, SIDEREAL])

    with pytest.raises(ValueError, match="unsupported exposure_map pattern"):
        list(build_sk_tasks(run, APP_CONFIG, request, collect=collect))


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("config", CONFIGS)
def test_every_instrument_uses_the_source_frame_type(run, source, config):
    collect = CollectConfig.model_validate(CONFIGS[config][0])
    expected = FrameType.FLAT if source == "twilight_flats" else FrameType.LIGHT

    submissions = list(
        build_sk_tasks(run, APP_CONFIG, observation([SIDEREAL, RATE], source), collect=collect)
    )

    assert {p.frame_type for s in submissions for p in s.task.exposures} == {expected}


def test_omitted_collect_selects_automatically(run):
    submissions = list(build_sk_tasks(run, APP_CONFIG, observation([SIDEREAL, RATE])))

    assert [submission.task.exposures[0].instrument for submission in submissions] == [None] * 4


@pytest.mark.parametrize(
    ("authored", "assigned"),
    [
        ({}, AUTOMATIC_INSTRUMENT),
        ({"instrument": "cam/east"}, "cam/east"),
        ({"instruments": {"cam/east": {}}}, "cam/east"),
    ],
    ids=["automatic", "named", "mapped"],
)
def test_every_selection_names_files_with_the_assigned_instrument(run, authored, assigned):
    collect = CollectConfig.model_validate(authored)
    request = observation([SIDEREAL, RATE, RATE])
    submissions = list(build_sk_tasks(run, APP_CONFIG, request, collect=collect))

    names = file_names(submissions)
    pattern = (
        rf"\d{{8}}T\d{{6}}_coverage_field_7_([0-9a-f-]{{36}})"
        rf"_{assigned.encode().hex()}_f(\d)\.fits"
    )
    frames = [re.fullmatch(pattern, name) for name in names]

    assert all(frames)
    assert [int(frame[2]) for frame in frames] == [0, 0, 1] * 2
    assert len({uuid.UUID(frame[1]) for frame in frames}) == len(submissions)


def test_configured_instruments_sharing_a_directory_get_distinct_file_names(run, tmp_path):
    collect = CollectConfig.model_validate({"instruments": {n: {} for n in FILE_INSTRUMENTS}})
    modes = [SIDEREAL, RATE, RATE]
    submissions = [
        submission
        for _ in range(2)
        for submission in build_sk_tasks(run, APP_CONFIG, observation(modes), collect=collect)
    ]

    names = file_names(submissions)

    # Each request has two exposures with three frames per instrument.
    assert len(names) == 2 * 2 * 3 * len(FILE_INSTRUMENTS)
    # Encoded names remain distinct on case-insensitive filesystems.
    assert len({name.casefold() for name in names}) == len(names)
    assert all(re.fullmatch(r"\d{8}T\d{6}_coverage_field_7_[^/\\]+_f\d\.fits", n) for n in names)

    for name in names:
        (tmp_path / name).touch()

    assert len(list(tmp_path.iterdir())) == len(names)
