# SPDX-License-Identifier: Apache-2.0
"""EOObservation building and publishing from senpai detections."""

import asyncio
import contextlib
import math
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockCollectRequest
from loguru import logger

from sensorkit.senpai.models import Detection, SenpaiResult
from sensorkit.udl.models import (
    EOObservationPublishConfig,
    PublishConfig,
    UDLAPIConfig,
    UDLConfig,
)
from sensorkit.udl.program import UDLProgram
from sensorkit.udl.publishers import (
    EOObservationPublisher,
    RequestSnapshot,
    _add_annual_aberration,
    build_eo_observations,
    convert_wcs_observation,
)

TIMESTAMP = datetime(2026, 3, 21, 7, 18, 47, 82000, tzinfo=UTC)


def make_detection(kind="point", ra=210.5, dec=-12.25, **overrides):
    fields = dict(kind=kind, x=100.0, y=200.0, snr=25.0, ra=ra, dec=dec)
    fields.update(overrides)
    return Detection(**fields)


def make_result(
    *,
    kinds=("point",),
    track_mode="RATE",
    task_id="test-request-001",
    solved=True,
    from_sequence=True,
    exposure=4.0,
    file_path="/data/raw/frame_0001.fits",
    detections=None,
):
    if detections is None:
        detections = [make_detection(kind=kind) for kind in kinds]
    return SenpaiResult(
        file_path=file_path,
        timestamp=TIMESTAMP,
        track_mode=track_mode,
        task_id=task_id,
        frame_num=0,
        frame_count=3,
        from_sequence=from_sequence,
        exposure_time_seconds=exposure,
        n_sources=len(detections),
        median_fwhm_pixels=None,
        std_fwhm_pixels=None,
        pixel_scale_arcsec=None,
        median_fwhm_arcsec=None,
        std_fwhm_arcsec=None,
        solved=solved,
        detections=detections,
    )


@pytest.fixture
def snapshot():
    return RequestSnapshot(
        request_id="test-request-001",
        classification_marking="U",
        data_mode="REAL",
        origin="TEST_ORG",
        task_id="udl-task-42",
    )


@pytest.fixture
def api_config():
    return UDLAPIConfig(id_sensor="SENSOR-01", source="TEST_SOURCE")


@pytest.fixture
def eo_config():
    # sequence_only off by default in tests; the gate has its own test.
    return EOObservationPublishConfig(sequence_only=False)


@pytest.fixture
def site():
    return SimpleNamespace(
        latitude_degrees=41.9168354,
        longitude_degrees=-84.0290721,
        altitude_km=0.05,
    )


def build(result, snapshot, api_config, eo_config, site=None):
    return build_eo_observations(
        result, snapshot, site=site, api=api_config, config=eo_config
    )


class TestConvertWcsObservation:
    """UDL EO conventions: photon-arrival obTime, annual aberration added back."""

    def test_ob_time_is_photon_arrival_mid_exposure(self):
        result = make_result(exposure=4.0)
        ob_time, _, _ = convert_wcs_observation(result, result.detections[0])
        assert ob_time == "2026-03-21T07:18:49.082000Z"

    def test_ob_time_without_exposure_is_frame_timestamp(self):
        result = make_result(exposure=None)
        ob_time, _, _ = convert_wcs_observation(result, result.detections[0])
        assert ob_time == "2026-03-21T07:18:47.082000Z"

    def test_position_carries_annual_aberration(self):
        # Expected values are astropy's aberration of (210.5, -12.25) at the
        # mid-exposure epoch; the tolerance is the model's known residual.
        result = make_result()
        _, ra, declination = convert_wcs_observation(result, result.detections[0])
        assert (ra, declination) != (210.5, -12.25)
        assert ra == pytest.approx(210.5046, abs=3e-4)
        assert declination == pytest.approx(-12.2516, abs=3e-4)


class TestAnnualAberration:
    """The correction reproduces astropy's full-ephemeris aberration to <0.5",
    the residual expected of a model that ignores Earth's orbital eccentricity.
    Expected values below were cross-checked against astropy, not self-generated.
    """

    @staticmethod
    def offset_arcsec(ra, dec, when):
        new_ra, new_dec = _add_annual_aberration(ra, dec, when)
        return math.hypot(
            (new_ra - ra) * math.cos(math.radians(dec)) * 3600.0,
            (new_dec - dec) * 3600.0,
        )

    def test_matches_reference_position(self):
        # astropy's aberration of (43.35, -13.42) on 2026-04-15 is
        # (43.34481, -13.42237); we agree to 0.06".
        when = datetime(2026, 4, 15, 6, tzinfo=UTC)
        ra, dec = _add_annual_aberration(43.35, -13.42, when)
        assert ra == pytest.approx(43.3448, abs=3e-4)
        assert dec == pytest.approx(-13.4224, abs=3e-4)

    def test_never_exceeds_aberration_constant(self):
        """Displacement is bounded by kappa = v/c = 20.5"."""
        for month in range(1, 13):
            when = datetime(2026, month, 1, tzinfo=UTC)
            for ra in range(0, 360, 45):
                for dec in (-80, -20, 0, 20, 80):
                    assert self.offset_arcsec(ra, dec, when) <= 20.6

    def test_reaches_aberration_constant_broadside(self):
        """Peaks near kappa where the line of sight is normal to Earth's velocity."""
        peak = max(
            self.offset_arcsec(ra, 0.0, datetime(2026, month, 1, tzinfo=UTC))
            for month in range(1, 13)
            for ra in range(0, 360, 5)
        )
        assert peak == pytest.approx(20.5, abs=0.2)

    def test_correction_reverses_over_half_a_year(self):
        """Earth's velocity reverses, so the correction does too."""
        january = _add_annual_aberration(180.0, 0.0, datetime(2026, 1, 4, tzinfo=UTC))
        july = _add_annual_aberration(180.0, 0.0, datetime(2026, 7, 4, tzinfo=UTC))
        assert (january[0] - 180.0) * (july[0] - 180.0) < 0


class TestBuildEOObservations:
    def test_required_fields_present(self, snapshot, api_config, eo_config):
        [record] = build(make_result(), snapshot, api_config, eo_config)
        assert record["classification_marking"] == "U"
        assert record["data_mode"] == "REAL"
        assert record["source"] == "TEST_SOURCE"
        assert "ob_time" in record

    def test_ob_time_is_mid_exposure(self, snapshot, api_config, eo_config):
        [record] = build(make_result(exposure=4.0), snapshot, api_config, eo_config)
        assert record["ob_time"] == "2026-03-21T07:18:49.082000Z"
        assert record["exp_duration"] == 4.0

    def test_ob_time_falls_back_to_timestamp(self, snapshot, api_config, eo_config):
        [record] = build(make_result(exposure=None), snapshot, api_config, eo_config)
        assert record["ob_time"] == "2026-03-21T07:18:47.082000Z"
        assert "exp_duration" not in record

    def test_rate_mode_posts_points_only(self, snapshot, api_config, eo_config):
        result = make_result(
            track_mode="RATE", kinds=("point", "streak", "streak_candidate")
        )
        records = build(result, snapshot, api_config, eo_config)
        assert len(records) == 1

    def test_sidereal_mode_posts_confirmed_streaks_only(
        self, snapshot, api_config, eo_config
    ):
        result = make_result(
            track_mode="SIDEREAL", kinds=("point", "streak", "streak_candidate")
        )
        records = build(result, snapshot, api_config, eo_config)
        assert len(records) == 1

    def test_unknown_track_mode_posts_nothing(self, snapshot, api_config, eo_config):
        assert build(make_result(track_mode="UNKNOWN"), snapshot, api_config, eo_config) == []

    def test_detection_without_radec_skipped(self, snapshot, api_config, eo_config):
        result = make_result(detections=[make_detection(ra=None, dec=None)])
        assert build(result, snapshot, api_config, eo_config) == []

    def test_records_are_raw_uncorrelated_tracks(self, snapshot, api_config, eo_config):
        [record] = build(make_result(), snapshot, api_config, eo_config)
        assert record["uct"] is True
        assert "sat_no" not in record
        assert "id_on_orbit" not in record

    def test_identity_and_provenance_fields(self, snapshot, api_config, eo_config):
        [record] = build(make_result(), snapshot, api_config, eo_config)
        # Position is the detection's, plus annual aberration (< 20.5");
        # TestConvertWcsObservation pins the correction itself.
        assert record["ra"] == pytest.approx(210.5, abs=0.006)
        assert record["declination"] == pytest.approx(-12.25, abs=0.006)
        assert record["reference_frame"] == "J2000"
        assert record["id_sensor"] == "SENSOR-01"
        assert record["orig_sensor_id"] == "SENSOR-01"
        assert record["track_id"] == "test-request-001"
        assert record["task_id"] == "udl-task-42"
        assert record["origin"] == "TEST_ORG"
        assert record["descriptor"] == "frame_0001.fits"

    def test_site_fields(self, snapshot, api_config, eo_config, site):
        [record] = build(make_result(), snapshot, api_config, eo_config, site=site)
        assert record["senlat"] == 41.9168354
        assert record["senlon"] == -84.0290721
        assert record["senalt"] == 0.05

    def test_no_site_fields_without_site(self, snapshot, api_config, eo_config):
        [record] = build(make_result(), snapshot, api_config, eo_config, site=None)
        assert "senlat" not in record

    def test_none_values_stripped(self, api_config, eo_config):
        snapshot = RequestSnapshot(
            request_id="r1",
            classification_marking="U",
            data_mode=None,
            origin=None,
            task_id=None,
        )
        [record] = build(make_result(), snapshot, api_config, eo_config)
        assert "origin" not in record
        assert record["data_mode"] == "TEST"  # fallback, as with SkyImagery

    def test_mag_band_priority(self, snapshot, api_config):
        config = EOObservationPublishConfig(
            sequence_only=False, mag_bands=["G", "V"]
        )
        det = make_detection(
            calibrated_magnitudes={"V": 12.34},
            magnitude_errs={"V": 0.05},
        )
        [record] = build(make_result(detections=[det]), snapshot, api_config, config)
        assert record["mag"] == 12.34
        assert record["mag_unc"] == 0.05

    def test_instrumental_mag_never_posted(self, snapshot, api_config, eo_config):
        det = make_detection(instrumental_mag=10.0, calibrated_magnitudes=None)
        [record] = build(make_result(detections=[det]), snapshot, api_config, eo_config)
        assert "mag" not in record


@pytest.fixture
def program():
    config = UDLConfig(
        controller="controller1",
        api=UDLAPIConfig(id_sensor="SENSOR-01", source="TEST_SOURCE"),
        publish=PublishConfig(
            eo_observation=EOObservationPublishConfig(sequence_only=False)
        ),
    )
    p = UDLProgram()
    p.config = config
    p.program = MagicMock()
    p.program.entity = "udl_program"
    p._site = None
    p.upload_client = MagicMock()
    p.upload_client.observations.eo_observations.create_bulk = AsyncMock()
    p._upload_headers = {}
    return p


@pytest.fixture
def eo(program):
    return EOObservationPublisher(program)


def create_bulk_mock(program):
    return program.upload_client.observations.eo_observations.create_bulk


class TestEOObservationPublisher:
    @pytest.mark.asyncio
    async def test_posts_records_for_tasked_solved_result(self, program, eo):
        request = MockCollectRequest.with_tle()
        eo.note_request(request)

        await eo._handle_result(make_result(task_id=request.id))

        create_bulk_mock(program).assert_awaited_once()
        body = create_bulk_mock(program).call_args.kwargs["body"]
        assert len(body) == 1
        assert body[0]["track_id"] == request.id

    @pytest.mark.asyncio
    async def test_batch_carries_all_detections_of_frame(self, program, eo):
        request = MockCollectRequest.with_tle()
        eo.note_request(request)

        result = make_result(
            task_id=request.id,
            detections=[make_detection(ra=1.0), make_detection(ra=2.0)],
        )
        await eo._handle_result(result)

        body = create_bulk_mock(program).call_args.kwargs["body"]
        assert len(body) == 2

    @pytest.mark.asyncio
    async def test_untasked_result_dropped(self, program, eo):
        await eo._handle_result(make_result(task_id=None))
        create_bulk_mock(program).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unsolved_result_dropped(self, program, eo):
        request = MockCollectRequest.with_tle()
        eo.note_request(request)
        await eo._handle_result(make_result(task_id=request.id, solved=False))
        create_bulk_mock(program).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sequence_only_gates_per_frame_results(self, program, eo):
        eo._config.sequence_only = True
        request = MockCollectRequest.with_tle()
        eo.note_request(request)

        await eo._handle_result(
            make_result(task_id=request.id, from_sequence=False, file_path="/a.fits")
        )
        create_bulk_mock(program).assert_not_awaited()

        await eo._handle_result(
            make_result(task_id=request.id, from_sequence=True, file_path="/b.fits")
        )
        create_bulk_mock(program).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unknown_task_dropped(self, program, eo):
        await eo._handle_result(make_result(task_id="never-seen"))
        create_bulk_mock(program).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_to_program_tasks(self, program, eo):
        request = MockCollectRequest.with_tle()
        program.tasks[request.id] = request  # no note_request call

        await eo._handle_result(make_result(task_id=request.id))
        create_bulk_mock(program).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_duplicate_result_posted_once(self, program, eo):
        request = MockCollectRequest.with_tle()
        eo.note_request(request)

        result = make_result(task_id=request.id)
        await eo._handle_result(result)
        await eo._handle_result(result)
        create_bulk_mock(program).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_intake_loop_filters_and_handles_senpai_results(self, program, eo):
        """The wildcard stream is post-filtered to SenpaiResult keywords."""
        request = MockCollectRequest.with_tle()
        eo.note_request(request)

        result = make_result(task_id=request.id)
        messages = [
            SimpleNamespace(  # some other entity keyword — must be skipped
                subject=SimpleNamespace(prop="AltAzPointing", path=("mount",)),
                data=b'{"altitude_degrees": 45.0}',
            ),
            SimpleNamespace(
                subject=SimpleNamespace(prop="SenpaiResult", path=("senpai",)),
                data=result.model_dump_json().encode(),
            ),
        ]

        async def consumer():
            for msg in messages:
                yield msg

        kit = program.program.sensorkit.return_value
        # First connect yields the messages; the reconnect attempt cancels out.
        kit.backend.impl.stream_consume = AsyncMock(
            side_effect=[consumer(), asyncio.CancelledError()]
        )

        with contextlib.suppress(asyncio.CancelledError):
            await eo._intake_loop()

        create_bulk_mock(program).assert_awaited_once()
        durable = kit.backend.impl.stream_consume.call_args_list[0].kwargs["durable_name"]
        assert durable == "udl-eo-udl_program"


class TestSenpaiPresenceWarning:
    @staticmethod
    def _kv_entries(program, props):
        entries = [SimpleNamespace(key=SimpleNamespace(prop=prop)) for prop in props]
        kit = program.program.sensorkit.return_value
        kit.backend.key_value.return_value.get_all = AsyncMock(return_value=entries)

    @pytest.mark.asyncio
    async def test_warns_when_senpai_not_configured(self, program, eo):
        self._kv_entries(program, ["UDLConfig", "DataGraph"])

        messages = []
        handler = logger.add(lambda m: messages.append(str(m)), level="WARNING")
        try:
            await eo._warn_if_senpai_missing()
        finally:
            logger.remove(handler)

        assert any("senpai" in m for m in messages)

    @pytest.mark.asyncio
    async def test_silent_when_senpai_configured(self, program, eo):
        self._kv_entries(program, ["UDLConfig", "SenpaiConfig"])

        messages = []
        handler = logger.add(lambda m: messages.append(str(m)), level="WARNING")
        try:
            await eo._warn_if_senpai_missing()
        finally:
            logger.remove(handler)

        assert not messages
