"""Tests for translating burr ObservationRequests into SK collect tasks."""

from datetime import UTC, datetime
from types import SimpleNamespace

import burr.models.observation as obs
from burr.models.tracking import Rates, TrackingMode

from sensorkit.burr.tasks import BurrContext, build_sk_tasks
from sensorkit.data.fits import FITSHeader


def _run(end_time: datetime | None = None):
    """Minimal stand-in for BurrRun: build_sk_tasks only reads schedule_end_utc."""
    return SimpleNamespace(
        lighting_schedule=SimpleNamespace(
            schedule_end_utc=end_time or datetime(2026, 7, 6, 12, 0, tzinfo=UTC),
        ),
    )


def _config():
    """Minimal stand-in for AppConfig: RADec paths never touch site."""
    return SimpleNamespace(site=None, sk=SimpleNamespace(rate_target_frame="icrf"))


def _sidereal_request(exposure_seconds: list[float]) -> obs.ObservationRequest:
    return obs.ObservationRequest(
        task_id="task-1",
        source_name="photometric_standards",
        target=obs.RADecTarget(right_ascension_deg=280.0, declination_deg=0.5),
        exposure_seconds=exposure_seconds,
        exposure_map=[
            obs.ExposureSpec(tracking_mode=TrackingMode.SIDEREAL),
            obs.ExposureSpec(tracking_mode=TrackingMode.SIDEREAL),
        ],
        metadata={"star_name": "SA 110-364"},
    )


class TestContextStampsFITSCards:
    def test_cards_match_burr_context(self):
        """Each submission carries a FITSHeader whose BURRSEQ/BURRTARG/BURRMODE
        mirror the BurrContext, so array_to_fits writes them into every frame
        without camera-graph header config."""
        submissions = list(build_sk_tasks(_run(), _config(), _sidereal_request([1.0])))
        assert len(submissions) == 1

        context = submissions[0].context
        header = context.get(FITSHeader)
        burr_context = context.get(BurrContext)
        assert header is not None
        assert header["BURRSEQ"] == burr_context.seq
        assert header["BURRTARG"] == "SA_110-364"  # star_name, filename-sanitized
        assert header["BURRMODE"] == "photometric_standards"

    def test_distinct_exposures_get_distinct_seq(self):
        """Each entry in exposure_seconds is its own collect: fresh BURRSEQ."""
        submissions = list(build_sk_tasks(_run(), _config(), _sidereal_request([1.0, 2.0])))
        assert len(submissions) == 2

        seqs = [s.context.get(FITSHeader)["BURRSEQ"] for s in submissions]
        assert seqs[0] != seqs[1]

    def test_fanout_tasks_share_seq(self):
        """A sidereal-then-rate exposure fans out into two SK tasks that share
        one BURRSEQ, so downstream batching regroups the collect's frames."""
        request = obs.ObservationRequest(
            task_id="task-2",
            source_name="calsats",
            target=obs.RADecTarget(right_ascension_deg=10.0, declination_deg=20.0),
            exposure_seconds=[1.0],
            exposure_map=[
                obs.ExposureSpec(tracking_mode=TrackingMode.SIDEREAL),
                obs.ExposureSpec(tracking_mode=TrackingMode.RATE),
            ],
            rates=Rates(right_ascension_rate_arcsec_s=15.0, declination_rate_arcsec_s=5.0),
            metadata={"satellite_name": "FAKESAT 1"},
        )

        submissions = list(build_sk_tasks(_run(), _config(), request))
        assert len(submissions) == 2

        headers = [s.context.get(FITSHeader) for s in submissions]
        assert headers[0] is not None
        assert headers[0]["BURRSEQ"] == headers[1]["BURRSEQ"]
        assert all(h["BURRTARG"] == "FAKESAT_1" for h in headers)
        assert all(h["BURRMODE"] == "calsats" for h in headers)
