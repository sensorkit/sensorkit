# SPDX-License-Identifier: Apache-2.0
"""SenpaiResult extraction from SENPAI collect runs."""

from types import SimpleNamespace

import sensorkit.senpai.pipeline as pipeline_mod
from sensorkit.senpai.pipeline import FrameInput, SenpaiPipeline


def make_image(file_path="frame.fits", **header):
    header.setdefault("DATE-OBS", "2026-07-09T01:02:03")
    return SimpleNamespace(header=header, file_path=file_path)


def make_det(kind="streak", **overrides):
    fields = dict(
        x=10.0,
        y=20.0,
        snr=30.0,
        pixel_fwhm=3.0,
        ra=210.0,
        dec=-12.0,
        angle_deg=45.0,
        length_pixels=25.0,
        rate_pixels_per_sec=5.0,
        rate_arcsec_per_sec=6.0,
        flux=1000.0,
        flux_err=10.0,
        instrumental_magnitude=11.0,
        calibrated_magnitudes={"G": 12.0},
        magnitude_errs={"G": 0.1},
        detection_type=kind,
    )
    fields.update(overrides)
    return SimpleNamespace(**fields)


def make_candidate():
    return SimpleNamespace(
        x=50.0,
        y=60.0,
        peak_snr=8.0,
        angle_deg=90.0,
        length_pixels=12.0,
        width_pixels=2.0,
        ra=211.0,
        dec=-13.0,
        rate_pixels_per_sec=None,
        rate_arcsec_per_sec=None,
        flux=None,
        flux_err=None,
        instrumental_magnitude=None,
        calibrated_magnitudes=None,
        magnitude_errs=None,
    )


def make_frame(image, *, detections=(), streak_candidates=(), fit=True, exposure=2.5):
    starfield = SimpleNamespace(
        fit=fit,
        wcs=None,  # skip the WCS diagnostic block
        wcs_metadata=SimpleNamespace(x_ifov_arcsec=1.5),
        fwhm_stats=SimpleNamespace(median_fwhm=3.0, std_fwhm=0.5),
    )
    return SimpleNamespace(
        frame=image,
        starfield=starfield,
        detections=SimpleNamespace(detections=list(detections)) if detections else None,
        streak_candidates=list(streak_candidates),
        photometry_summary=None,
        frame_metadata=(SimpleNamespace(exposure_time_seconds=exposure) if exposure else None),
        seeing=SimpleNamespace(pixel_fwhm=3.2, pixel_fwhm_stdev=0.4),
        streak=SimpleNamespace(fwhm=4.2),
    )


def run_pipeline(
    monkeypatch, inputs, images, *, sidereal=(), rate=(), from_sequence=False, completed=True
):
    """Run process_frames with the SENPAI engine calls stubbed out."""
    p = SenpaiPipeline.__new__(SenpaiPipeline)
    p.config = SimpleNamespace(runtime=SimpleNamespace(output_dir="/tmp/senpai-test"))

    run = SimpleNamespace(
        sidereal_frames=list(sidereal),
        rate_track_frames=list(rate),
        completed=completed,
    )
    monkeypatch.setattr(
        pipeline_mod,
        "ProcessedFitsImage",
        SimpleNamespace(from_file_bytes=lambda data, file_path: images[file_path]),
    )
    monkeypatch.setattr(pipeline_mod, "process_senpai_collect", lambda imgs, **kwargs: run)
    monkeypatch.setattr(pipeline_mod, "final_plots", lambda run, out: None)

    return p.process_frames(inputs, from_sequence)


def inp(name, **fields):
    return FrameInput(data=b"", file_path=name, **fields)


class TestProcessFrames:
    def test_maps_results_by_file_path(self, monkeypatch):
        images = {name: make_image(file_path=name) for name in ("a.fits", "b.fits", "c.fits")}
        results = run_pipeline(
            monkeypatch,
            [
                inp("a.fits", task_id="t1", frame_num=0, frame_count=3),
                inp("b.fits", task_id="t1", frame_num=1, frame_count=3),
                inp("c.fits", task_id="t1", frame_num=2, frame_count=3),
            ],
            images,
            sidereal=[make_frame(images["a.fits"])],
            rate=[make_frame(images["b.fits"])],
            from_sequence=True,
        )

        assert [r.file_path for r in results] == ["a.fits", "b.fits", "c.fits"]
        assert [r.track_mode for r in results] == ["SIDEREAL", "RATE", "UNKNOWN"]
        # The frameless input gets an empty, unsolved result.
        assert results[2].solved is False
        assert results[2].detections == []
        assert [r.from_sequence for r in results] == [True, True, True]
        assert all(r.task_id == "t1" for r in results)

    def test_from_sequence_follows_argument(self, monkeypatch):
        images = {"a.fits": make_image(file_path="a.fits")}
        [result] = run_pipeline(
            monkeypatch, [inp("a.fits")], images, sidereal=[make_frame(images["a.fits"])]
        )
        assert result.from_sequence is False

        [result] = run_pipeline(
            monkeypatch,
            [inp("a.fits")],
            images,
            sidereal=[make_frame(images["a.fits"])],
            from_sequence=True,
        )
        assert result.from_sequence is True

    def test_kind_mapping(self, monkeypatch):
        images = {"a.fits": make_image(file_path="a.fits")}
        frame = make_frame(
            images["a.fits"],
            detections=[make_det("streak"), make_det("point")],
            streak_candidates=[make_candidate()],
        )
        [result] = run_pipeline(monkeypatch, [inp("a.fits")], images, sidereal=[frame])

        assert [det.kind for det in result.detections] == [
            "streak",
            "point",
            "streak_candidate",
        ]
        assert result.solved is True
        # fwhm_stats (3.0 px) × plate scale (1.5 "/px)
        assert result.median_fwhm_arcsec == 4.5

    def test_rate_mode_uses_streak_fwhm(self, monkeypatch):
        images = {"a.fits": make_image(file_path="a.fits")}
        frame = make_frame(images["a.fits"], detections=[make_det("point")])
        [result] = run_pipeline(monkeypatch, [inp("a.fits")], images, rate=[frame])

        assert result.track_mode == "RATE"
        assert result.detections[0].kind == "point"
        assert result.median_fwhm_pixels == 4.2  # streak fwhm, not point-source stats

    def test_exposure_from_frame_metadata(self, monkeypatch):
        images = {"a.fits": make_image(file_path="a.fits")}
        frame = make_frame(images["a.fits"], exposure=2.5)
        [result] = run_pipeline(monkeypatch, [inp("a.fits")], images, sidereal=[frame])
        assert result.exposure_time_seconds == 2.5

    def test_exposure_falls_back_to_header(self, monkeypatch):
        images = {"a.fits": make_image(file_path="a.fits", EXPTIME=3.5)}
        frame = make_frame(images["a.fits"], exposure=None)
        [result] = run_pipeline(monkeypatch, [inp("a.fits")], images, sidereal=[frame])
        assert result.exposure_time_seconds == 3.5

    def test_failed_run_still_returns_results(self, monkeypatch):
        """A collect with no solved frames returns unsolved results without raising."""
        images = {"a.fits": make_image(file_path="a.fits")}
        results = run_pipeline(monkeypatch, [inp("a.fits", task_id="t9")], images, completed=False)
        assert [r.file_path for r in results] == ["a.fits"]
        assert results[0].solved is False
        assert results[0].track_mode == "UNKNOWN"

    def test_bad_frame_does_not_discard_batch(self, monkeypatch):
        """A frame whose results can't be extracted is skipped, not batch-fatal."""
        images = {
            "a.fits": make_image(file_path="a.fits"),
            "b.fits": SimpleNamespace(header={}, file_path="b.fits"),  # no DATE-OBS
        }
        results = run_pipeline(
            monkeypatch,
            [inp("a.fits"), inp("b.fits")],
            images,
            sidereal=[make_frame(images["a.fits"]), make_frame(images["b.fits"])],
        )

        assert [r.file_path for r in results] == ["a.fits"]


# ---------------------------------------------------------------------------
# AFMODE: a batch can request a cheaper pipeline mode via its FITS header
# ---------------------------------------------------------------------------


class _Img:
    def __init__(self, **cards):
        self.header = dict(cards)


class TestRequestedPipelineMode:
    """Reading the requested mode off the frames. Unsafe inputs fall back to configured."""

    def test_reads_a_valid_request(self):
        from sensorkit.senpai.pipeline import _requested_pipeline_mode

        assert _requested_pipeline_mode([_Img(AFMODE="detect")]) == "detect"
        assert _requested_pipeline_mode([_Img(AFMODE=" detect_solve ")]) == "detect_solve"

    def test_science_frames_request_nothing(self):
        from sensorkit.senpai.pipeline import _requested_pipeline_mode

        assert _requested_pipeline_mode([_Img(), _Img(FOCUSPOS=25000.0)]) is None

    def test_unknown_mode_is_ignored(self):
        """Header content drives behaviour here, so an unrecognized value must not be guessed."""
        from sensorkit.senpai.pipeline import _requested_pipeline_mode

        assert _requested_pipeline_mode([_Img(AFMODE="turbo")]) is None

    def test_conflicting_requests_are_ignored(self):
        from sensorkit.senpai.pipeline import _requested_pipeline_mode

        batch = [_Img(AFMODE="detect"), _Img(AFMODE="full")]
        assert _requested_pipeline_mode(batch) is None

    def test_agreeing_requests_across_a_batch_are_honoured(self):
        from sensorkit.senpai.pipeline import _requested_pipeline_mode

        batch = [_Img(AFMODE="detect"), _Img(AFMODE="detect"), _Img()]
        assert _requested_pipeline_mode(batch) == "detect"
