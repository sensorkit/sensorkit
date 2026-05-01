"""Tests for AllClearPipeline header parsing and MovieBuilder encoding."""

from __future__ import annotations

import pathlib
import shutil
import tempfile

import numpy as np
import pytest
from astropy.io import fits

from sensorkit.sky_transmission.pipeline import AllClearPipeline, FrameResult, MovieBuilder
from sensorkit.sky_transmission.models import AllClearConfig

SAMPLE_IMAGE = "/opt/sk/data/sky_transmission/input/2026-03-12-0620_2-CapObj_0312.fits"
INSTRUMENT_MODEL = "/opt/sk/data/sky_transmission/input/instrument_model.json"

_sample_data_available = (
    pathlib.Path(SAMPLE_IMAGE).exists() and pathlib.Path(INSTRUMENT_MODEL).exists()
)
requires_sample_data = pytest.mark.skipif(
    not _sample_data_available, reason="Sample data not available"
)


@pytest.fixture
def pipeline():
    config = AllClearConfig(
        instrument_model_path=INSTRUMENT_MODEL,
        clear_threshold=0.7,
    )
    return AllClearPipeline(config)


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp(prefix="sky_tx_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# -- Pipeline header parsing tests --


@requires_sample_data
class TestPipelineHeaderParsing:
    """Verify process_frame handles missing FITS headers gracefully."""

    def test_normal_fits_with_all_headers(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert isinstance(result, FrameResult)
        assert result.status != "failed", f"Pipeline failed: {result.status}"
        assert result.status != "no_exposure"
        assert result.n_matched > 0

    def test_site_location_fallback(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.status != "failed"
        assert result.n_expected > 0

    def test_instrument_model_fallback(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=None,
        )
        assert result.status != "failed"
        assert result.n_expected > 0

    def test_missing_exposure_returns_no_exposure(self, pipeline, tmp_dir):
        no_exp_path = str(pathlib.Path(tmp_dir) / "no_exposure.fits")
        hdu = fits.open(SAMPLE_IMAGE)
        for key in ("EXPTIME", "XPOSURE", "EXPOSURE"):
            if key in hdu[0].header:
                del hdu[0].header[key]
        hdu.writeto(no_exp_path, overwrite=True)
        hdu.close()

        result = pipeline.process_frame(
            no_exp_path,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.status == "no_exposure"
        assert result.clear_fraction == 0.0
        assert result.n_matched == 0

    def test_missing_date_obs_uses_mtime(self, pipeline, tmp_dir):
        no_date_path = str(pathlib.Path(tmp_dir) / "no_date.fits")
        hdu = fits.open(SAMPLE_IMAGE)
        for key in ("DATE-OBS", "DATE"):
            if key in hdu[0].header:
                del hdu[0].header[key]
        hdu.writeto(no_date_path, overwrite=True)
        hdu.close()

        result = pipeline.process_frame(
            no_date_path,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.status != "failed"

    def test_annotated_jpeg_produced(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.status != "failed"
        assert len(result.annotated_jpeg) > 0

    def test_annotated_png_saved(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.annotated_path is not None
        assert pathlib.Path(result.annotated_path).exists()

    def test_sky_result_returned(self, pipeline, tmp_dir):
        from allclear.api import SkyTransmissionResult

        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        assert result.status != "failed"
        assert isinstance(result.sky_result, SkyTransmissionResult)
        assert result.sky_result.clear_fraction == result.clear_fraction

        trans = result.sky_result.query(az_deg=180, alt_deg=45)
        assert isinstance(trans, float)

        info = result.sky_result.query_radec(ra_deg=83.63, dec_deg=22.01)
        assert "status" in info
        assert info["status"] in ("CLEAR", "CLOUDY", "NO_DATA", "BELOW_HORIZON")

        d = result.sky_result.to_dict()
        assert isinstance(d, dict)
        assert "transmission_map" in d

    def test_sky_result_none_on_no_exposure(self, pipeline, tmp_dir):
        no_exp_path = str(pathlib.Path(tmp_dir) / "no_exposure2.fits")
        hdu = fits.open(SAMPLE_IMAGE)
        for key in ("EXPTIME", "XPOSURE", "EXPOSURE"):
            if key in hdu[0].header:
                del hdu[0].header[key]
        hdu.writeto(no_exp_path, overwrite=True)
        hdu.close()

        result = pipeline.process_frame(no_exp_path, site_location=(20.7458, -156.4317))
        assert result.status == "no_exposure"
        assert result.sky_result is None


# -- MovieBuilder tests --


class TestMovieBuilder:
    """Verify MovieBuilder produces valid MP4 files."""

    def _create_fake_frames(self, output_dir: str, count: int = 3):
        from PIL import Image

        out = pathlib.Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        for i in range(count):
            img = Image.fromarray(
                np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
            )
            img.save(str(out / f"frame_{i:03d}_transmission.png"))

    def test_build_movie_from_synthetic_frames(self, tmp_dir):
        self._create_fake_frames(tmp_dir, count=4)
        builder = MovieBuilder(max_width=320)
        movie_path = str(pathlib.Path(tmp_dir) / "test.mp4")
        ok = builder.build_movie(tmp_dir, movie_path, lookback_hours=1.0)
        assert ok is True
        assert pathlib.Path(movie_path).exists()
        assert pathlib.Path(movie_path).stat().st_size > 0

    def test_build_movie_insufficient_frames(self, tmp_dir):
        self._create_fake_frames(tmp_dir, count=1)
        builder = MovieBuilder()
        movie_path = str(pathlib.Path(tmp_dir) / "test.mp4")
        ok = builder.build_movie(tmp_dir, movie_path, lookback_hours=1.0)
        assert ok is False

    @requires_sample_data
    def test_build_movie_from_pipeline_output(self, pipeline, tmp_dir):
        result = pipeline.process_frame(
            SAMPLE_IMAGE,
            output_dir=tmp_dir,
            site_location=(20.7458, -156.4317),
        )
        if result.status == "failed" or result.annotated_path is None:
            pytest.skip("Pipeline did not produce an annotated frame")

        src = pathlib.Path(result.annotated_path)
        for i in range(2):
            shutil.copy(str(src), str(src.parent / f"copy_{i}_transmission.png"))

        builder = MovieBuilder(max_width=800)
        movie_path = str(pathlib.Path(tmp_dir) / "latest.mp4")
        ok = builder.build_movie(tmp_dir, movie_path, lookback_hours=1.0)
        assert ok is True
        assert pathlib.Path(movie_path).stat().st_size > 0

    def test_build_movie_odd_dimensions(self, tmp_dir):
        from PIL import Image

        out = pathlib.Path(tmp_dir)
        for i in range(3):
            img = Image.fromarray(
                np.random.randint(0, 255, (481, 641, 3), dtype=np.uint8)
            )
            img.save(str(out / f"odd_{i:03d}_transmission.png"))

        builder = MovieBuilder(max_width=641)
        movie_path = str(pathlib.Path(tmp_dir) / "odd.mp4")
        ok = builder.build_movie(tmp_dir, movie_path, lookback_hours=1.0)
        assert ok is True
