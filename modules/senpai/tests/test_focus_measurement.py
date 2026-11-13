# SPDX-License-Identifier: Apache-2.0
"""Focus width measurements using the real SENPAI engine."""

import importlib.resources
import io

import pytest
from astropy.io import fits

from sensorkit.senpai.pipeline import SenpaiPipeline

from .images import star_field_fits


@pytest.fixture
def pipeline(tmp_path):
    config = importlib.resources.files("senpai") / "resources" / "config" / "local.yaml"
    return SenpaiPipeline(str(config), str(tmp_path))


def test_starless_image_has_no_width_measurement(pipeline):
    result = pipeline.measure_focus(star_field_fits(4.0, stars=0), "blank.fits")

    assert result.median_fwhm_pixels is None
    assert result.measurement_count == 0
    assert result.reason is not None


def test_default_width_is_rejected_near_an_image_edge(pipeline):
    buffer = io.BytesIO()

    with fits.open(io.BytesIO(star_field_fits(4.0, size=64, stars=1))) as image:
        # Crop near the star while keeping its core visible.
        image[0].data = image[0].data[:40, :40]
        image.writeto(buffer)

    result = pipeline.measure_focus(buffer.getvalue(), "edge.fits")

    assert result.median_fwhm_pixels is None
    assert result.measurement_count == 0
    assert result.reason is not None


def test_default_width_is_rejected_with_a_detected_star(pipeline):
    result = pipeline.measure_focus(star_field_fits(4.0, stars=1), "star.fits")

    assert result.median_fwhm_pixels is None
    assert result.measurement_count == 0
    assert result.reason is not None


@pytest.mark.parametrize("width", [3.0, 6.0])
def test_nondefault_width_is_usable(pipeline, width):
    result = pipeline.measure_focus(star_field_fits(width), "stars.fits")

    assert result.reason is None
    assert result.measurement_count == 1
    assert result.median_fwhm_pixels == pytest.approx(width, abs=0.5)
