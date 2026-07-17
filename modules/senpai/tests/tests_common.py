# SPDX-License-Identifier: Apache-2.0
"""Shared helpers for senpai module tests."""

from datetime import UTC, datetime

from sensorkit.senpai.models import SenpaiResult


def make_result(file_path="/data/raw/frame.fits", **overrides) -> SenpaiResult:
    fields = dict(
        file_path=file_path,
        timestamp=datetime(2026, 7, 9, 1, 2, 3, tzinfo=UTC),
        track_mode="SIDEREAL",
        n_sources=0,
        median_fwhm_pixels=None,
        std_fwhm_pixels=None,
        pixel_scale_arcsec=None,
        median_fwhm_arcsec=None,
        std_fwhm_arcsec=None,
        solved=False,
        detections=[],
    )
    fields.update(overrides)
    return SenpaiResult(**fields)
