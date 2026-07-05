# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

import sensorkit.api as sk


class SenpaiConfig(BaseModel):
    senpai_config: str
    senpai_output_dir: str


sk.declare_config_section(
    "senpai",
    SenpaiConfig,
    entity_mapper=lambda raw: raw.pop("id", "senpai"),
    service_path="sensorkit.senpai.service",
)


class Detection(BaseModel):
    """Per-detection stats for either point sources or streaks in either a sidereal or rate-tracked frame."""

    x: float
    y: float
    snr: float | None = None
    fwhm_pixels: float | None = None

    # Photometry
    flux: float | None = None
    flux_err: float | None = None
    instrumental_mag: float | None = None
    calibrated_magnitudes: dict[str, float] | None = None
    magnitude_errs: dict[str, float] | None = None

    # Streak fields (None for point sources)
    angle_deg: float | None = None
    length_pixels: float | None = None
    width_pixels: float | None = None

    # Pointing from WCS solution
    ra: float | None = None
    dec: float | None = None

    # Rates (for streaks with known exposure time)
    rate_pixels_per_sec: float | None = None
    rate_arcsec_per_sec: float | None = None


class Photometry(BaseModel):
    """Frame-level summary of photometry data for either a sidereal or a rate-tracked frame."""

    zero_point: float | None = None
    zero_point_err: float | None = None
    limiting_mag: float | None = None
    limiting_mag_50: float | None = None
    limiting_mag_90: float | None = None
    median_snr: float | None = None
    median_background: float | None = None
    n_stars: int = 0
    n_quality: int = 0


@sk.declare_keyword
class SenpaiResult(BaseModel):
    """Data published to SensorKit."""

    file_path: str
    timestamp: datetime
    track_mode: str
    n_sources: int | None
    median_fwhm_pixels: float | None
    std_fwhm_pixels: float | None
    pixel_scale_arcsec: float | None
    median_fwhm_arcsec: float | None
    std_fwhm_arcsec: float | None
    solved: bool
    detections: list[Detection]
    photometry: Photometry | None = None
