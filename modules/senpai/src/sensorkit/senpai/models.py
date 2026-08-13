# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

import sensorkit.api as sk


class SenpaiConfig(BaseModel):
    senpai_config: str
    senpai_output_dir: str
    process_sequence: bool = Field(default=True)
    # NB: a batch can request a cheaper pipeline mode per-frame via the AFMODE FITS card
    # (see sensorkit.autofocus VCurveConfig.pipeline_mode). There is no config
    # knob here: the requesting module owns the choice, and it travels on the frames.


sk.declare_config_section(
    "senpai",
    SenpaiConfig,
    entity_mapper=lambda raw: raw.pop("id", "senpai"),
    service_path="sensorkit.senpai.service",
)


class Star(BaseModel):
    """A detected field star, for consumers needing per-source geometry in any pipeline mode
    (e.g. the autofocus defocus-sign quadrupoles) — unlike `detections`, which the full-mode
    catalog pass builds (catalog-unmatched sources) and UDL publishes as observations."""

    x: float
    y: float
    snr: float | None = None


class Detection(BaseModel):
    """Per-detection stats for either point sources or streaks in either a sidereal or rate-tracked frame."""

    # SENPAI's discriminator: "streak" = confirmed satellite streak, "point" =
    # point source (the satellite candidate in rate-tracked frames),
    # "streak_candidate" = unconfirmed streak.
    kind: Literal["streak", "point", "streak_candidate"] | None = None

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

    # Collect identity passed through from the DataGraph context (present when
    # the deployment maps the corresponding FITS headers), letting consumers
    # correlate results to the tasking that produced the frame.
    task_id: str | None = None
    frame_num: int | None = None
    frame_count: int | None = None

    # True when this result came out of multi-frame sequence processing
    # (e.g. sidereal-anchored WCS, cross-frame streak confirmation).
    from_sequence: bool = True

    exposure_time_seconds: float | None = None
    n_sources: int | None
    median_fwhm_pixels: float | None
    std_fwhm_pixels: float | None
    pixel_scale_arcsec: float | None
    median_fwhm_arcsec: float | None
    std_fwhm_arcsec: float | None
    solved: bool
    detections: list[Detection]
    # Detection-stage field stars — populated in EVERY pipeline mode (see Star).
    stars: list[Star] = Field(default_factory=list)
    photometry: Photometry | None = None
