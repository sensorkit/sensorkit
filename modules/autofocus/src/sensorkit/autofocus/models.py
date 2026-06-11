from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, Field

import sensorkit.api as sk


class FocuserConfig(BaseModel):
    entity: str
    min_position: float
    max_position: float


class VCurveConfig(BaseModel):
    schedule: str | None = None
    num_steps: int | None = None
    filter_name: str | None = None
    exposure_time: float = 1
    binning: int = 1


class CorrectionConfig(BaseModel):
    max_step: float
    min_correction: float
    cooldown_seconds: float = 300
    defocus_target_arcsec: float | None = None  # Adjust focus for this FWHM rather than the minimum


class OutputConfig(BaseModel):
    directory: str = "/opt/sk/data/autofocus"
    retention_hours: float = 48.0


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8200


class AutofocusConfig(BaseModel):
    entity: str
    controller: str
    senpai_entity: str
    focuser: FocuserConfig
    vcurve: VCurveConfig = Field(default_factory=VCurveConfig)
    correction: CorrectionConfig = Field(default_factory=CorrectionConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    calibrate_on_startup: bool = False
    min_altitude: float = 15
    max_psf_arcsec: float = 10  # Do not correct if larger than this value


@sk.declare_keyword
class AutofocusStatus(BaseModel):
    # Focus correction (passive or V-curve)
    old_position: float | None = None
    new_position: float | None = None
    method: str | None = None

    # V-curve results
    vcurve_timestamp: datetime | None = None
    vcurve_best_position: float | None = None
    vcurve_best_fwhm_arcsec: float | None = None


@dataclass
class AutofocusState:
    vcurve_slope: float | None = None
    vcurve_best_fwhm_pixels: float | None = None
    defocus_sign_convention: int = 1
    last_correction_time: datetime | None = None
