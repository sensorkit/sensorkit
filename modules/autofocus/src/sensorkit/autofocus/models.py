from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

import sensorkit.api as sk


class FocuserConfig(BaseModel):
    entity: str
    # Focuser hard limits [steps]; the sweep is clamped to these.
    min_position: float
    max_position: float
    timeout: float = 60.0  # budget for one focuser move [s]; sets V-curve task end times


class VCurveConfig(BaseModel):
    schedule: str | None = None  # optional daily sweep time; see README (Example Config)
    # `num_steps` samples spaced by `step_size` steps, centered on current focus; if `step_size`
    # is unset, `num_steps` span the full [min_position, max_position] range.
    num_steps: int = 9
    step_size: float | None = None
    filter_name: str | None = None
    exposure_time: float = 1
    binning: int = 1
    # Finalize a stalled sweep after this long with no new frame (lost-frame fallback); must
    # exceed the per-step interval (exposure + slew + SENPAI processing).
    frame_timeout_seconds: float = 90.0

    # SENPAI mode for THIS sweep's frames, carried as the AFMODE FITS card (science frames keep
    # SENPAI's own mode). See README (Pipeline Mode) for the detect/full FWHM-scale trade-off.
    pipeline_mode: Literal["full", "detect_solve", "detect"] = "detect"


class CorrectionConfig(BaseModel):
    # Correction limits are FWHM DEGRADATION FROM BEST, in arcsec — the metric we act on.
    min_arcsec: float = 0.5  # Deadband: skip correction if FWHM is within this of best [arcsec]
    max_arcsec: float = 3.0  # Above this, don't correct — kick off a V-curve instead
    cooldown_seconds: float = 300  # Don't correct if have already corrected within this period
    # Min |mean radial ellipticity| to call a defocus direction [dimensionless 0-1]. Optics-
    # specific — lower it if corrections skip with defocus_sign=0, raise it if the direction
    # looks noisy. Scale: ~0.003 (no field aberration), 0.03-0.07 (OmniSim sim), 0.10-0.18 (strong).
    defocus_sign_threshold: float = 0.02
    defocus_target_arcsec: float | None = (
        None  # Adjust focus for this FWHM rather than the minimum
    )


class RunVCurve(BaseModel):
    """Queue an autofocus V-curve sweep. Omit ra/dec to auto-select a target."""

    ra: float | None = None
    dec: float | None = None


class SetAutofocusEnabled(BaseModel):
    """Enable or disable the analyzer's focus corrections."""

    enabled: bool


# The analyzer's control surface: entity-level typed Requests (the agent's pattern), registered
# in entity_init via sk.entity().handle_request — no DeviceCommand machinery involved.
#
# Both reply immediately: a simple request must answer inside the backend's NATS deadline (0.5s),
# so run_vcurve hands the sweep off to a task rather than queueing it inline.
run_vcurve_request = sk.Request.define("run_vcurve", payload=RunVCurve)
set_enabled_request = sk.Request.define("set_enabled", payload=SetAutofocusEnabled)


class AutofocusConfig(BaseModel):
    entity: str
    controller: str
    senpai_entity: str
    focuser: FocuserConfig
    vcurve: VCurveConfig = Field(default_factory=VCurveConfig)
    correction: CorrectionConfig = Field(default_factory=CorrectionConfig)
    calibrate_on_startup: bool = False

    # V-curve target-selection constraints.
    min_altitude: float = 15  # Minimum target altitude [deg]
    min_solar_elongation: float = 0  # Minimum angular separation from the Sun [deg]; 0 disables
    min_magnitude: float = 2.0  # Reject targets BRIGHTER than this (avoid saturation) [V mag]
    max_magnitude: float | None = (
        None  # Optional faint cut [V mag]; None -> the catalog's faint end
    )
    catalog_path: str | None = (
        None  # SSTRC7 catalog dir; REQUIRED for target selection (no fallback)
    )

    # Native (1x1) plate scale [arcsec/pixel], used for arcsec reporting when SENPAI doesn't solve
    # (detect mode). Optional; see README (Pipeline Mode).
    pixel_scale_arcsec: float | None = None


@sk.declare_keyword
class VCurveStep(BaseModel):
    """Correlates one V-curve sweep's frames, as the submission context of each per-step task.

    Stamped on every frame as two FITS cards: AFID (sweep id, so the analyzer regroups the
    sweep's frames) and AFMODE (the requested SENPAI pipeline_mode).
    """

    session: str
    pipeline_mode: str = "detect"

    def get_fits_cards(self):
        yield "AFID", (self.session, "Autofocus V-curve ID")
        yield "AFMODE", (self.pipeline_mode, "Autofocus SENPAI pipeline mode")


@sk.declare_keyword
class AutofocusStatus(BaseModel):
    """A focus correction applied by the analyzer."""

    timestamp: datetime  # when the correction was applied
    old_position: float | None = None
    new_position: float | None = None
    method: str | None = None


@sk.declare_keyword
class VCurveResult(BaseModel):
    """Result of a V-curve sweep: the fit and the samples it was taken from.

    A separate keyword from AutofocusStatus so a later passive correction can't clobber the
    retained sweep (clients read the last fit at any time). `samples` are
    `(focuser position [steps], FWHM [pixels])` sorted by position; ×`pixel_scale_arcsec` for arcsec.
    """

    timestamp: datetime
    session: str  # sweep id (FITS AFID)
    best_position: float
    best_fwhm_pixels: float
    slope: float
    r_squared: float
    pixel_scale_arcsec: float | None = None
    samples: list[tuple[float, float]] = Field(default_factory=list)
    # Positions we shot but SENPAI returned no FWHM for (too defocused).
    unmeasured_positions: list[float] = Field(default_factory=list)


class AutofocusState(BaseModel):
    """Persisted autofocus calibration/correction state (survives restarts via the entity KV)."""

    # Master switch for the ANALYZER (passive corrections, recalibrate triggers, V-curve folds).
    # Base + filter offset + last residual always drive regardless. Off by default; toggle with
    # the set_enabled request.
    enabled: bool = False
    vcurve_slope: float | None = None
    vcurve_best_fwhm_pixels: float | None = None
    # Learned mapping from a measured defocus sign to a direction of focuser travel (instrument-
    # specific). None until a V-curve learns it; passive correction stays off until then, since a
    # guessed direction corrects INTO the error. See README.
    defocus_sign_convention: int | None = None
    last_correction_time: datetime | None = None
    last_vcurve_trigger_time: datetime | None = None  # last recalibrate trigger (cooldown)


sk.declare_config_section(
    "autofocus",
    list[AutofocusConfig],
    id_source="by_subkey",
    id_key="entity",
    service_path="sensorkit.autofocus.program",
)
