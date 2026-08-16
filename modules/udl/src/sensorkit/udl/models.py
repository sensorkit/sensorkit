# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field

# Re-export SDK types for convenience
from unifieddatalibrary.types import CollectRequestFull, CollectResponseFull  # noqa: F401
from unifieddatalibrary.types.shared import StateVectorFull  # noqa: F401

import sensorkit.api as sk


class ResponseStatus(StrEnum):
    """UDL CollectResponse status values."""
    ACCEPTED = "ACCEPTED"
    CANCELLED = "CANCELLED"
    COLLECTED = "COLLECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    SCHEDULED = "SCHEDULED"


class UDLReferenceFrame(StrEnum):
    """Reference frame types from UDL state vectors."""
    J2000 = "J2000"
    TEME = "TEME"
    EFG_TDR = "EFG/TDR"
    ICRF = "ICRF"

    def to_sensorkit_frame(self):
        """Convert UDL reference frame to SensorKit ReferenceFrame."""
        from sensorkit.astro.common import ReferenceFrame
        match self:
            case UDLReferenceFrame.J2000 | UDLReferenceFrame.ICRF:
                return ReferenceFrame.GCRF
            case UDLReferenceFrame.TEME:
                return ReferenceFrame.TEME
            case UDLReferenceFrame.EFG_TDR:
                return ReferenceFrame.ITRF
            case _:
                raise ValueError(f"Unsupported reference frame: {self}")


# =============================================================================
# Configuration Models
# =============================================================================

class UDLEndpointConfig(BaseModel):
    """Connection and auth settings for one UDL-compatible endpoint."""
    # Base URL (optional - SDK defaults to UDL production)
    base_url: str | None = None

    # Timeouts
    timeout: float = Field(default=60.0, description="HTTP request timeout in seconds")
    upload_timeout: float = Field(
        default=300.0,
        description=(
            "Timeout for SkyImagery uploads in seconds. Imagery can be "
            "hundreds of MB, so this is more generous than the per-request "
            "timeout used for the JSON API."
        ),
    )

    # Auth method selector
    use_certs: bool = Field(
        default=False,
        description=(
            "If True, use cert-based auth (for UDL-compliant endpoints that "
            "require client certificates). If False, use username/password."
        ),
    )

    # Path to .env file for username/password auth (when use_certs=False)
    # Expects UDL_USERNAME and UDL_PASSWORD
    env_file: str = Field(default=".env", description="Path to .env file containing UDL_USERNAME and UDL_PASSWORD")

    # Cert-based auth (when use_certs=True)
    client_cert: str | None = None
    client_key: str | None = None
    client_verify: bool = Field(default=True)


class UDLAPIConfig(UDLEndpointConfig):
    """Configuration for UDL API connection."""
    # Optional separate endpoint for SkyImagery uploads, with its own auth
    # settings (e.g. poll UDL with basic auth, upload to a cert-authenticated
    # UDL-compliant endpoint). When None (default), imagery is uploaded to the
    # primary endpoint (backward compatible).
    upload: UDLEndpointConfig | None = Field(
        default=None,
        description=(
            "Separate endpoint for SkyImagery uploads. Polling and "
            "CollectResponses always use the primary endpoint."
        ),
    )

    # Sensor identification — the CollectRequest poll filter value, and stamped
    # as both idSensor and origSensorId on CollectResponses (and as idSensor on
    # SkyImagery)
    id_sensor: str = Field(description="Sensor ID (poll filter value; maps to idSensor and origSensorId)")
    poll_filter: Literal["id_sensor", "orig_sensor_id"] = Field(
        default="id_sensor",
        description=(
            "Which CollectRequest field the poll matches id_sensor against: "
            "idSensor (default) or origSensorId."
        ),
    )
    source: str = Field(description="Data source identifier (e.g. 'DAO')")


class SkyImageryPublishConfig(BaseModel):
    """SkyImagery delivery settings; presence of the block enables the publisher."""
    image_type: str = Field(
        default="FITS",
        description=(
            "Default SkyImagery imageType (e.g. FITS, EOSSA, EOCHIP, MP4). "
            "Per-frame override is honored if the data graph context provides 'image_type'."
        ),
    )
    save_path: str | None = Field(
        default=None,
        description="Path to save skyimagery archives locally before upload"
    )


class EOObservationPublishConfig(BaseModel):
    """EOObservation delivery settings; presence of the block enables the publisher.

    Detections come from the senpai module's published SenpaiResults; only
    frames correlated to a CollectRequest produce records. Detection-quality
    thresholds live in SENPAI's own engine config, not here.
    """
    sequence_only: bool = Field(
        default=True,
        description=(
            "Only post detections from sequence-derived SenpaiResults "
            "(multi-frame SENPAI processing with sidereal-anchored WCS). "
            "False also posts per-frame results."
        ),
    )
    mag_bands: list[str] = Field(
        default_factory=lambda: ["G"],
        description=(
            "Calibrated-magnitude band priority; the first band present in a "
            "detection's calibrated_magnitudes populates mag/magUnc."
        ),
    )
    save_path: str | None = Field(
        default=None,
        description="Path to save posted EOObservation JSON locally"
    )


class PublishConfig(BaseModel):
    """Data delivery configuration: which UDL record types to publish."""
    upload: bool = Field(
        default=True,
        description="Master switch; False disables all data delivery without removing the blocks.",
    )
    sky_imagery: SkyImageryPublishConfig | None = Field(
        default=None,
        description="SkyImagery upload of collected frames. Absent ⇒ disabled.",
    )
    eo_observation: EOObservationPublishConfig | None = Field(
        default=None,
        description="EOObservation posting of senpai detections. Absent ⇒ disabled.",
    )


class UDLConfig(BaseModel):
    """Main configuration for UDL program."""
    controller: str
    api: UDLAPIConfig
    poll_frequency: float = Field(default=10.0, description="Polling interval in seconds")
    end_time_deadband_s: float = Field(default=0.0, description="Deadband added to task end times")
    publish: PublishConfig = Field(
        default_factory=PublishConfig,
        description="Data delivery: sky_imagery and/or eo_observation blocks.",
    )


sk.declare_config_section(
    "udl",
    list[UDLConfig],
    id_source="by_subkey",
    service_path="sensorkit.udl.service",
)
