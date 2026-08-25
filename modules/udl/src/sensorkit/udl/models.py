# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, model_validator

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


class UDLEndpointConfig(BaseModel):
    """Connection and auth settings for one UDL-compatible endpoint."""
    # Base URL (optional; SDK defaults to UDL production)
    base_url: str | None = None

    # Timeouts
    timeout: float = Field(default=60.0, description="HTTP request timeout in seconds")
    upload_timeout: float = Field(
        default=300.0,
        description=(
            "Timeout for SkyImagery uploads in seconds."
        ),
    )

    # Path to .env file for username/password auth (alternative to certs)
    env_file: str = Field(default=".env", description="Path to .env file containing UDL_USERNAME and UDL_PASSWORD")

    # Path to certs-based auth files (alternative to username/password)
    client_cert: str | None = None
    client_key: str | None = None
    client_verify: bool = Field(default=True)


class UDLAPIConfig(UDLEndpointConfig):
    """Configuration for UDL API connection."""
    # Optional separate endpoint for SkyImagery uploads, with its own auth
    # settings (e.g. poll UDL with basic auth, upload to a cert-authenticated
    # UDL-compliant endpoint). When None (default), imagery is uploaded to the
    # primary endpoint.
    upload: UDLEndpointConfig | None = Field(
        default=None,
        description=(
            "Separate endpoint for SkyImagery uploads. Polling, CollectResponses, "
            "and EOObservations use the primary endpoint."
        ),
    )

    # Identify the sensor and the polling method
    id_sensor: str = Field(description="The sensor ID, mapping to 'idSensor' and 'origSensorId'.")
    # Poll for CollectRequests based on idSensor *or* origSensorId
    poll_filter: Literal["idSensor", "origSensorId"] = Field(
        default="idSensor",
        description=(
            "The UDL key ('idSensor' or 'origSensorId', defaults to 'idSensor') to use for CollectRequest polling."
        ),
    )
    source: str = Field(description="The UDL data 'source' identifier.")


class SkyImageryPublishConfig(BaseModel):
    """SkyImagery delivery settings; presence of the block enables the publisher."""
    # Provenance for frames with no CollectRequest
    classification_marking: str | None = None
    data_mode: str | None = None
    origin: str | None = None

    image_type: str = Field(
        default="FITS",
        description=(
            "Default SkyImagery imageType (e.g. FITS, EOSSA, EOCHIP, MP4). "
            "Per-frame override is honored if the data graph context provides 'image_type'."
        ),
    )
    save_path: str | None = Field(
        default=None,
        description="Path to save SkyImagery locally."
    )

    @model_validator(mode="after")
    def _require_classification_marking_and_data_mode(self):
        if bool(self.classification_marking) != bool(self.data_mode):
            raise ValueError("Must set both classification_marking and data_mode for publishing without a CollectRequest")
        return self


class EOObservationPublishConfig(BaseModel):
    """EOObservation delivery settings; presence of the block enables the publisher."""
    # Provenance for results with no CollectRequest
    classification_marking: str | None = None
    data_mode: str | None = None
    origin: str | None = None

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
        description="Path to save EOObservations locally."
    )

    @model_validator(mode="after")
    def _require_classification_marking_and_data_mode(self):
        if bool(self.classification_marking) != bool(self.data_mode):
            raise ValueError("Must set both classification_marking and data_mode for publishing without a CollectRequest")
        return self


class PublishConfig(BaseModel):
    """Which UDL record types to publish. CollectResponses are always published."""
    sky_imagery: SkyImageryPublishConfig | None = Field(
        default=None,
        description="Upload SkyImagery; absent=disabled.",
    )
    eo_observation: EOObservationPublishConfig | None = Field(
        default=None,
        description="Upload EOObservation(s); absent=disabled.).",
    )


class CollectConfig(BaseModel):
    filter_name: str | None = None
    readout_mode: int | None = None
    gain: float | None = None
    binning: int | None = None


class UDLConfig(BaseModel):
    """Main configuration for UDL program."""
    controller: str
    api: UDLAPIConfig
    poll_frequency: float = Field(default=10.0, description="Polling interval in seconds.")
    poll_horizon: float = Field(
        default=86400.0,
        description="Polling horizon in seconds.",
    )
    end_time_deadband_s: float = Field(default=0.0, description="Deadband added to task end times in seconds.")
    collect: CollectConfig = Field(default_factory=CollectConfig)
    publish: PublishConfig = Field(
        default_factory=PublishConfig,
        description="Data delivery: `sky_imagery` and/or `eo_observation` block(s).",
    )


sk.declare_config_section(
    "udl",
    list[UDLConfig],
    id_source="by_subkey",
    service_path="sensorkit.udl.service",
)
