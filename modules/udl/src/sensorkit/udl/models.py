from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

# Re-export SDK types for convenience
from unifieddatalibrary.types import CollectRequestFull, CollectResponseFull
from unifieddatalibrary.types.shared import StateVectorFull


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

class UDLAPIConfig(BaseModel):
    """Configuration for UDL API connection."""
    # Base URL (optional - SDK defaults to UDL production)
    base_url: str | None = None

    # Sensor identification — used as both idSensor (SkyImagery, CollectResponse)
    # and origSensorId (CollectRequest polling filter)
    id_sensor: str = Field(description="Sensor ID (maps to idSensor and origSensorId)")
    source: str = Field(description="Data source identifier (e.g. 'DAO', 'MACHINA')")

    # Timeouts
    timeout: float = Field(default=60.0, description="HTTP request timeout in seconds")

    # Auth method selector
    use_certs: bool = Field(
        default=False,
        description="If True, use cert-based auth (MACHINA). If False, use username/password (UDL)."
    )

    # Path to .env file for username/password auth (UDL, when use_certs=False)
    # Expects UDL_USERNAME and UDL_PASSWORD
    env_file: str = Field(default=".env", description="Path to .env file containing UDL_USERNAME and UDL_PASSWORD")

    # Cert-based auth (for MACHINA, when use_certs=True)
    client_cert: str | None = None
    client_key: str | None = None
    client_verify: bool = Field(default=True)


class UDLConfig(BaseModel):
    """Main configuration for UDL program."""
    entity: str
    controller: str
    api: UDLAPIConfig
    poll_frequency: float = Field(default=10.0, description="Polling interval in seconds")
    end_time_deadband_s: float = Field(default=0.0, description="Deadband added to task end times")
    skyimagery_save_path: str | None = Field(
        default=None,
        description="Path to save skyimagery archives locally before upload"
    )
