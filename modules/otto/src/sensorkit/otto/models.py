# SPDX-License-Identifier: Apache-2.0
from typing import List, Literal

from pydantic import BaseModel, Field, model_validator

import sensorkit.api as sk

OrbitRegime = Literal["LEO", "MEO", "GEO", "HEO"]


class TaskConfig(BaseModel):
    stars: List[str] = Field(default_factory=list)
    orbits: List[OrbitRegime] = Field(default_factory=list)
    tles: List[str] = Field(default_factory=list)
    horizons: List[str] = Field(default_factory=list)
    tle_update_interval_hours: int = 24
    horizons_update_interval_hours: int = 6
    graylist_interval_minutes: int = 300
    end_time_deadband_seconds: int = 60
    inter_task_delay_seconds: float = 0.0

    @model_validator(mode="after")
    def _require_targets(self):
        if not self.stars and not self.orbits and not self.tles and not self.horizons:
            raise ValueError(
                "at least one of task.stars, task.orbits, task.tles, "
                "or task.horizons is required"
            )
        return self

    @property
    def named_objects(self) -> List[str]:
        """Explicitly named objects across every source: star names, NORAD IDs,
        and Horizons names. These share the whitelist/graylist/blacklist
        machinery; orbit regimes are selected live and never list-managed."""
        return [*self.stars, *self.tles, *self.horizons]


class CollectConfig(BaseModel):
    altitude_min: float = 20.0
    track_mode: Literal["rate", "sidereal", "rate_sidereal"]
    dither: bool = False
    dither_amount_arcsec: float = 0.0
    scan_mode: bool = False
    scan_direction: Literal["eastward", "westward"] | None = None
    filters: List[str] = Field(default_factory=list)
    readout_mode: int | None = None
    gain: float | None = None
    exposure_min: int = 1
    exposure_max: int = 10
    exposure_delta: int = 1
    binning: List[int] = Field(default_factory=lambda: [1, 2, 4])
    num_frames: int = 3


class GDrivePublishConfig(BaseModel):
    folder_id: str  # destination Drive folder
    # Credentials: GDRIVE_TOKEN_PATH (path to the saved OAuth token)


class DropboxPublishConfig(BaseModel):
    upload_path: str = "/otto"  # Dropbox folder path
    # Credentials: DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN


class UDLPublishConfig(BaseModel):
    base_url: str | None = None  # None -> production UDL
    id_sensor: str  # SkyImagery idSensor (the UDL-registered sensor ID)
    source: str  # UDL provenance: the org/system originating the record
    classification_marking: str = "U"
    data_mode: str = "TEST"
    image_type: str = "FITS"
    upload_timeout: float = 300.0  # imagery can be large; more generous than JSON APIs
    # Credentials: UDL_USERNAME, UDL_PASSWORD (both, or neither for
    # UDL-compliant endpoints that don't enforce auth)


class PublishConfig(BaseModel):
    upload: bool = False
    env_file: str = ".env"  # publisher credentials; falls back to the process environment
    gdrive: GDrivePublishConfig | None = None
    dropbox: DropboxPublishConfig | None = None
    udl: UDLPublishConfig | None = None


class OttoConfig(BaseModel):
    controller: str
    task: TaskConfig
    collect: CollectConfig
    publish: PublishConfig


sk.declare_config_section(
    "otto",
    list[OttoConfig],
    id_source="by_subkey",
    service_path="sensorkit.otto.program",
)
