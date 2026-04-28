import os
from typing import List, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class EnvResolvingModel(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def resolve_env_vars(cls, values):
        for key, val in values.items():
            if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
                env_name = val[2:-1]
                resolved = os.environ.get(env_name)
                if resolved is None:
                    raise ValueError(f"Environment variable {env_name} is not set")
                values[key] = resolved
        return values


class TaskConfig(BaseModel):
    objects: List[str]
    tle_update_interval_hours: int = 24
    graylist_interval_minutes: int = 300
    end_time_deadband_seconds: int = 60


class CollectConfig(BaseModel):
    altitude_min: float = 20.0
    track_mode: str
    dither: bool = False
    dither_amount_arcsec: float = 0.0
    scan_mode: bool = False
    scan_direction: Literal["eastward", "westward"] | None = None
    filters: List[str] = Field(default_factory=list)
    exposure_min: int = 1
    exposure_max: int = 10
    exposure_delta: int = 1
    binning: List[int] = Field(default_factory=lambda: [1, 2, 4])
    num_frames: int = 3


class GDrivePublishConfig(EnvResolvingModel):
    credentials_file: str  # path to credentials.json (OAuth client)
    token_file: str  # path to token.json (saved refresh token)
    folder_id: str


class DropboxPublishConfig(EnvResolvingModel):
    app_key: str
    app_secret: str
    refresh_token: str
    upload_path: str = "/otto"  # Dropbox folder path


class UDLPublishConfig(EnvResolvingModel):
    username: str
    password: str


class PublishConfig(EnvResolvingModel):
    upload: bool = False
    sensor_name: str
    gdrive: GDrivePublishConfig | None = None
    dropbox: DropboxPublishConfig | None = None
    udl: UDLPublishConfig | None = None


UvicornLogLevel = Literal["critical", "error", "warning", "info", "debug", "trace"]


class ServerConfig(BaseModel):
    host: str
    port: int = 8000
    log_level: UvicornLogLevel | None = None

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, v):
        return v.lower() if isinstance(v, str) else v


class OttoConfig(BaseModel):
    controller: str
    task: TaskConfig
    collect: CollectConfig
    publish: PublishConfig
    server: ServerConfig