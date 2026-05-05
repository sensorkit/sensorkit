from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel

import sensorkit.api as sk


class AcquisitionConfig(BaseModel):
    mode: str = "watch_directory"
    watch_path: str | None = None
    watch_pattern: list[str] | str = "*.fits"
    watch_recursive: bool = False
    alpaca_host: str | None = None
    alpaca_port: int = 11111
    alpaca_device_number: int = 0
    exposure_time_seconds: float = 10.0
    interval_seconds: float = 60.0


class AllClearConfig(BaseModel):
    instrument_model_path: str
    clear_threshold: float = 0.7


class OutputConfig(BaseModel):
    directory: str
    movie_lookback_hours: float = 24.0
    retention_hours: float = 48.0


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8200


class SkyTransmissionConfig(BaseModel):
    entity: str
    controller: str
    acquisition: AcquisitionConfig
    allclear: AllClearConfig
    mounts: list[str] = []
    output: OutputConfig
    server: ServerConfig


@dataclass
class FrameState:
    """Shared mutable state between the analyzer and the image server."""

    latest_jpeg: bytes = b""
    frame_id: int = 0
    clear_fraction: float = 0.0
    n_matched: int = 0
    n_expected: int = 0
    solve_status: str = "pending"
    output_dir: str = ""


@sk.declare_keyword
class SkyTransmission(BaseModel):
    """Sky transmission state from allclear."""

    clear_fraction: float
    n_matched: int
    n_expected: int
    threshold: float
    status: str


@sk.declare_keyword
class SkyTransmissionMap(BaseModel):
    """Sky transmission map in alt/az coordinates."""

    alt_grid: list[float]
    az_grid: list[float]
    transmission: list[list[float]]
    obs_time: str
    site_lat: float
    site_lon: float
