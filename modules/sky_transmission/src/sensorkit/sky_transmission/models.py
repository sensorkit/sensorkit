# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

import sensorkit.api as sk


class AcquisitionConfig(BaseModel):
    mode: str = "watch_directory"  # "watch_directory" | "alpaca"

    # Both modes consume frames from `watch_path` via the directory watcher.
    watch_path: str | None = None
    watch_pattern: list[str] | str = "*.fits"
    watch_recursive: bool = False

    # "alpaca" mode: drive a SensorKit alpaca-module camera entity. Its DataGraph
    # producer (app_source -> array_to_fits -> write_file) writes frames into
    # `watch_path`; we issue a CameraCapture on `camera` every `interval_seconds`.
    camera: str | None = None
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
    # Mount pointing for any mount controlled by these controllers is discovered
    # at runtime and overlaid on the frame; no need to list mounts explicitly.
    controllers: list[str] = []
    acquisition: AcquisitionConfig
    allclear: AllClearConfig
    output: OutputConfig
    server: ServerConfig


sk.declare_config_section(
    "sky_transmission",
    list[SkyTransmissionConfig],
    id_source="by_subkey",
    service_path="sensorkit.sky_transmission.service",
)


@dataclass
class FrameState:
    """Shared mutable state between the analyzer and the image server.

    Only the latest annotated JPEG is needed: status/metrics are published via
    the SkyTransmission keyword (and surfaced by the webapi), so the server has
    no /status endpoint to feed.
    """

    latest_jpeg: bytes = b""
    frame_id: int = 0


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
