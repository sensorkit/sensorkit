# SPDX-License-Identifier: Apache-2.0
"""Deprecated surfaces both sensor implementations still carry.

Kept together so that retiring them is one deletion rather than an archaeology
exercise. Neither is tied to the legacy implementation: the flat context keys
below appear in deployed configuration, and the capability record is what today's
UI reads, so both outlive the implementation switch.
"""

from __future__ import annotations

import contextlib
import pathlib
from typing import Literal

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.sensor.config import SensorDevices


# TODO: Phase out when UI code is updated to use ControllerInfo and SensorConfig for this info.
class Capabilities(BaseModel):
    """Deprecated controller capability descriptor for the sensor service."""

    type: Literal["controller"] = "controller"
    tasks: list[str]
    devices: SensorDevices


def add_compat_context(context: sk.Context):
    """Populate the flat context keys deployed header templates still reference."""
    # FIXME: Temporary to avoid breaking existing deployed config.
    compat = dict(
        ra="RADecPointing.right_ascension_hours * 15",
        ra_hms="RADecPointing.ra_hms",
        ra_rate="AxisRates.right_ascension.velocity * 3600",
        dec="RADecPointing.declination_degrees",
        dec_dms="RADecPointing.dec_dms",
        dec_rate="AxisRates.declination.velocity * 3600",
        alt="AltAzPointing.altitude_degrees",
        alt_rate="AxisRates.altitude.velocity * 3600",
        az="AltAzPointing.azimuth_degrees",
        az_rate="AxisRates.azimuth.velocity * 3600",
        track_mode="Collect.track_mode",
        target_id="Collect.target_id",
        target_name="Collect.target.tle.line0 if Collect.target.target_type == 'tle' else target_id",
        tle_line0="Collect.target.tle.line0 if Collect.target.target_type == 'tle' else None",
        tle_line1="Collect.target.tle.line1 if Collect.target.target_type == 'tle' else None",
        tle_line2="Collect.target.tle.line2 if Collect.target.target_type == 'tle' else None",
        frame_num="Collect.frame_number",
        frame_count="Collect.params.frame_count",
        integration_time_seconds="Collect.params.integration_time_seconds",
        binning_x="Collect.params.binning_x",
        binning_y="Collect.params.binning_y",
        filter_name="Collect.params.filter_name",
        elevation="SitePosition.altitude_km",
        latitude="SitePosition.latitude_degrees",
        longitude="SitePosition.longitude_degrees",
        task_id="TaskInfo.task_id",
    )

    for key, expr in compat.items():
        with contextlib.suppress(Exception):
            context.set_value(key, context.eval(expr))

    # Fold legacy flat file_name/file_path keys into FileNameTemplate / FileInfo keywords.
    # A file_name is an input naming template; a file_path is an explicit output location.
    from sensorkit.data.filesys import FileInfo, FileNameTemplate

    file_name = context.pop("file_name", None)
    file_path = context.pop("file_path", None)

    if file_name is not None:
        logger.warning("The 'file_name' context key is deprecated. Use FileNameTemplate instead.")
        context.set(FileNameTemplate(template=file_name))

    if file_path is not None:
        logger.warning("The 'file_path' context key is deprecated. Use FileInfo instead.")
        context.set(FileInfo(path=pathlib.Path(file_path)))
