from __future__ import annotations

from typing import Literal

from pydantic import Field

from sensorkit.core.device import DeviceCommand
from sensorkit.std.instrument import CameraCapture


class SetROI(DeviceCommand):
    command_id: Literal["SetROI"] = "SetROI"
    start_x: int
    start_y: int
    num_x: int
    num_y: int


class ResetROI(DeviceCommand):
    command_id: Literal["ResetROI"] = "ResetROI"


class SetGain(DeviceCommand):
    command_id: Literal["SetGain"] = "SetGain"
    value: int | float


class SetOffset(DeviceCommand):
    command_id: Literal["SetOffset"] = "SetOffset"
    value: int | float


class SetReadoutMode(DeviceCommand):
    command_id: Literal["SetReadoutMode"] = "SetReadoutMode"
    mode: int | str


class CoolerOn(DeviceCommand):
    command_id: Literal["CoolerOn"] = "CoolerOn"
    on: bool = Field(default=True)


class CameraSequenceCapture(CameraCapture):
    command_id: Literal["CameraSequenceCapture"] = "CameraSequenceCapture"
    integration_time: float
    count: int
    interval_s: float = Field(default=0.0)
    light: bool = Field(default=True)
