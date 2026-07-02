from typing import Literal

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.std.traits import TemperatureUnit


class AcquireData(sk.DeviceCommand):
    """Control instrument data acquisition.

    While nothing stops a device from implementing this command directly, typically commands
    derived from it, such as `CameraCapture`, which add fields and specialize the command
    semantics for a particular device archetype, are implemented instead.

    Four distinct actions are defined here, indicated by the `action` field:

    - *acquire* - Executes a single, self-contained acquisition cycle. This is used for
      one-shot data collection operations where the device performs a complete measurement
      and returns the result. The acquisition begins and ends within a single command execution.
      The command returns when the acquisition is complete.

    - *start* - Initiates a passive acquisition process. This begins an ongoing operation where
      the device continuously collects data until explicitly stopped by another `AcquireData`
      command (graceful), by an `Abort` command (forcible), or an error occurs. The context
      parameter allows implementation-specific configuration passthrough and provides metadata for
      data products in the case where the implementation outputs data products continuously. The
      command returns once the acquisition has successfully begun.

    - *continue* - Maintains a passive acquisition that was previously started. This action is
      used to update the acquisition context or to act as a keepalive signal, should those things
      be supported or required by the device implementation. If a passive collection is not
      underway, an error is raised. In all cases, this command should return immediately.

    - *stop* - Ends a passive acquisition that was previously started. If the device writes its
      output data product once at the end of acquisition, it may support the context parameter in
      this mode.

    Note that the storage or transfer of the acquired data is outside the scope of this command.
    Typically, the device implementing the command will also be the receiver of the acquired data
    and will write it to a configured `DataGraph`, but it is also possible for data to be handled
    differently, e.g., received by a different entity or even handled entirely out-of-band of
    SensorKit.

    Attributes:
        action: either "acquire", "start", "continue", or "stop"
        context: dictionary containing context Keywords
    """

    action: Literal["acquire", "start", "continue", "stop"]
    context: sk.Context | None


@sk.declare_keyword
class Binning(BaseModel):
    """Camera pixel binning configuration.

    Attributes:
        x: horizontal binning factor
        y: vertical binning factor
    """

    x: float
    y: float


@sk.declare_keyword
class CameraSensorSize(BaseModel):
    """Camera sensor physical dimensions in pixels.

    Attributes:
        x: sensor width in pixels
        y: sensor height in pixels
    """

    x: int
    y: int

    @property
    def width(self):
        return self.x

    @property
    def height(self):
        return self.y


@sk.declare_keyword
class CameraSensorTemperature(BaseModel):
    """Camera sensor temperature measurement or setpoint.

    Attributes:
        temperature: sensor temperature value
        units: temperature unit (Fahrenheit, Celsius, or Kelvin)
    """

    temperature: float
    units: TemperatureUnit


class ConfigureCameraCooler(sk.DeviceCommand):
    """Set the target temperature of a camera cooler.

    Attributes:
        enable: whether the cooler should be enabled
        setpoint: the target `CameraSensorTemperature`
    """

    enable: bool
    setpoint: CameraSensorTemperature


class ConfigureCameraSensor(sk.DeviceCommand):
    """Configure the camera sensor and other settings."""

    binning: Binning | None = None
    bias: float | None = None
    gain: float | None = None


class CameraCapture(AcquireData):
    """Execute a single camera exposure.

    Attributes:
        action: action type for the command (always "acquire")
        integration_time: exposure time in seconds
    """

    action: Literal["acquire"] = "acquire"
    integration_time: float


StandardCamera = sk.declare_archetype(
    "camera",
    required_commands=(CameraCapture,),
    optional_commands=(ConfigureCameraCooler,),
)
"""A standard camera device that can capture images with configurable integration time.

This archetype defines the interface for camera devices capable of image acquisition
with specified exposure durations. It supports optional temperature control for
cooled camera sensors.
"""


@sk.declare_keyword
class RotatorPosition(BaseModel):
    """Current angular position of the rotator."""
    position: float


class ChangeRotatorPosition(sk.DeviceCommand):
    """Move the rotator to a specified orientation.

    Attributes:
        position: target position value for the rotator
    """

    position: float


StandardRotator = sk.declare_archetype(
    "rotator",
    required_commands=(ChangeRotatorPosition,),
)
"""A device that can rotate an optical element to a specified position.

This archetype defines the interface for rotator devices that control the orientation of optical
components such as cameras instrument adapters within an optical system.
"""
