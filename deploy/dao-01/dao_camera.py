"""SensorKit camera module for the Teledyne Princeton Instruments Cosmos-8k.

Drives the camera directly through the PICam C library via ctypes.

  - One producer-side copy: PICam circular buffer -> bytearray. The numpy
    array handed to the DataGraph is a zero-copy view over that buffer.
  - ExposureTime is only set + committed when it changes. A collect arrives
    as N serial CameraCapture commands (one per frame); at fixed exposure
    only the first pays the Set+Commit round trip.
  - The DataGraph write runs as a background task, so the CameraCapture
    command returns (and the mount can slew) while the file is written.

All PICam calls run on a single dedicated worker thread: the manual documents
no thread-safety guarantees, so calls are serialized per handle. The one
exception is Picam_StopAcquisition on abort -- it "returns immediately" by
design while the worker blocks in Picam_WaitForAcquisitionUpdate, and the
Wait loop then runs down until status.running goes false (PICam manual 6.4.3
/ 6.4.5).

GPS timing: the COSMOS timestamps both the start and end of every exposure in
hardware (frame metadata, enabled via PicamTimeStampsMask). Until the GPS
data/context model is defined, those instants propagate through the capture
*context* only, as the stub `ExposureTiming` keyword below -- see the STUB
comment there before extending.

Config section (one camera per service process; PICam library init is
process-global):

    dao_camera:
      - id: Cosmos-8k
        serial_number: "X090002624"
        temperature: -25
        readout_mode: 0     # index into readout_modes (default table below)
        binning: 1

To run standalone on the camera host (Windows, next to the PICam runtime):

    SENSORKIT_IMPORTS=dao_camera sensorkit service run Cosmos-8k dao_camera.py

Under a unified config, list this file's path in `sensorkit.imports` (and, as
with huntsman_dome.py, mount it into every container that parses the config).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from ctypes import (
    CDLL,
    POINTER,
    Structure,
    byref,
    c_char,
    c_char_p,
    c_double,
    c_int,
    c_int64,
    c_ubyte,
    c_void_p,
    pointer,
)
from datetime import UTC, datetime, timedelta
from typing import Literal

import numpy as np
from loguru import logger
from pydantic import AwareDatetime, BaseModel, Field

import sensorkit.api as sk
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ArrayInfo, ImageInfo
from sensorkit.std import (
    Binning,
    CameraCapture,
    CameraSensorSize,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
    Connect,
    Connected,
    Disconnect,
    ExposureInfo,
    FrameType,
    Stop,
    TemperatureUnit,
)
from sensorkit.std.collect import Collect
from sensorkit.std.instrument import StandardCamera

# ── PICam FFI ────────────────────────────────────────────────────────
#
# Parameter IDs are encoded as (constraint << 24) | (value_type << 16) |
# ordinal, straight out of picam.h. Only the parameters this module touches
# are listed.

_VALUE_TYPE = {"Integer": 1, "FloatingPoint": 2, "Boolean": 3, "Enumeration": 4, "Rois": 5, "LargeInteger": 6}
_CONSTRAINT = {"None": 1, "Range": 2, "Collection": 3, "Rois": 4}


def _pi(value_type: str, constraint: str, ordinal: int) -> int:
    return (_CONSTRAINT[constraint] << 24) | (_VALUE_TYPE[value_type] << 16) | ordinal


_P = {
    "ExposureTime": _pi("FloatingPoint", "Range", 23),  # milliseconds
    "AdcAnalogGain": _pi("Enumeration", "Collection", 35),
    "AdcQuality": _pi("Enumeration", "Collection", 36),
    "AdcBitDepth": _pi("Integer", "Collection", 34),
    "PixelFormat": _pi("Enumeration", "Collection", 41),  # 1=Mono16Bit, 2=Mono32Bit
    "ReadoutControlMode": _pi("Enumeration", "Collection", 26),
    "ReadoutCount": _pi("LargeInteger", "Range", 40),
    "ReadoutStride": _pi("Integer", "None", 45),  # bytes
    "ReadoutTimeCalculation": _pi("FloatingPoint", "None", 27),  # milliseconds
    "Rois": _pi("Rois", "Rois", 37),
    "SensorActiveWidth": _pi("Integer", "None", 59),
    "SensorActiveHeight": _pi("Integer", "None", 60),
    "PixelWidth": _pi("FloatingPoint", "None", 9),  # microns
    "PixelHeight": _pi("FloatingPoint", "None", 10),
    "TimeStamps": _pi("Enumeration", "Collection", 68),  # mask: 1=ExposureStarted, 2=ExposureEnded
    "TimeStampResolution": _pi("LargeInteger", "Collection", 69),  # ticks/second
    "SensorTemperatureReading": _pi("FloatingPoint", "None", 15),  # deg C
    "SensorTemperatureSetPoint": _pi("FloatingPoint", "Range", 14),
    "SensorTemperatureStatus": _pi("Enumeration", "None", 16),  # 1=unlocked, 2=locked, 3=faulted
    "EnableSensorWindowHeater": _pi("Boolean", "Collection", 127),
}
_P_NAMES = {value: name for name, value in _P.items()}

_ERRORS = {
    0: "None", 1: "LibraryNotInitialized", 2: "InvalidParameterValue",
    3: "UnexpectedNullPointer", 4: "UnexpectedError", 5: "LibraryAlreadyInitialized",
    6: "InvalidDemoModel", 7: "CameraAlreadyOpened", 8: "InvalidCameraID",
    9: "InvalidHandle", 10: "ParameterValueIsReadOnly", 11: "ParameterHasInvalidValueType",
    12: "ParameterDoesNotExist", 13: "ParameterHasInvalidConstraintType",
    14: "ParameterValueIsIrrelevant", 15: "DeviceCommunicationFailed",
    16: "InvalidEnumeratedType", 17: "EnumerationValueNotDefined",
    18: "NotDiscoveringCameras", 19: "AlreadyDiscoveringCameras",
    20: "AcquisitionInProgress", 21: "InvalidDemoSerialNumber",
    22: "DemoAlreadyConnected", 23: "DeviceDisconnected", 24: "DeviceOpenElsewhere",
    25: "ParameterIsNotOnlineable", 26: "ParameterIsNotReadable",
    27: "AcquisitionNotInProgress", 28: "InvalidParameterValues",
    29: "ParametersNotCommitted", 30: "InvalidAcquisitionBuffer",
    31: "InsufficientMemory", 32: "TimeOutOccurred",
    33: "AcquisitionUpdatedHandlerRegistered", 34: "NoCamerasAvailable",
}
_TIMEOUT_OCCURRED = 32


class PicamError(Exception):
    def __init__(self, code: int, operation: str = ""):
        self.code = code
        name = _ERRORS.get(code, f"Unknown({code})")
        super().__init__(f"{operation or 'PICam'}: PicamError_{name} ({code})")


class PicamCameraID(Structure):
    _fields_ = [
        ("model", c_int),
        ("computer_interface", c_int),
        ("sensor_name", c_char * 64),
        ("serial_number", c_char * 64),
    ]


class PicamRoi(Structure):
    _fields_ = [
        ("x", c_int), ("width", c_int), ("x_binning", c_int),
        ("y", c_int), ("height", c_int), ("y_binning", c_int),
    ]


class PicamRois(Structure):
    _fields_ = [("roi_array", POINTER(PicamRoi)), ("roi_count", c_int)]


# NOTE: pibln in the PICam headers is a 4-byte int, not a 1-byte C bool.
class PicamRangeConstraint(Structure):
    _fields_ = [
        ("scope", c_int), ("severity", c_int), ("empty_set", c_int),
        ("minimum", c_double), ("maximum", c_double), ("increment", c_double),
        ("excluded_values_array", POINTER(c_double)), ("excluded_values_count", c_int),
        ("outlying_values_array", POINTER(c_double)), ("outlying_values_count", c_int),
    ]


class PicamRoisConstraint(Structure):
    _fields_ = [
        ("scope", c_int), ("severity", c_int), ("empty_set", c_int),
        ("rules", c_int), ("maximum_roi_count", c_int),
        ("x_constraint", PicamRangeConstraint), ("width_constraint", PicamRangeConstraint),
        ("x_binning_limits_array", POINTER(c_int)), ("x_binning_limits_count", c_int),
        ("y_constraint", PicamRangeConstraint), ("height_constraint", PicamRangeConstraint),
        ("y_binning_limits_array", POINTER(c_int)), ("y_binning_limits_count", c_int),
    ]


class PicamCollectionConstraint(Structure):
    _fields_ = [
        ("scope", c_int), ("severity", c_int),
        ("values_array", POINTER(c_double)), ("values_count", c_int),
    ]


class PicamAvailableData(Structure):
    _fields_ = [("initial_readout", c_void_p), ("readout_count", c_int64)]


class PicamAcquisitionStatus(Structure):
    _fields_ = [("running", c_int), ("errors", c_int), ("readout_rate", c_double)]


_PH = c_void_p


def _declare_argtypes(lib):
    # Without explicit argtypes, ctypes on 64-bit Windows truncates pointer
    # arguments, causing intermittent segfaults inside the DLL.
    signatures = {
        "Picam_InitializeLibrary": [],
        "Picam_UninitializeLibrary": [],
        "Picam_GetVersion": [POINTER(c_int)] * 4,
        "Picam_GetAvailableCameraIDs": [POINTER(POINTER(PicamCameraID)), POINTER(c_int)],
        "Picam_DestroyCameraIDs": [POINTER(PicamCameraID)],
        "Picam_ConnectDemoCamera": [c_int, c_char_p, POINTER(PicamCameraID)],
        "Picam_OpenCamera": [POINTER(PicamCameraID), POINTER(_PH)],
        "Picam_CloseCamera": [_PH],
        "Picam_GetParameterIntegerValue": [_PH, c_int, POINTER(c_int)],
        "Picam_SetParameterIntegerValue": [_PH, c_int, c_int],
        "Picam_GetParameterFloatingPointValue": [_PH, c_int, POINTER(c_double)],
        "Picam_SetParameterFloatingPointValue": [_PH, c_int, c_double],
        "Picam_ReadParameterIntegerValue": [_PH, c_int, POINTER(c_int)],
        "Picam_ReadParameterFloatingPointValue": [_PH, c_int, POINTER(c_double)],
        "Picam_GetParameterLargeIntegerValue": [_PH, c_int, POINTER(c_int64)],
        "Picam_SetParameterLargeIntegerValue": [_PH, c_int, c_int64],
        "Picam_SetParameterRoisValue": [_PH, c_int, POINTER(PicamRois)],
        "Picam_GetParameterRangeConstraint": [_PH, c_int, c_int, POINTER(POINTER(PicamRangeConstraint))],
        "Picam_DestroyRangeConstraints": [POINTER(PicamRangeConstraint)],
        "Picam_GetParameterRoisConstraint": [_PH, c_int, c_int, POINTER(POINTER(PicamRoisConstraint))],
        "Picam_DestroyRoisConstraints": [POINTER(PicamRoisConstraint)],
        "Picam_GetParameterCollectionConstraint": [_PH, c_int, c_int, POINTER(POINTER(PicamCollectionConstraint))],
        "Picam_DestroyCollectionConstraints": [POINTER(PicamCollectionConstraint)],
        "Picam_CommitParameters": [_PH, POINTER(POINTER(c_int)), POINTER(c_int)],
        "Picam_DestroyParameters": [POINTER(c_int)],
        "Picam_StartAcquisition": [_PH],
        "Picam_StopAcquisition": [_PH],
        "Picam_WaitForAcquisitionUpdate": [_PH, c_int, POINTER(PicamAvailableData), POINTER(PicamAcquisitionStatus)],
    }
    for name, argtypes in signatures.items():
        fn = getattr(lib, name)
        fn.argtypes = argtypes
        fn.restype = c_int


def _call(fn, *args, op: str = ""):
    error = fn(*args)
    if error:
        raise PicamError(error, op or fn.__name__)


def _get_i(lib, handle, param: int) -> int:
    value = c_int()
    _call(lib.Picam_GetParameterIntegerValue, handle, param, byref(value))
    return value.value


def _get_f(lib, handle, param: int) -> float:
    value = c_double()
    _call(lib.Picam_GetParameterFloatingPointValue, handle, param, byref(value))
    return value.value


def _get_l(lib, handle, param: int) -> int:
    value = c_int64()
    _call(lib.Picam_GetParameterLargeIntegerValue, handle, param, byref(value))
    return value.value


def _collection(lib, handle, param: int) -> list[int] | None:
    """Capable values for a collection-constrained parameter (None if unavailable).

    Camera-wide capabilities only -- whether a *combination* of values is
    valid is known only to Picam_CommitParameters."""
    constraint = POINTER(PicamCollectionConstraint)()
    if lib.Picam_GetParameterCollectionConstraint(handle, param, 1, byref(constraint)):  # 1 = Capable
        return None
    values = [int(constraint.contents.values_array[i]) for i in range(constraint.contents.values_count)]
    lib.Picam_DestroyCollectionConstraints(constraint)
    return values


def _commit(lib, handle):
    failed = POINTER(c_int)()
    count = c_int()
    error = lib.Picam_CommitParameters(handle, byref(failed), byref(count))
    bad = [_P_NAMES.get(failed[i], failed[i]) for i in range(count.value)] if count.value else []
    if failed:
        lib.Picam_DestroyParameters(failed)
    if error:
        raise PicamError(error, f"CommitParameters{f' (failed parameters {bad})' if bad else ''}")


# ── Keywords ─────────────────────────────────────────────────────────


@sk.declare_keyword
class DaoCameraStatus(BaseModel):
    """PICam camera telemetry."""

    connected: bool = False
    camera_state: str = "idle"  # "idle" | "exposing"
    sensor_name: str | None = None
    serial_number: str | None = None
    readout_mode: str | None = None
    bin_x: int = 1
    bin_y: int = 1
    temperature: float | None = None
    set_temperature: float | None = None
    temperature_status: str | None = None  # "unlocked" | "locked" | "faulted"
    readout_time_ms: float | None = None
    exposure_min: float | None = None
    exposure_max: float | None = None


# STUB -- GPS timing data model pending.
#
# The COSMOS hardware timestamps the start and end of every exposure (frame
# metadata, TimeStampResolution ticks/sec, relative to acquisition start).
# Until the GPS data/context model is defined with Erik, those instants are
# rebased onto a host-clock anchor taken just before StartAcquisition and
# propagated through the capture *context* only -- nothing is published on
# the device stream and no FITS keywords are claimed here. Replace this model
# (and its context.set in camera_capture) when the data model lands; `source`
# becomes "gps" once the camera clock is GPS-disciplined.
@sk.declare_keyword
class ExposureTiming(BaseModel):
    """Hardware-timestamped exposure boundaries (STUB, see above)."""

    started: AwareDatetime | None = None
    ended: AwareDatetime | None = None
    source: Literal["host_clock", "gps"] = "host_clock"


# ── Config ───────────────────────────────────────────────────────────


class DaoCameraReadoutMode(BaseModel):
    """One camera readout mode: label plus the PICam parameter values
    [AdcAnalogGain, AdcQuality, AdcBitDepth, PixelFormat, ReadoutControlMode];
    -1 means not applicable for this mode (skipped)."""

    label: str
    values: list[int] = Field(min_length=5, max_length=5)


# The Cosmos-8k mode table. Two former rows (16bit-Low-Global_Shutter,
# 14bit-Low-Global_Shutter) are omitted: camera s/n X120003324 rejects those
# combinations at commit (analog gain Low is not allowed with global shutter,
# and 16-bit is rolling-only). Indices 0 and 5 are hardware-verified.
_COSMOS_8K_MODES = [
    DaoCameraReadoutMode(label="16bit-High-Rolling_Shutter", values=[3, 1, 16, 1, 8]),  # CMS
    DaoCameraReadoutMode(label="18bit-HDR-Rolling_Shutter", values=[-1, 5, 18, 2, 8]),
    DaoCameraReadoutMode(label="16bit-Low-Rolling_Shutter", values=[1, 1, 16, 1, 8]),
    DaoCameraReadoutMode(label="14bit-Low-Rolling_Shutter", values=[1, 4, 14, 1, 8]),
    DaoCameraReadoutMode(label="14bit-High-Rolling_Shutter", values=[3, 4, 14, 1, 8]),
    DaoCameraReadoutMode(label="14bit-High-Global_Shutter", values=[3, 4, 14, 1, 10]),
]


class DaoCameraConfig(BaseModel):
    library: str = "C:/Program Files/Common Files/Princeton Instruments/Picam/Runtime/Picam.dll"
    dll_directories: list[str] = [
        "C:/Program Files/Common Files/Princeton Instruments/Picam/Runtime",
        "C:/Program Files/Common Files/Pleora/eBUS SDK",
    ]
    serial_number: str = ""  # empty -> first camera found
    demo_model: int | None = None  # PicamModel id (e.g. 2807) -> connect a demo camera
    temperature: float = -25.0  # cooler setpoint, deg C
    readout_mode: int = 0  # index into readout_modes
    readout_modes: list[DaoCameraReadoutMode] = _COSMOS_8K_MODES
    binning: int = 1  # symmetric startup binning
    warmup_frame: bool = True  # discard one frame at open (first-acquisition calibration)
    window_heater: bool = True
    open_timeout: float = 90.0  # camera discovery can be slow after power-up
    exposure_timeout: float = 60.0  # capture margin beyond exposure + readout
    status_frequency: float = 1.0

    def create_device(self) -> "DaoCamera":
        return DaoCamera(self)


# Registers only when this loose file is imported *by name* (path entry in
# `sensorkit.imports`, or SENSORKIT_IMPORTS=dao_camera). service_path=__name__
# makes `sensorkit go` launch the entrypoint; do NOT also add a `services:`
# entry for it, or the double import raises "already registered".
sk.declare_config_section(
    "dao_camera",
    list[DaoCameraConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


# ── Device ───────────────────────────────────────────────────────────


@sk.declare_device(type=StandardCamera)
class DaoCamera:
    """COSMOS-8k camera on PICam.

    Async surface (command handlers, status loop) runs on the event loop;
    every PICam call is pushed onto `self._exec`, a one-thread executor, so
    the C library only ever sees a single calling thread (plus the documented
    cross-thread StopAcquisition on abort).
    """

    def __init__(self, config: DaoCameraConfig):
        self.config = config
        self._lib = None
        self._handle = None
        self._exec: ThreadPoolExecutor | None = None
        self._capture_lock = asyncio.Lock()
        self._capturing = False
        self._capture_gen = 0  # correlates aborts with the capture they target
        self._aborted = False
        self._status_task: asyncio.Task | None = None
        self._data_tasks: set[asyncio.Task] = set()

        # Populated by _open/_apply_binning on the worker thread.
        self._sensor_name: str | None = None
        self._serial: str | None = None
        self._sensor_w = 0
        self._sensor_h = 0
        self._pixel_w: float | None = None  # microns, unbinned
        self._pixel_h: float | None = None
        self._exp_min: float | None = None  # seconds
        self._exp_max: float | None = None
        self._binnings: list[int] = [1]
        self._bin = 1
        self._width = 0  # binned frame geometry
        self._height = 0
        self._dtype = np.uint16
        self._readout_stride = 0
        self._readout_ms: float | None = None
        self._ts_res = 1e7  # timestamp ticks/second
        self._mode_label: str | None = None
        self._max_adu: int | None = None
        self._committed_exposure_ms: float | None = None
        self._last_temp: float | None = None
        self._temp_status: int | None = None
        self._set_temp: float | None = None

    async def _run(self, fn, *args):
        """Run a blocking PICam call on the dedicated worker thread."""
        return await asyncio.get_running_loop().run_in_executor(self._exec, fn, *args)

    # ── Lifecycle ──

    @sk.on_attach
    async def entity_init(self):
        self._exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="picam")
        try:
            await self._run(self._open)
        except Exception as e:
            logger.warning(f"PICam open failed at attach: {e} — will retry on demand")
        await sk.device().publish(Connected(is_connected=self._handle is not None))
        if self._handle is not None:
            await sk.device().publish(CameraSensorSize(x=self._sensor_w, y=self._sensor_h))
        self._status_task = asyncio.create_task(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        if self._status_task is not None:
            self._status_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._status_task
        # Let in-flight DataGraph writes finish before the backend goes away.
        if self._data_tasks:
            await asyncio.gather(*self._data_tasks, return_exceptions=True)
        if self._capturing and self._handle is not None:
            # Unblock the worker thread if it is sitting in WaitForAcquisitionUpdate.
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._lib.Picam_StopAcquisition, self._handle)
        if self._handle is not None:
            try:
                # Bounded: if the worker is wedged inside the DLL, abandon the
                # handle rather than hang the detach forever.
                async with asyncio.timeout(30.0):
                    await self._run(self._close)
            except TimeoutError:
                logger.warning("PICam worker unresponsive; abandoning camera handle")
            except Exception as e:
                logger.warning(f"PICam close failed: {e}")
        # A capture that was in flight during the first drain may have queued
        # its delivery after the gather; drain again so no frame is dropped.
        if self._data_tasks:
            await asyncio.gather(*self._data_tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await sk.device().publish(Connected(is_connected=False))
        self._exec.shutdown(wait=False)

    async def require_connected(self):
        if self._handle is None:
            logger.warning("camera not open, attempting to open")
            await self.camera_connect(Connect())

    # ── Command handlers ──

    @sk.command_handler
    async def camera_connect(self, cmd: Connect):
        if self._handle is None:
            async with asyncio.timeout(self.config.open_timeout + 30.0):
                await self._run(self._open)
            await sk.device().publish(CameraSensorSize(x=self._sensor_w, y=self._sensor_h))
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def camera_disconnect(self, cmd: Disconnect):
        if self._handle is not None:
            if self._capturing:
                # Unblock the worker before queueing _close behind it.
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self._lib.Picam_StopAcquisition, self._handle)
            async with asyncio.timeout(30.0):
                await self._run(self._close)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def camera_stop(self, cmd: Stop):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        if not self._capturing or self._handle is None:
            return
        logger.debug("aborting in-flight capture")
        self._aborted = True
        generation = self._capture_gen

        def _stop():
            # Re-check at execution time: the targeted capture (or a
            # disconnect) may have finished while this call was queued, and a
            # stale stop must not kill the next exposure.
            if self._capturing and self._capture_gen == generation and self._handle is not None:
                return self._lib.Picam_StopAcquisition(self._handle)
            return 0

        error = await asyncio.to_thread(_stop)
        if error and error != 27:  # AcquisitionNotInProgress: it just finished
            logger.warning(f"StopAcquisition failed: PicamError {error}")

    @sk.command_handler
    async def camera_set_temperature(self, cmd: ConfigureCameraCooler):
        await self.require_connected()
        setpoint = cmd.setpoint.temperature
        if cmd.setpoint.units == TemperatureUnit.FAHRENHEIT:
            setpoint = (setpoint - 32.0) * 5.0 / 9.0
        elif cmd.setpoint.units == TemperatureUnit.KELVIN:
            setpoint = setpoint - 273.15
        if not cmd.enable:
            logger.warning("PICam sensor cooling cannot be disabled; leaving setpoint unchanged")
            return
        def _apply():
            _call(self._lib.Picam_SetParameterFloatingPointValue, self._handle,
                  _P["SensorTemperatureSetPoint"], c_double(setpoint), op="Set_SensorTemperatureSetPoint")
            _commit(self._lib, self._handle)

        # Serialize with captures: committing mid-acquisition is undocumented,
        # and waiting on the lock keeps the worker free for status reads.
        async with self._capture_lock:
            await self._run(_apply)
        self._set_temp = setpoint
        logger.info(f"cooler setpoint -> {setpoint:.1f} °C")

    @sk.command_handler
    async def camera_set_binning(self, cmd: ConfigureCameraSensor):
        if cmd.gain is not None or cmd.bias is not None:
            logger.warning("gain/bias are fixed by the readout mode on this camera; ignoring")
        if cmd.binning is None:
            return
        await self.require_connected()
        bin_x, bin_y = int(cmd.binning.x), int(cmd.binning.y)
        if bin_x != bin_y:
            logger.warning(f"asymmetric binning unsupported; using {bin_x}x{bin_x} (requested {bin_x}x{bin_y})")
        if bin_x not in self._binnings:
            raise ValueError(f"binning {bin_x} not in supported binnings {self._binnings}")
        if bin_x != self._bin:
            async with self._capture_lock:  # never reconfigure mid-exposure
                await self._run(self._apply_binning, bin_x)
            logger.info(f"binning -> {bin_x}x{bin_x} ({self._width}x{self._height})")
        await sk.device().publish(Binning(x=self._bin, y=self._bin))

    # TODO: device-side multi-frame via AcquireData(action="start"/"stop") --
    # PICam maps cleanly (ReadoutCount=0, one StartAcquisition, the Wait loop
    # streams every readout with no inter-frame gap). Measured on the real
    # camera: free-running delivery sustains the hardware cadence in every
    # mode (0.82 / 2.9 / 18.5 fps for CMS / HDR / 14-bit global full frame),
    # so streaming through this FFI reaches LightField-class rates; the
    # single-shot path below carries ~0.2 s fixed overhead per frame instead.
    # Needs: the
    # PicamAdvanced_SetAcquisitionBuffer FFI (a user ring buffer is required
    # for ReadoutCount=0), a device-side frame counter stamped into a copied
    # context per readout (FileNameTemplate), and per-frame pointing metadata
    # (ContextSubscription to the mount, as sdasim does). Held pending the
    # data-model discussion; the standard collect loop does not send
    # start/stop today.
    @sk.command_handler
    async def camera_capture(self, cmd: CameraCapture):
        await self.require_connected()
        exposure = float(cmd.integration_time)
        if (self._exp_min is not None and exposure < self._exp_min) or (
            self._exp_max is not None and exposure > self._exp_max
        ):
            raise ValueError(
                f"integration_time {exposure}s outside camera range "
                f"[{self._exp_min}, {self._exp_max}]s"
            )

        async with self._capture_lock:
            self._aborted = False
            self._capture_gen += 1
            self._capturing = True
            logger.info(f"starting {exposure:.3f}s capture")
            try:
                # The Wait loop enforces its own (tighter) timeout, including a
                # 10s post-stop rundown; this is a backstop in case the worker
                # thread wedges inside the DLL.
                backstop = exposure + (self._readout_ms or 0.0) / 1000.0 + self.config.exposure_timeout + 45.0
                async with asyncio.timeout(backstop):
                    result = await self._run(self._acquire, exposure)
            except (TimeoutError, asyncio.CancelledError):
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self._lib.Picam_StopAcquisition, self._handle)
                raise
            finally:
                self._capturing = False

        if result is None:
            raise RuntimeError("capture aborted" if self._aborted else "acquisition ended with no readout")
        # Geometry comes from the result, not from self: a queued binning
        # change may already have mutated the instance fields by now.
        payload, image, binning, anchor, started, ended, actual_exposure = result
        logger.debug(
            f"captured {image.shape[1]}x{image.shape[0]} {image.dtype} frame, "
            f"exposure {started} -> {ended}"
        )

        context = cmd.context if cmd.context is not None else sk.Context()
        # Frame type rides in on the Collect keyword (flats, darks, ...);
        # LIGHT when absent.
        collect = context.get(Collect)
        frame_type = (collect.params.frame_type if collect else None) or FrameType.LIGHT
        context.set(
            ImageInfo(
                array=ArrayInfo.from_array(image),
                binning=(binning, binning),
                pixel_size=(self._pixel_w * binning, self._pixel_h * binning)
                if self._pixel_w and self._pixel_h
                else None,
            ),
            ExposureInfo(
                date_obs=started or anchor,
                exposure_time=actual_exposure,
                instrument=str(sk.device().entity),
                image_type=frame_type,
                readout_mode=self._mode_label,
                ccd_temperature=self._last_temp,
                set_temperature=self._set_temp,
                max_adu=self._max_adu,
            ),
            # STUB: context-only GPS timing propagation (see ExposureTiming).
            ExposureTiming(started=started, ended=ended, source="host_clock"),
        )
        if not context.get(FileNameTemplate):
            context.set(FileNameTemplate(template=f"{uuid.uuid1()}.fits"))

        # Deliver in the background: the command returns (and the controller
        # can start the next slew/frame) while the file writes.
        task = asyncio.create_task(self._deliver(payload, context))
        self._data_tasks.add(task)
        task.add_done_callback(self._data_tasks.discard)

    async def _deliver(self, payload: bytearray, context: sk.Context):
        try:
            if graph := await sk.device().data_graph():
                writer = graph.app_source().produce(context)
                writer.write(payload)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                logger.debug(f"wrote {len(payload)} bytes to DataGraph")
            else:
                logger.warning("No DataGraph defined; discarding frame")
        except Exception:
            logger.exception("Failed to deliver frame to DataGraph")

    # ── Status ──

    async def status_publish(self):
        while True:
            try:
                device = sk.device()
                is_open = self._handle is not None
                await device.publish(Connected(is_connected=is_open))
                if is_open:
                    if not self._capturing:
                        # SensorTemperature* are hardware reads; skip them while
                        # exposing rather than queue behind the acquisition.
                        try:
                            self._last_temp, self._temp_status = await self._run(self._read_temperature)
                        except Exception as e:
                            logger.debug(f"temperature read failed: {e}")
                    await device.publish(Binning(x=self._bin, y=self._bin))
                    await device.publish(CameraSensorSize(x=self._sensor_w, y=self._sensor_h))
                    if self._last_temp is not None:
                        await device.publish(
                            CameraSensorTemperature(
                                temperature=self._last_temp, units=TemperatureUnit.CELSIUS
                            )
                        )
                await device.publish(
                    DaoCameraStatus(
                        connected=is_open,
                        camera_state="exposing" if self._capturing else "idle",
                        sensor_name=self._sensor_name,
                        serial_number=self._serial,
                        readout_mode=self._mode_label,
                        bin_x=self._bin,
                        bin_y=self._bin,
                        temperature=self._last_temp,
                        set_temperature=self._set_temp,
                        temperature_status={1: "unlocked", 2: "locked", 3: "faulted"}.get(
                            self._temp_status
                        ),
                        readout_time_ms=self._readout_ms,
                        exposure_min=self._exp_min,
                        exposure_max=self._exp_max,
                    )
                )
            except Exception as e:
                logger.exception(f"Error in camera status publish: {e}")
            await asyncio.sleep(self.config.status_frequency)

    # ── Blocking PICam side (worker thread only) ──

    def _open(self):
        if self._handle is not None:  # concurrent opens serialize here; second is a no-op
            return
        cfg = self.config
        if self._lib is None:
            if sys.platform.startswith("win"):
                for directory in cfg.dll_directories:
                    if os.path.isdir(directory):
                        os.add_dll_directory(directory)
                from ctypes import WinDLL

                lib = WinDLL(cfg.library)
            else:
                lib = CDLL(cfg.library)
            _declare_argtypes(lib)
            self._lib = lib
        lib = self._lib

        error = lib.Picam_InitializeLibrary()
        if error not in (0, 5):  # 5 = LibraryAlreadyInitialized
            raise PicamError(error, "InitializeLibrary")
        major, minor, distribution, release = c_int(), c_int(), c_int(), c_int()
        if lib.Picam_GetVersion(byref(major), byref(minor), byref(distribution), byref(release)) == 0:
            logger.debug(f"PICam {major.value}.{minor.value}.{distribution.value} release {release.value}")

        if cfg.demo_model is not None:
            camera_id = PicamCameraID()
            error = lib.Picam_ConnectDemoCamera(
                cfg.demo_model, (cfg.serial_number or "DEMO001").encode(), byref(camera_id)
            )
            if error not in (0, 22):  # 22 = DemoAlreadyConnected (left over from a failed open)
                raise PicamError(error, "ConnectDemoCamera")

        # USB/CXP enumeration can take a while after camera power-up; demo
        # cameras show up in the same list once connected above.
        handle = c_void_p()
        deadline = time.monotonic() + cfg.open_timeout
        while True:
            ids = POINTER(PicamCameraID)()
            count = c_int()
            _call(lib.Picam_GetAvailableCameraIDs, byref(ids), byref(count), op="GetAvailableCameraIDs")
            index = next(
                (i for i in range(count.value)
                 if ids[i].serial_number.decode(errors="replace") == cfg.serial_number),
                0 if count.value and not cfg.serial_number else None,
            )
            if index is not None:
                _call(lib.Picam_OpenCamera, byref(ids[index]), byref(handle), op="OpenCamera")
                self._sensor_name = ids[index].sensor_name.decode(errors="replace").strip()
                self._serial = ids[index].serial_number.decode(errors="replace").strip()
                lib.Picam_DestroyCameraIDs(ids)
                break
            lib.Picam_DestroyCameraIDs(ids)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"no camera{f' with serial {cfg.serial_number}' if cfg.serial_number else 's'} "
                    f"found within {cfg.open_timeout:.0f}s — check the connection and that no "
                    f"other application has the camera open"
                )
            time.sleep(2.0)
        self._handle = handle

        try:
            for name in ("AdcAnalogGain", "AdcQuality", "AdcBitDepth", "PixelFormat", "ReadoutControlMode"):
                logger.debug(f"capable {name}: {_collection(lib, handle, _P[name])}")

            self._sensor_w = _get_i(lib, handle, _P["SensorActiveWidth"])
            self._sensor_h = _get_i(lib, handle, _P["SensorActiveHeight"])
            try:
                self._pixel_w = _get_f(lib, handle, _P["PixelWidth"])
                self._pixel_h = _get_f(lib, handle, _P["PixelHeight"])
            except PicamError:
                self._pixel_w = self._pixel_h = None

            constraint = POINTER(PicamRangeConstraint)()
            _call(lib.Picam_GetParameterRangeConstraint, handle, _P["ExposureTime"], 1,  # 1 = Capable
                  byref(constraint), op="ExposureTimeRangeConstraint")
            self._exp_min = constraint.contents.minimum / 1000.0
            self._exp_max = constraint.contents.maximum / 1000.0
            lib.Picam_DestroyRangeConstraints(constraint)

            rois_constraint = POINTER(PicamRoisConstraint)()
            _call(lib.Picam_GetParameterRoisConstraint, handle, _P["Rois"], 1,
                  byref(rois_constraint), op="RoisConstraint")
            limits = rois_constraint.contents
            self._binnings = sorted(
                {limits.x_binning_limits_array[i] for i in range(limits.x_binning_limits_count)}
            ) or [1]
            lib.Picam_DestroyRoisConstraints(rois_constraint)

            self._ts_res = float(_get_l(lib, handle, _P["TimeStampResolution"])) or 1e7

            # Defaults: readout mode, single-readout exposures, hardware
            # timestamps on both exposure edges, heater, cooler setpoint.
            # All committed together by _apply_binning below.
            self._apply_readout_mode(cfg.readout_mode)
            _call(lib.Picam_SetParameterIntegerValue, handle, _P["TimeStamps"], 0x1 | 0x2, op="Set_TimeStamps")
            _call(lib.Picam_SetParameterLargeIntegerValue, handle, _P["ReadoutCount"], c_int64(1), op="Set_ReadoutCount")
            if cfg.window_heater:
                try:
                    _call(lib.Picam_SetParameterIntegerValue, handle, _P["EnableSensorWindowHeater"], 1)
                except PicamError as e:
                    logger.warning(f"window heater unavailable: {e}")
            _call(lib.Picam_SetParameterFloatingPointValue, handle,
                  _P["SensorTemperatureSetPoint"], c_double(cfg.temperature), op="Set_SensorTemperatureSetPoint")
            self._set_temp = cfg.temperature

            self._apply_binning(cfg.binning if cfg.binning in self._binnings else 1)
            self._committed_exposure_ms = None

            if cfg.warmup_frame:
                # The first acquisition after opening runs ~20 s of on-camera
                # calibration before PICam delivers the readout (measured on
                # the Cosmos-8k; subsequent frames are clean, including after
                # ExposureTime and Rois/binning commits). Burn it on a
                # discarded frame here so science frames are fast and their
                # hardware timestamps are trustworthy.
                logger.info("acquiring warm-up frame")
                self._acquire(0.0)
        except Exception:
            self._handle = None
            lib.Picam_CloseCamera(handle)
            raise

        logger.info(
            f"connected to {self._sensor_name} s/n {self._serial}: "
            f"{self._sensor_w}x{self._sensor_h}, mode '{self._mode_label}', "
            f"binnings {self._binnings}, exposure [{self._exp_min:.6g}, {self._exp_max:.6g}]s, "
            f"timestamp resolution {self._ts_res:.0f} ticks/s"
        )

    def _apply_readout_mode(self, index: int):
        """Stage the readout-mode parameters (no commit — callers commit via
        _apply_binning, which also refreshes the pixel-format-dependent
        geometry). A runtime mode-switch command handler becomes a two-liner
        (_apply_readout_mode + _apply_binning) once the SensorKit data model
        grows one — but it must also re-run a warm-up frame: a mode change
        re-triggers the on-camera calibration (measured ~22 s to global,
        ~39 s to HDR), unlike ExposureTime/Rois commits which are free."""
        if not 0 <= index < len(self.config.readout_modes):
            raise ValueError(
                f"readout_mode {index} out of range for "
                f"{len(self.config.readout_modes)} configured readout modes"
            )
        mode = self.config.readout_modes[index]
        for name, value in zip(
            ("AdcAnalogGain", "AdcQuality", "AdcBitDepth", "PixelFormat", "ReadoutControlMode"),
            mode.values,
        ):
            if value >= 0:  # -1 = not applicable for this mode
                _call(self._lib.Picam_SetParameterIntegerValue, self._handle, _P[name], value, op=f"Set_{name}")
        self._mode_label = mode.label
        self._max_adu = (1 << mode.values[2]) - 1

    def _apply_binning(self, binning: int):
        """Full frame at the given symmetric binning; commits all pending parameters."""
        lib, handle = self._lib, self._handle
        width = (self._sensor_w // binning) * binning
        height = (self._sensor_h // binning) * binning
        roi = PicamRoi(0, width, binning, 0, height, binning)
        rois = PicamRois(pointer(roi), 1)
        _call(lib.Picam_SetParameterRoisValue, handle, _P["Rois"], byref(rois), op="Set_Rois")
        _commit(lib, handle)
        self._bin = binning
        self._width = width // binning
        self._height = height // binning
        pixel_format = _get_i(lib, handle, _P["PixelFormat"])
        self._dtype = np.uint32 if pixel_format == 2 else np.uint16  # 2 = Monochrome32Bit
        self._readout_stride = _get_i(lib, handle, _P["ReadoutStride"])
        try:
            self._readout_ms = _get_f(lib, handle, _P["ReadoutTimeCalculation"])
        except PicamError:
            self._readout_ms = None

    def _acquire(self, exposure_seconds: float):
        """Run one exposure; returns (payload, image, binning, anchor, started,
        ended, actual_s) or None if the acquisition was stopped externally
        before a readout."""
        lib, handle = self._lib, self._handle
        binning = self._bin  # snapshot the geometry this exposure runs at

        milliseconds = exposure_seconds * 1000.0
        if self._committed_exposure_ms is None or abs(milliseconds - self._committed_exposure_ms) > 1e-6:
            _call(lib.Picam_SetParameterFloatingPointValue, handle,
                  _P["ExposureTime"], c_double(milliseconds), op="Set_ExposureTime")
            _commit(lib, handle)
            # Re-read: the camera may coerce to its exposure resolution.
            self._committed_exposure_ms = _get_f(lib, handle, _P["ExposureTime"])
        actual_ms = self._committed_exposure_ms

        # Host-clock anchor for the hardware timestamps (ticks are relative to
        # acquisition start). This is the piece GPS timing will replace.
        anchor = datetime.now(UTC)
        _call(lib.Picam_StartAcquisition, handle, op="StartAcquisition")

        timeout_ms = c_int(int(actual_ms + (self._readout_ms or 0.0) + self.config.exposure_timeout * 1000.0))
        available = PicamAvailableData()
        status = PicamAcquisitionStatus()
        payload = None
        ticks: tuple[int, int] | None = None
        stopped_on_timeout = False
        pixel_bytes = self._width * self._height * np.dtype(self._dtype).itemsize

        acq_t0 = time.perf_counter()
        wait_ms = timeout_ms
        while True:
            error = lib.Picam_WaitForAcquisitionUpdate(handle, wait_ms, byref(available), byref(status))
            if error == 0:
                logger.debug(
                    f"acquisition update +{time.perf_counter() - acq_t0:.3f}s: "
                    f"readouts={available.readout_count} running={bool(status.running)} "
                    f"errors=0x{status.errors:x}"
                )
            if error == _TIMEOUT_OCCURRED:
                # Acquisition is still running and `available`/`status` are
                # invalid. Stop it and keep waiting for running -> false, but
                # give the rundown a short budget so the capture backstop
                # (which is sized above this whole path) never fires first.
                if stopped_on_timeout:
                    raise PicamError(error, "WaitForAcquisitionUpdate (unresponsive after stop)")
                logger.warning("acquisition timed out; stopping")
                lib.Picam_StopAcquisition(handle)
                stopped_on_timeout = True
                wait_ms = c_int(10_000)
                continue
            if error:
                raise PicamError(error, "WaitForAcquisitionUpdate")
            if available.readout_count > 0 and available.initial_readout and payload is None:
                # Copy out immediately: the buffer is only valid until the
                # next WaitForAcquisitionUpdate call. Single memcpy.
                raw = memoryview(
                    (c_ubyte * self._readout_stride).from_address(available.initial_readout)
                )
                payload = bytearray(raw[:pixel_bytes])
                # Frame metadata directly follows the pixel data: two 64-bit
                # timestamps (ExposureStarted, ExposureEnded), per the enabled
                # TimeStamps mask.
                if self._readout_stride >= pixel_bytes + 16:
                    ticks = (
                        int.from_bytes(raw[pixel_bytes : pixel_bytes + 8], "little", signed=True),
                        int.from_bytes(raw[pixel_bytes + 8 : pixel_bytes + 16], "little", signed=True),
                    )
            if not status.running:
                break

        if status.errors:
            raise RuntimeError(f"acquisition failed with errors mask 0x{status.errors:x}")
        if payload is None:
            if stopped_on_timeout:
                raise TimeoutError(f"no readout within {timeout_ms.value} ms")
            return None  # stopped externally (abort)

        image = np.frombuffer(payload, dtype=self._dtype).reshape(self._height, self._width)
        started = anchor + timedelta(seconds=ticks[0] / self._ts_res) if ticks else None
        ended = anchor + timedelta(seconds=ticks[1] / self._ts_res) if ticks else None
        return payload, image, binning, anchor, started, ended, actual_ms / 1000.0

    def _read_temperature(self):
        """Hardware reads of the sensor temperature and its lock status."""
        lib, handle = self._lib, self._handle
        temperature = c_double()
        _call(lib.Picam_ReadParameterFloatingPointValue, handle,
              _P["SensorTemperatureReading"], byref(temperature), op="Read_SensorTemperatureReading")
        temperature_status = c_int()
        try:
            _call(lib.Picam_ReadParameterIntegerValue, handle,
                  _P["SensorTemperatureStatus"], byref(temperature_status), op="Read_SensorTemperatureStatus")
            status = temperature_status.value
        except PicamError:
            status = None
        return temperature.value, status

    def _close(self):
        lib, handle = self._lib, self._handle
        self._handle = None
        if handle is not None:
            lib.Picam_StopAcquisition(handle)  # no-op error if nothing is running
            lib.Picam_CloseCamera(handle)
        lib.Picam_UninitializeLibrary()
        logger.info("PICam camera closed")


# ── Service entrypoint ───────────────────────────────────────────────


@sk.service_entrypoint(version=sk.VERSION)
async def dao_camera_service(svc: sk.Service):
    await svc.register()

    config = await svc.context.kv_get_model(DaoCameraConfig)

    # No name: the camera is the service's delegate entity, sharing the
    # service name (e.g. Cosmos-8k) — one camera per process.
    svc.include(config.create_device())

    await svc.run()
