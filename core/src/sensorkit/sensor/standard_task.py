# SPDX-License-Identifier: Apache-2.0
"""Translate standard collect tasks into hardware-independent collect
intents.

The target becomes sensor-subject preparation and epoch settings; Stop becomes
cleanup. Camera parameters become per-request instrument settings. Packing
chooses devices and checks that requested settings have supporting commands.

Target changes, including sidereal frames, divide the collect into epochs.
Segments of each exposure share an assignment so they stay on one instrument
and continue their request ordinals. Shorter exposures leave later epochs.

The `Collect` keyword records requested target and camera parameters,
separately from achieved device values. Packing fills its per-instrument frame
number.
"""

from __future__ import annotations

import itertools

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import (
    CatalogTarget,
    FrameTarget,
    ICRSTarget,
    Target,
    TLETarget,
)
from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
)
from sensorkit.sensor.selection import Supports
from sensorkit.std.collect import (
    CameraParameterSet,
    Collect,
    StandardCollectTask,
)
from sensorkit.std.instrument import Binning, ConfigureCameraSensor
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.std.traits import Stop

SIDEREAL = FrameTarget(frame=ReferenceFrame.ICRF)
"""Target value that holds the current pointing under sidereal tracking."""


def translate(task: StandardCollectTask, *, readout_margin_s: float = 60.0) -> CollectIntent:
    """Build collect requests and target-based epochs from a standard task.

    Exposure segments retain their ids and assignments across epochs.
    Preparation reaches the original target even if the first frame uses
    sidereal tracking. Each capture's deadline is its integration time plus
    `readout_margin_s`.

    Raises:
        ValueError: A sidereal frame index is out of range or only one binning
            axis is supplied.
    """
    exposures = task.exposures
    settings = tuple(camera_settings(params) for params in exposures)
    longest = max(params.frame_count for params in exposures)
    _check_sidereal(task, longest)

    epochs: list[RequestEpoch] = []
    first = 0

    for target, frames in itertools.groupby(frame_targets(task, longest)):
        count = sum(1 for _ in frames)
        # Include only the segment frames remaining for each exposure.
        units = tuple(
            _segment(
                task,
                number,
                target,
                min(first + count, params.frame_count) - first,
                settings[number],
                readout_margin_s,
            )
            for number, params in enumerate(exposures)
            if first < params.frame_count
        )
        epochs.append(RequestEpoch(units=units, settings=(_pointing(target),)))
        first += count

    # Empty collects need no preparation or cleanup.
    if not epochs:
        return CollectIntent(name=task.task_type, epochs=())

    return CollectIntent(
        name=task.task_type,
        epochs=tuple(epochs),
        prepare=(_pointing(task.target),),
        cleanup=(
            CommandRequest(
                command=Stop(), subject="sensor", select=Supports(supports=FollowTarget)
            ),
        ),
    )


def _segment(
    task: StandardCollectTask,
    number: int,
    target: Target,
    count: int,
    settings: tuple[CommandRequest, ...],
    margin_s: float,
) -> InstrumentRequest:
    """Build one exposure segment with a stable request id and assignment
    group.
    """
    params = task.exposures[number]
    name = f"exposure-{number}"

    return InstrumentRequest(
        id=name,
        assignment=name,
        acquisition=AcquisitionRequest(
            integration_time_s=params.integration_time_seconds,
            count=count,
            timeout_s=params.integration_time_seconds + margin_s,
        ),
        collect=Collect(target=target, target_id=target_id(task), params=params),
        settings=settings,
    )


def _pointing(target: Target) -> CommandRequest:
    """Request FollowTarget on the common sensor target device."""
    return CommandRequest(command=FollowTarget(target=target), subject="sensor")


def frame_targets(task: StandardCollectTask, count: int) -> tuple[Target, ...]:
    """Return the target for each zero-based frame index.

    ICRS and catalog targets remain sidereal throughout. Other targets switch
    to `SIDEREAL` at the indices specified by the task.
    """
    frames = range(count)

    if isinstance(task.target, (ICRSTarget, CatalogTarget)):
        return tuple(task.target for _ in frames)

    switching = set(task.sidereal_frames)

    return tuple(SIDEREAL if number in switching else task.target for number in frames)


def camera_settings(params: CameraParameterSet) -> tuple[CommandRequest, ...]:
    """Build instrument settings from requested camera parameters.

    Binning and gain share one command because collect state replacement
    operates by command type, without merging partial fields.

    Raises:
        ValueError: Only one binning axis is supplied.
    """
    commands: list[DeviceCommand] = []

    if params.filter_name is not None:
        commands.append(SetFilter(filter=params.filter_name))

    match params.binning_x, params.binning_y:
        case None, None:
            binning = None
        case int(x), int(y):
            binning = Binning(x=x, y=y)
        case x, y:
            raise ValueError(f"binning is {x} by {y}; give both axes or neither")

    if binning is not None or params.gain is not None:
        commands.append(ConfigureCameraSensor(binning=binning, gain=params.gain))

    return tuple(CommandRequest(command=command) for command in commands)


def target_id(task: StandardCollectTask) -> str | None:
    """Use the explicit target id, otherwise infer a NORAD id or catalog
    object name.
    """
    if task.target_id:
        return task.target_id

    match task.target:
        case TLETarget(tle=tle):
            return tle.norad_id
        case CatalogTarget(object=name):
            return name
        case _:
            return None


def _check_sidereal(task: StandardCollectTask, longest: int) -> None:
    """Check sidereal indices against the longest exposure's frame count.

    Raises:
        ValueError: An index is negative or beyond all exposures.
    """
    outside = sorted(number for number in task.sidereal_frames if not 0 <= number < longest)

    if outside:
        raise ValueError(
            f"sidereal_frames {outside} name no frame; the longest exposure takes {longest}"
        )
