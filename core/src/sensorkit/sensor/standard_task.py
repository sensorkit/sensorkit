# SPDX-License-Identifier: Apache-2.0
"""Translate standard collect tasks into hardware-independent collect
intents.

Target commands establish pointing before acquisition and stop it during cleanup.
Each camera parameter set supplies an acquisition request and its settings.
A parameter set naming an instrument selects only that instrument. Packing
resolves command targets and checks the requested settings.

When planning keywords are supplied, packing prefers instruments whose routed
wheel reports the requested filter. A nonempty list that lacks the name makes
the instrument ineligible. Missing or empty lists leave name validation to the
setting command.

Target changes, including sidereal frames, divide the collect into epochs.
Segments of each exposure share an assignment and stay on one instrument.
Request ordinals continue across segments. Exposures appear only in epochs
where they have frames remaining.

The `Collect` keyword records the requested target and camera parameters.
Packing fills its per-instrument frame number. Reported device values are
sampled at dispatch.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import (
    CatalogTarget,
    FrameTarget,
    ICRSTarget,
    Target,
    TLETarget,
)
from sensorkit.common.predicate import contains, exists
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
)
from sensorkit.sensor.selection import AnyOf, IsRef, KeywordMatch, Supports
from sensorkit.sensor.topology import Placement
from sensorkit.std.collect import (
    CameraParameterSet,
    Collect,
    StandardCollectTask,
)
from sensorkit.std.instrument import Binning, ConfigureCameraSensor
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import Filters, SetFilter
from sensorkit.std.traits import Stop

SIDEREAL = FrameTarget(frame=ReferenceFrame.ICRF)
"""Target value that holds the current pointing under sidereal tracking."""


def translate(
    task: StandardCollectTask,
    *,
    instruments: Mapping[str, Placement] | None = None,
    readout_margin_s: float = 60.0,
) -> CollectIntent:
    """Translate a standard collect into acquisition requests and settings.

    Each exposure keeps its request ID and instrument assignment across tracking
    segments. `instruments` resolves published names to collection targets.
    Untargeted exposures are assigned during packing.

    Preparation points at the task's target, including when frame zero uses
    sidereal tracking. Capture timeouts include `readout_margin_s`.

    Raises:
        ValueError: An instrument name is unknown, a sidereal frame index is
            invalid, or only one binning axis is set.
    """
    exposures = task.exposures
    known = {} if instruments is None else instruments
    selections = tuple(instrument_selection(params.instrument, known) for params in exposures)
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
                selections[number],
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
    selection: IsRef | None,
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
            frame_type=params.frame_type,
            timeout_s=params.integration_time_seconds + margin_s,
        ),
        count=count,
        collect=Collect(target=target, target_id=target_id(task), params=params),
        select=selection,
        settings=settings,
    )


def instrument_selection(name: str | None, instruments: Mapping[str, Placement]) -> IsRef | None:
    """Resolve an instrument name, preserving automatic selection for `None`.

    Raises:
        ValueError: The instrument name is not registered.
    """
    if name is None:
        return None

    if name not in instruments:
        known = ", ".join(f"'{key}'" for key in instruments) or "none"
        raise ValueError(f"Unknown instrument '{name}'. This sensor exposes {known}.")

    return IsRef(device=instruments[name].device)


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
    settings: list[CommandRequest] = []

    if params.filter_name is not None:
        settings.append(_filter(params.filter_name))

    match params.binning_x, params.binning_y:
        case None, None:
            binning = None
        case int(x), int(y):
            binning = Binning(x=x, y=y)
        case x, y:
            raise ValueError(f"binning is {x} by {y}; give both axes or neither")

    if binning is not None or params.gain is not None:
        settings.append(
            CommandRequest(command=ConfigureCameraSensor(binning=binning, gain=params.gain))
        )

    return tuple(settings)


def _filter(name: str) -> CommandRequest:
    """Request a filter with availability requirements and preferences.

    Wheels with absent or empty filter lists remain eligible.
    """
    held = KeywordMatch(keyword=Filters, field="filters.name", predicate=contains(name))
    unreported = KeywordMatch(keyword=Filters, field="filters", predicate=exists(False))

    return CommandRequest(
        command=SetFilter(filter=name),
        requires=(AnyOf(any_of=(held, unreported)),),
        prefers=(held,),
    )


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
