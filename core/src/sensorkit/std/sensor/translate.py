# SPDX-License-Identifier: Apache-2.0
"""A `StandardCollectTask` in the request vocabulary.

The standard task names no devices — a target, a filter, a binning, a frame count
— which is what makes it translatable rather than merely runnable: it is already a
capability task with the selection implied. What it implies is written down here,
once:

* the **target** is a sensor-scope command, so it lands on whatever mount lies
  above the instrument taking the frames, and two mounts are an error rather than
  a coin flip;
* the **camera parameters** are instrument-scope commands, so they land on the
  camera and on the wheel in front of it, and which of the two is which is read
  off the structure rather than authored;
* **`sidereal_frames` is a step boundary.** A step is one configuration epoch, and
  holding the current pointing under sidereal tracking is a different epoch from
  following the target. Contiguous frames sharing a target become one step each,
  and `compile_collect` elides the re-commands between them, so a collect that
  never switches compiles to exactly one slew.

What a frame says about itself is built here too — one `Collect` keyword per frame,
because the target it names is the one that frame's step commanded, and only this
layer knows which that was.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import CatalogTarget, FrameTarget, ICRSTarget, Target, TLETarget
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import Binning, ConfigureCameraSensor, FrameType
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.workflow import (
    ByCapability,
    CommandId,
    CommandRequest,
    DeviceRef,
    ExposureRequest,
    InstrumentEntry,
    InstrumentRole,
    OpContext,
    RequestResolver,
    RequestStep,
    Selector,
)

SIDEREAL = FrameTarget(frame=ReferenceFrame.ICRF)
"""Hold the current pointing under sidereal tracking.

A tracking mode is a target value rather than a flag, so the switch a streak frame
asks for is an ordinary command with an ordinary target in it."""

SCIENCE = ByCapability(role=InstrumentRole.SCIENCE, count=1)
"""The one instrument a standard collect exposes: the selection its parameters
imply, and the whole of what the task says about which camera it wants."""


@dataclass(frozen=True)
class Translation:
    """A standard collect task said twice: as steps to resolve, and as what each
    frame will carry into its header.

    The two halves are produced together because they answer the same question —
    which target a given frame is taken under — and answering it twice is how a
    header comes to disagree with the pointing it records.

    `acquire` is the collect's opening bracket, and is set only when the first
    step cannot serve as one: holding sidereal means holding *what the mount
    already has*, so a collect whose very first frame is sidereal must reach the
    target before it can hold it. A step is a configuration epoch with frames in
    it, and this is neither, so it is composed above the graph — the same place,
    and for the same reason, as the halt that closes the collect.
    """

    steps: tuple[RequestStep, ...]
    frames: Mapping[DeviceRef, tuple[Collect, ...]]
    acquire: Target | None = None

    def keywords(self, ctx: OpContext, number: int) -> Iterable[object]:
        """The dispatcher's `FrameKeywords` hook: this frame's own metadata.

        Frames are numbered per instrument and per run, which is the same count
        this indexes: a frame beyond what was translated is one nobody asked for,
        and it carries nothing rather than another frame's header.
        """
        frames = self.frames.get(ctx.op.ref, ())

        return (frames[number],) if number < len(frames) else ()


def translate(task: StandardCollectTask, resolver: RequestResolver) -> Translation:
    """Put a standard collect task into request vocabulary.

    Args:
        task: The collect to perform.
        resolver: What the sensor's instruments are, and what they can do.

    Returns:
        The steps to resolve, and the metadata their frames carry.

    Raises:
        ValueError: No instrument satisfies the task.
    """
    entry = instrument(resolver, SCIENCE)
    params = task.camera_params
    targets = frame_targets(task)
    settings = camera_settings(params, entry.commands)

    return Translation(
        steps=tuple(
            RequestStep(
                settings=(CommandRequest(command=FollowTarget(target=target)),),
                exposures=(ExposureRequest(
                    select=SCIENCE,
                    integration_time=params.integration_time_seconds,
                    frame_count=count,
                    frame_type=params.frame_type or FrameType.LIGHT,
                    settings=settings),))
            for target, count in runs(targets)),
        frames={entry.ref: tuple(
            Collect(target=target, target_id=target_id(task), params=params,
                    frame_number=number)
            for number, target in enumerate(targets))},
        acquire=(task.target if targets and targets[0] != task.target else None),
    )


def instrument(resolver: RequestResolver, select: Selector) -> InstrumentEntry:
    """The manifest entry a selector resolves to, by the resolver's own rule.

    The entry rather than the path, because a translation asks two things of the
    instrument it picked: which device to number frames against, and what the chain
    behind it can be told to do.
    """
    paths = set(resolver.resolve_paths(select))

    return next(e for e in resolver.manifest if e.path in paths)


def frame_targets(task: StandardCollectTask) -> tuple[Target, ...]:
    """The target each frame is taken under.

    A target that is itself sidereal is followed throughout: the slew that acquired
    it established sidereal tracking already, so there is nothing for a sidereal
    frame to switch to and no frame of such a collect is treated as one.
    """
    frames = range(task.camera_params.frame_count)

    if isinstance(task.target, (ICRSTarget, CatalogTarget)):
        return tuple(task.target for _ in frames)

    switching = set(task.sidereal_frames)

    return tuple(SIDEREAL if number in switching else task.target
                 for number in frames)


def runs(targets: Sequence[Target]) -> Iterator[tuple[Target, int]]:
    """Contiguous frames sharing a target, which is exactly one step's worth."""
    for target, group in itertools.groupby(targets):
        yield target, sum(1 for _ in group)


def camera_settings(params: CameraParameterSet,
                    supported: frozenset[CommandId]) -> tuple[CommandRequest, ...]:
    """The instrument-scope commands a parameter set asks for.

    A filter is only commanded where something on the chain can change one: a site
    with a fixed filter names it so that the header records it, and has nothing to
    send it to. A sensor configuration is commanded wherever it is named, so a
    camera that cannot take the binning it was given fails the collect rather than
    quietly returning frames at another one.
    """
    settings: list[CommandRequest] = []

    if params.filter_name is not None and SetFilter.model_tag() in supported:
        settings.append(CommandRequest(
            command=SetFilter(filter=params.filter_name)))

    if params.binning_x is not None and params.binning_y is not None:
        settings.append(CommandRequest(command=ConfigureCameraSensor(
            binning=Binning(x=params.binning_x, y=params.binning_y))))

    return tuple(settings)


def target_id(task: StandardCollectTask) -> str | None:
    """What the header calls the object, inferred where the task does not say."""
    if task.target_id:
        return task.target_id

    match task.target:
        case TLETarget(tle=tle):
            return tle.norad_id
        case CatalogTarget(object=name):
            return name
        case _:
            return None
