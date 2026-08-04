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

**Several exposures are packed, not scheduled.** A task says what it wants exposed
and never how many instruments a site has, so how much of it happens at once is
derived here: each exposure is matched against the manifest and joins the newest
wave that will hold it, opening the next one when none will. A sensor with one
instrument therefore yields a wave per exposure and takes them in turn; a sensor
with several fills a wave before opening the next and takes that many at once.
Between instruments that can equally take an exposure, the one with least queued
gets it, since a camera's frames serialize with themselves however they were
authored.

**`resolve_step` decides what will hold**, rather than a rule restated here. It
already knows every reason a wave can fail — a repeated instrument, two ports of
one selector, two cameras behind one wheel asking for different filters — and the
last of those is invisible to anything counting instruments. So the seams land
wherever the optics put them, and an exposure that will not resolve even alone
raises its own error instead of packing forever.

**A wave divides where its tracking changes, and `coalesce` is what divides it.**
This layer says only what each frame is taken under, frame by frame; where the
boundaries between those fall needs nothing but equality, and belongs with the
vocabulary it produces. A target is a sensor-scope command, so there is no epoch in
which one camera holds sidereal while another follows — every instrument in a wave
switches together, and one with fewer frames than the wave's longest has taken all
of them by then and simply stops appearing.

What a frame says about itself is built here too — one `Collect` keyword per frame,
because the target it names is the one that frame's step commanded, and only this
layer knows which that was. Frames are numbered within the exposure that asked for
them, which is what `sidereal_frames` numbers, so a header frame number always
names the frame the task named.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import CatalogTarget, FrameTarget, ICRSTarget, Target, TLETarget
from sensorkit.core.device import DeviceCommand
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import Binning, ConfigureCameraSensor, FrameType
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.workflow import (
    ByCapability,
    ByRef,
    CommandId,
    CommandRequest,
    DeviceRef,
    ExposureRequest,
    InstrumentEntry,
    InstrumentPath,
    InstrumentRole,
    OpContext,
    RequestResolver,
    RequestStep,
    coalesce,
    matches,
    select,
)

SIDEREAL = FrameTarget(frame=ReferenceFrame.ICRF)
"""Hold the current pointing under sidereal tracking.

A tracking mode is a target value rather than a flag, so the switch a streak frame
asks for is an ordinary command with an ordinary target in it."""

SCIENCE = ByCapability(role=InstrumentRole.SCIENCE, count=1)
"""What a standard collect exposes through, before its parameters say more: the
whole of what the task states about which camera it wants."""


type Assignment = tuple[InstrumentEntry, CameraParameterSet]
"""One exposure and the instrument packing gave it.

The parameter set is carried alongside the request it produces rather than read back
off it, because a header records what was asked for and not what was derived from
it."""


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

        The dispatcher counts an instrument's frames across the whole run, which is
        the position this indexes rather than the number the frame carries — one
        instrument taking two exposures numbers each from its own start. A frame
        beyond what was translated is one nobody asked for, and it carries nothing
        rather than another frame's header.
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
    steps: list[RequestStep] = []
    frames: dict[DeviceRef, list[Collect]] = {}

    for wave in pack(task, resolver):
        asked = {entry.ref: params for entry, params in wave}
        numbered: dict[DeviceRef, int] = {}

        for step in coalesce(schedule(task, wave), requests(wave)):
            target = commanded_target(step)

            for exposure in step.exposures:
                ref = exposure.select.ref
                # Numbered from the start of the exposure that asked for them,
                # which is what `sidereal_frames` numbers. One instrument takes at
                # most one exposure per wave, so the count resets with the wave.
                first = numbered.get(ref, 0)
                frames.setdefault(ref, []).extend(
                    Collect(target=target, target_id=target_id(task),
                            params=asked[ref], frame_number=first + number)
                    for number in range(exposure.frame_count))
                numbered[ref] = first + exposure.frame_count

            steps.append(step)

    # A collect asking for no frames opens no epoch, and has nothing to reach for.
    opening = commanded_target(steps[0]) if steps else task.target

    return Translation(
        steps=tuple(steps),
        frames={ref: tuple(taken) for ref, taken in frames.items()},
        acquire=task.target if opening != task.target else None,
    )


def schedule(task: StandardCollectTask,
             wave: Sequence[Assignment]) -> tuple[tuple[CommandRequest, ...], ...]:
    """What each of a wave's frames is taken under, frame-major.

    The whole of what this layer says about chronology. Where the boundaries between
    these fall is `coalesce`'s, and it needs no more than equality to find them.
    """
    longest = max(params.frame_count for _, params in wave)

    return tuple((CommandRequest(command=FollowTarget(target=target)),)
                 for target in frame_targets(task, longest))


def requests(wave: Sequence[Assignment]) -> tuple[ExposureRequest, ...]:
    """A wave's exposures in request vocabulary, each asking for all of its frames.

    How they divide between steps is `coalesce`'s to say, so the counts here are the
    task's own.
    """
    return tuple(exposure_request(entry, params) for entry, params in wave)


def commanded_target(step: RequestStep) -> Target:
    """The target a step commands, read back off it rather than tracked alongside.

    One authority for what a frame was taken under, which is how a header comes to
    agree with the pointing it records.
    """
    return next(cmd.command.target for cmd in step.settings
                if isinstance(cmd.command, FollowTarget))


def pack(task: StandardCollectTask,
         resolver: RequestResolver) -> tuple[tuple[Assignment, ...], ...]:
    """Assign exposures to instruments, and instruments to waves.

    First fit into the newest wave, because a wave is what runs at once and the
    cheapest schedule is the one that fills it: an exposure joins the wave under
    construction if an instrument it matches will hold it there, and otherwise
    opens the next one.

    Which instrument is the same question rather than a prior one — an exposure
    takes the least loaded of its candidates that fits, so a site with a spare chain
    uses it rather than colliding on a shared one, and a wave that has to open finds
    an instrument that is nearly done rather than the one still working. A camera's
    frames serialize with themselves, so an exposure sent to a busy instrument waits
    on everything already queued there whatever step it was authored into.

    An exposure that will not resolve even alone is a task no boundary fixes, so
    its own error is let out rather than opening a wave per exposure.
    """
    waves: list[list[Assignment]] = [[]]
    load: dict[InstrumentPath, float] = {}

    for params in task.exposures:
        # Stable, so the capability order `candidates` returns survives as the
        # tie-break: everything it offers is equally able to take the exposure, and
        # load only decides between equals.
        options = sorted(candidates(resolver, params),
                         key=lambda e: load.get(e.path, 0.0))
        entry = next((e for e in options
                      if fits(resolver, task.target, (*waves[-1], (e, params)))),
                     None)

        if entry is None:
            resolve_wave(resolver, task.target, ((options[0], params),))
            waves.append([])
            entry = options[0]

        waves[-1].append((entry, params))
        load[entry.path] = load.get(entry.path, 0.0) + duration(params)

    return tuple(tuple(wave) for wave in waves)


def duration(params: CameraParameterSet) -> float:
    """How long an exposure occupies the instrument taking it.

    Integration only, for the same reason `FramePlan.duration_s` leaves readout and
    overhead out: what a site spends between frames is not modeled anywhere below
    here. Enough to rank instruments against each other, and not a prediction of
    when one comes free.
    """
    return params.integration_time_seconds * params.frame_count


def fits(resolver: RequestResolver, target: Target,
         wave: Sequence[Assignment]) -> bool:
    """Whether these exposures can share one configuration epoch.

    `resolve_step` is the oracle because it already knows every rule the answer
    depends on, including the ones no count of instruments can see: two cameras
    behind one wheel collide on the wheel rather than on each other.
    """
    try:
        resolve_wave(resolver, target, wave)
    except ValueError:
        return False

    return True


def resolve_wave(resolver: RequestResolver, target: Target,
                 wave: Sequence[Assignment]) -> None:
    """Put a wave to the resolver, discarding what it resolves to.

    How many frames an exposure takes does not bear on whether a step holds, so the
    trial asks about the whole of each and the division into runs comes after.
    """
    resolver.resolve_step(RequestStep(
        settings=(CommandRequest(command=FollowTarget(target=target)),),
        exposures=requests(wave)))


def exposure_request(entry: InstrumentEntry,
                     params: CameraParameterSet) -> ExposureRequest:
    """One instrument's exposure, in request vocabulary."""
    return ExposureRequest(
        # Named rather than described, because by here it is chosen: which of
        # several instruments an exposure goes to is what packing decided, and a
        # descriptor resolved again would answer for all of them alike.
        select=ByRef(entry.ref),
        integration_time=params.integration_time_seconds,
        frame_count=params.frame_count,
        frame_type=params.frame_type or FrameType.LIGHT,
        settings=camera_settings(params, entry.commands))


def candidates(resolver: RequestResolver,
               params: CameraParameterSet) -> tuple[InstrumentEntry, ...]:
    """The instruments that can take one exposure, in the order to prefer them.

    What the parameters imply is a preference rather than a requirement, and the
    two differ only where a site has a choice to make. An instrument whose chain can
    change a filter takes the exposure that names one ahead of an instrument whose
    cannot — but where none can, the exposure is taken anyway and the filter is left
    uncommanded, since a site with a fixed filter names it so that the header
    records it and has nothing to send it to.
    """
    role = tuple(e for e in resolver.manifest if matches(e, SCIENCE))

    if not role:
        # `matches` is what can count, and `select` is what says why nothing did —
        # in the terms the descriptor was written in. It raises on a shortfall,
        # which no instrument at all is.
        select(resolver.manifest, SCIENCE)

    return tuple(e for e in role if matches(e, descriptor(params))) or role


def descriptor(params: CameraParameterSet) -> ByCapability:
    """What a parameter set asks of an instrument, in capability terms.

    Derived from the parameters and never authored, which is what keeps the task as
    portable as it was while making it resolvable against a structure it does not
    name. Only the commands a translation actually sends appear: preferring an
    instrument for work nobody does would be a selection nobody could account for.
    """
    return ByCapability(
        role=InstrumentRole.SCIENCE,
        capabilities=tuple(type(c).model_tag() for c in commanded(params)),
        count=1)


def frame_targets(task: StandardCollectTask, count: int) -> tuple[Target, ...]:
    """The target each of a wave's frames is taken under.

    A target that is itself sidereal is followed throughout: the slew that acquired
    it established sidereal tracking already, so there is nothing for a sidereal
    frame to switch to and no frame of such a collect is treated as one.
    """
    frames = range(count)

    if isinstance(task.target, (ICRSTarget, CatalogTarget)):
        return tuple(task.target for _ in frames)

    switching = set(task.sidereal_frames)

    return tuple(SIDEREAL if number in switching else task.target
                 for number in frames)


def commanded(params: CameraParameterSet) -> tuple[DeviceCommand, ...]:
    """What a parameter set asks an instrument's chain to do, before anything asks
    whether a given chain can.

    `gain` is not among them: `ConfigureCameraSensor` has room for it and the
    hand-written handler never filled it, so it reaches no device and implies no
    capability.
    """
    commands: list[DeviceCommand] = []

    if params.filter_name is not None:
        commands.append(SetFilter(filter=params.filter_name))

    if params.binning_x is not None and params.binning_y is not None:
        commands.append(ConfigureCameraSensor(
            binning=Binning(x=params.binning_x, y=params.binning_y)))

    return tuple(commands)


def camera_settings(params: CameraParameterSet,
                    supported: frozenset[CommandId]) -> tuple[CommandRequest, ...]:
    """The instrument-scope commands a parameter set asks of one chain.

    A filter is only commanded where something on the chain can change one: a site
    with a fixed filter names it so that the header records it, and has nothing to
    send it to. A sensor configuration is commanded wherever it is named, so a
    camera that cannot take the binning it was given fails the collect rather than
    quietly returning frames at another one.
    """
    return tuple(CommandRequest(command=command)
                 for command in commanded(params)
                 if not isinstance(command, SetFilter)
                 or SetFilter.model_tag() in supported)


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
