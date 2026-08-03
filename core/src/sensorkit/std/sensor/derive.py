# SPDX-License-Identifier: Apache-2.0
"""Derivation of a `workflow.SensorPlan` from the `sensors:` configuration section.

A site writes seven device refs and eleven policy flags; the workflow library wants
a structural tree and a set of phase tables. Both are recoverable from what is
already written, so nothing is asked of a deployment and the flags keep a single
written-down meaning — this module.

* **Structure** (`derive_structure`). One canonical shape: sensor-wide devices at
  the root, the optical train on an `ota` assembly, the camera as the one
  instrument beneath it. Trait labels are the archetype names `std` declares, so an
  entry selects on exactly what a device reports through `DeviceDetails.archetype`
  and no translation table exists anywhere.

* **Tables** (`derive_tables`). Five: `init`, `standby` (its synonym), `shutdown`,
  `recover`, and `stop` — the last being what a failed bring-up runs to undo
  itself, composed by the handler rather than folded into `init`. The policy flags
  decide phase `after` sets and whether an entry's ops split into concurrent
  entries, which is the whole of what they say.

Timeouts are absent on purpose: a timeout is a property of hardware rather than of
a workflow, and reaches a run as dispatcher middleware keyed on `(trait, op)`.

One device set is reachable from this section and one only — `SensorDevices.camera`
is a single field, so a derived sensor has exactly one instrument. The library's
multi-instrument paths are unreachable from config until an authored structure
lands.
"""

from __future__ import annotations

from collections.abc import Mapping

from sensorkit.core.device import DeviceCommand
from sensorkit.core.entity import DeviceDetails
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure, StandardEnclosure
from sensorkit.std.instrument import StandardRotator
from sensorkit.std.mount import StandardMount
from sensorkit.std.optics import (
    CloseMirrorCover,
    OpenMirrorCover,
    StandardFilterChanger,
    StandardFocuser,
    StandardMirrorCover,
)
from sensorkit.std.sensor.config import SensorConfig, SensorDevices, SensorPolicies
from sensorkit.std.traits import Connect, Deinit, Init, Stop
from sensorkit.workflow import (
    Assembly,
    Attachment,
    CapabilityIndex,
    DeviceCapabilities,
    DeviceRef,
    Entry,
    InstrumentAssembly,
    OnFailure,
    OpSpec,
    Part,
    Phase,
    PhaseTable,
    SensorModel,
    SensorPlan,
    Trait,
)

OTA = "ota"
"""Path segment of the optical assembly: everything the camera looks through."""

PRIMARY = "primary"
"""Path segment of the one instrument a derived structure can carry."""

HALTABLE: tuple[Trait, ...] = (
    StandardMount.name, StandardEnclosure.name, StandardMirrorCover.name)
"""What a halt addresses: the devices that move under their own power."""


def derive_structure(name: str, devices: SensorDevices) -> SensorModel:
    """Build the structural tree a `SensorDevices` describes.

    The mount and the enclosure are claimed at the root, so they lie on every
    chain and their settings are shared; the filter wheel is claimed at the leaf,
    so it is the instrument's own. That split is what decides whether a command
    compiles into a step or into a frame block, and it falls out of the placement
    rather than being asserted anywhere.

    An absent device contributes no claim, and a `SensorDevices` with no camera
    yields no instrument — which is `sensor_collect`'s own guard, said
    structurally.
    """
    return SensorModel(
        name=name,
        attachments=_attachments(
            (devices.mount, StandardMount.name),
            (devices.dome, StandardEnclosure.name),
        ),
        parts=_optics(devices),
    )


def derive_tables(policies: SensorPolicies) -> dict[str, PhaseTable]:
    """Build the five phase tables a `SensorPolicies` describes.

    `standby` is `init`: today's handler is a literal synonym, and stays one until
    a deployment says what else it should mean.
    """
    init = _init_table(policies)

    return {
        "init": init,
        "standby": init.model_copy(update={"name": "standby"}),
        "shutdown": _shutdown_table(policies),
        "recover": _recover_table(),
        "stop": _stop_table(),
    }


def derive_plan(config: SensorConfig) -> SensorPlan:
    """Assemble the structure and the tables into a plan."""
    return SensorPlan(
        sensor=derive_structure(config.controller_name, config.devices),
        tables=derive_tables(config.policies),
    )


def capability_index(details: Mapping[DeviceRef, DeviceDetails]) -> CapabilityIndex:
    """Project what each device publishes about itself into the resolver's index.

    Keywords stay empty: no task in this scope carries a predicate over them, so
    nothing subscribes to capability keywords yet.
    """
    return {ref: _capabilities(d) for ref, d in details.items()}


def _capabilities(details: DeviceDetails) -> DeviceCapabilities:
    traits = {t.name for t in details.traits}

    # A device matches at most one archetype, and it is not among `details.traits`.
    if details.archetype is not None:
        traits.add(details.archetype.name)

    return DeviceCapabilities(
        traits=frozenset(traits),
        commands=details.supported_commands,
    )


def _attachments(*claims: tuple[DeviceRef | None, Trait]) -> list[Attachment]:
    return [Attachment(ref=ref, trait=trait) for ref, trait in claims if ref]


def _optics(devices: SensorDevices) -> list[Part]:
    """The `ota` assembly, or nothing at all when no device would hang on it."""
    attachments = _attachments(
        (devices.focuser, StandardFocuser.name),
        (devices.rotator, StandardRotator.name),
        (devices.mirror_cover, StandardMirrorCover.name),
    )
    parts: list[Part] = []

    if devices.camera:
        parts.append(InstrumentAssembly(
            name=PRIMARY,
            instrument=devices.camera,
            attachments=_attachments(
                (devices.filter_wheel, StandardFilterChanger.name)),
        ))
    else:
        # No instrument to own the wheel, and dropping it would put it out of reach
        # of the per-device ops that address every configured device.
        attachments += _attachments(
            (devices.filter_wheel, StandardFilterChanger.name))

    if not attachments and not parts:
        return []

    return [Assembly(name=OTA, attachments=attachments, parts=parts)]


def _ops(*commands: type[DeviceCommand]) -> list[str]:
    """Op names are registered command ids, which is what the dispatcher resolves."""
    return [c.model_tag() for c in commands]


def _optional(*commands: type[DeviceCommand],
              on_failure: OnFailure | None = None) -> tuple[OpSpec, ...]:
    return tuple(OpSpec(op=name, optional=True, on_failure=on_failure)
                 for name in _ops(*commands))


def _init_table(policies: SensorPolicies) -> PhaseTable:
    # Init and open are one entry — serial per device by construction — unless the
    # dome may do both at once, which is two entries in one phase.
    enclosure: tuple[Entry, ...] = (
        (Entry(trait=StandardEnclosure.name, ops=_ops(Init)),
         Entry(trait=StandardEnclosure.name, ops=_ops(OpenEnclosure)))
        if policies.concurrent_dome_init_open
        else (Entry(trait=StandardEnclosure.name,
                    ops=_ops(Init, OpenEnclosure)),))

    mount_after = () if policies.concurrent_dome_and_mount_init else ("enclosure",)

    # The mirror cover either starts on the same precondition the mount did, or
    # waits for everything already under way. Naming both predecessors rather than
    # relying on the mount's own wait is what keeps the second case right when the
    # mount is not itself waiting on the enclosure.
    optics_after = (mount_after
                    if policies.concurrent_mount_and_mirror_cover_init
                    else ("enclosure", "mount"))

    return PhaseTable(
        name="init",
        on_failure="stop",
        phases=(
            Phase(name="enclosure", entries=enclosure),
            Phase(name="mount", after=mount_after, entries=(
                Entry(trait=StandardMount.name, ops=_ops(Init)),)),
            Phase(name="optics", after=optics_after, entries=(
                Entry(trait=StandardMirrorCover.name,
                      ops=_ops(OpenMirrorCover)),)),
        ),
    )


def _shutdown_table(policies: SensorPolicies) -> PhaseTable:
    closing: tuple[Entry, ...] = (
        (Entry(trait=StandardEnclosure.name, ops=_ops(CloseEnclosure)),
         Entry(trait=StandardEnclosure.name, ops=_ops(Deinit)))
        if policies.concurrent_dome_deinit_close
        else (Entry(trait=StandardEnclosure.name,
                    ops=_ops(CloseEnclosure, Deinit)),))

    return PhaseTable(
        name="shutdown",
        # The flag is the table's failure policy, and the choice it encodes is
        # whether a failed step abandons the teardown or only what it invalidated.
        on_failure="skip" if policies.always_deinit_dome else "stop",
        phases=(
            Phase(name="optics", entries=(
                Entry(trait=StandardMirrorCover.name,
                      ops=_ops(CloseMirrorCover)),)),
            Phase(name="mount", entries=(
                Entry(trait=StandardMount.name, ops=_ops(Deinit)),)),
            # Halt leftover motion before closing, so nothing aborts the close
            # mid-flight; it is worth reporting and must not hold up the close.
            Phase(name="halt",
                  after=(("optics",)
                         if policies.concurrent_dome_and_mount_deinit else None),
                  entries=(Entry(trait=StandardEnclosure.name,
                                 ops=_optional(Stop, on_failure="continue")),)),
            Phase(name="enclosure", entries=closing),
        ),
    )


def _recover_table() -> PhaseTable:
    # Two phases rather than one entry with two ops: one entry serializes per
    # device, which would let one device's Stop precede another's Connect.
    return PhaseTable(
        name="recover",
        on_failure="continue",
        phases=(
            Phase(name="reconnect", entries=(
                Entry(match="all", ops=_optional(Connect)),)),
            Phase(name="halt", entries=(
                Entry(match="all", ops=_optional(Stop)),)),
        ),
    )


def _stop_table() -> PhaseTable:
    return PhaseTable(
        name="stop",
        on_failure="continue",
        phases=(Phase(name="halt", entries=tuple(
            Entry(trait=trait, ops=_optional(Stop))
            for trait in HALTABLE)),),
    )
