# SPDX-License-Identifier: Apache-2.0
"""Derivation of a `workflow.SensorPlan` from the `sensors:` configuration section.

A site writes seven device refs and eleven policy flags; the workflow library wants
a structural tree and a set of phase tables. Both are recoverable from what is
already written, so nothing is asked of a deployment and the flags keep a single
written-down meaning — this module.

* **Structure** (`derive_structure`). One canonical shape: sensor-wide devices at
  the root, the shared optical train on an `ota` assembly, and one instrument
  beneath it per configured camera. Trait labels are the archetype names `std`
  declares, so an entry selects on exactly what a device reports through
  `DeviceDetails.archetype` and no translation table exists anywhere.

* **Tables** (`derive_tables`). Five: `init`, `standby` (its synonym), `shutdown`,
  `recover`, and `stop` — the last being what a failed bring-up runs to undo
  itself, composed by the handler rather than folded into `init`. The policy flags
  decide phase `after` sets and whether an entry's ops split into concurrent
  entries, which is the whole of what they say.

* **Timeouts** (`timeouts`). Keyed on `(trait, op)` for the dispatcher's own
  resolution ladder rather than written into a table, because a timeout is a
  property of hardware rather than of a workflow: it belongs to *this dome*, not to
  the table that happens to open it.

What a derived structure cannot say is what a selector would: every instrument in it
is reachable at once, since the section pairs devices with cameras and never routes
a beam between them. Mutually exclusive ports wait on an authored structure.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

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
from sensorkit.std.sensor.dispatch import HandlerKey
from sensorkit.std.traits import Connect, Deinit, Home, Init, Stop
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

CAMERA = "camera"
"""Path segment stem of an instrument position, numbered from one in configuration
order: `camera-1`, `camera-2`. Config names devices and not positions, so the
numbering is the only name a derived instrument can have."""

HALTABLE: tuple[Trait, ...] = (
    StandardMount.name, StandardEnclosure.name, StandardMirrorCover.name)
"""What a halt addresses: the devices that move under their own power."""


def derive_structure(name: str, devices: SensorDevices) -> SensorModel:
    """Build the structural tree a `SensorDevices` describes.

    The mount and the enclosure are claimed at the root, so they lie on every
    chain and their settings are shared; a camera's own focuser and filter wheel
    are claimed at its leaf, so their settings are that instrument's. That split is
    what decides whether a command compiles into a step or into a frame block, and
    it falls out of the placement rather than being asserted anywhere.

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


def timeouts(policies: SensorPolicies) -> dict[HandlerKey, float]:
    """The deadlines a `SensorPolicies` states, keyed for the dispatch ladder.

    Only the operations a site has ever written a number for. Everything else runs
    to completion, which is what the absence of a policy says.
    """
    enclosure, cover, mount = (StandardEnclosure.name, StandardMirrorCover.name,
                               StandardMount.name)

    return {
        (enclosure, Init.model_tag()): policies.dome_init_timeout,
        (enclosure, Deinit.model_tag()): policies.dome_deinit_timeout,
        (enclosure, OpenEnclosure.model_tag()): policies.dome_open_close_timeout,
        (enclosure, CloseEnclosure.model_tag()): policies.dome_open_close_timeout,
        (cover, OpenMirrorCover.model_tag()): policies.mirror_cover_open_close_timeout,
        (cover, CloseMirrorCover.model_tag()): policies.mirror_cover_open_close_timeout,
        (mount, Init.model_tag()): policies.mount_init_timeout,
        (mount, Home.model_tag()): policies.mount_home_timeout,
    }


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
        (devices.rotator, StandardRotator.name),
        (devices.mirror_cover, StandardMirrorCover.name),
    )
    parts = _instruments(devices)

    if not parts:
        # No instrument to own the paired devices, and dropping them would put them
        # out of reach of the per-device ops that address every configured device.
        attachments += _attachments(
            *((ref, StandardFocuser.name) for ref in devices.focuser),
            *((ref, StandardFilterChanger.name) for ref in devices.filter_wheel),
        )

    if not attachments and not parts:
        return []

    return [Assembly(name=OTA, attachments=attachments, parts=parts)]


def _instruments(devices: SensorDevices) -> list[Part]:
    """One instrument per configured camera, holding the optics that camera alone
    looks through.

    Positions are numbered over the configured list rather than over the cameras
    found in it, so a camera keeps its position when a site leaves a hole in the
    list to skip one.
    """
    return [
        InstrumentAssembly(
            name=f"{CAMERA}-{index + 1}",
            instrument=camera,
            attachments=_attachments(
                (_paired(devices.focuser, index), StandardFocuser.name),
                (_paired(devices.filter_wheel, index), StandardFilterChanger.name),
            ),
        )
        for index, camera in enumerate(devices.camera) if camera
    ]


def _paired(refs: Sequence[str], index: int) -> DeviceRef | None:
    """The device configured for the camera at `index`, if any.

    A configured list holds one entry per camera, so a short read is a field the
    site left out entirely.
    """
    return refs[index] if index < len(refs) else None


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
    #
    # The ops are not optional. What a device has no command for is answered for
    # at compile time, so `optional` here would additionally tolerate a device
    # that has the command and refuses it — which is the one thing a recovery
    # exists to report.
    return PhaseTable(
        name="recover",
        on_failure="continue",
        phases=(
            Phase(name="reconnect", entries=(
                Entry(match="all", ops=_ops(Connect)),)),
            Phase(name="halt", entries=(
                Entry(match="all", ops=_ops(Stop)),)),
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
