# SPDX-License-Identifier: Apache-2.0
"""Generate optional lifecycle tables and deadline rules from site policies.

Generated models use the same compilers as authored definitions. Authored
tables replace generated tables by name; authored deadlines replace generated
rules with the same target and command.

Compose deadline rules before connecting a session. After binding, compose
tables to prune generated entries that select no equipment. The compilers,
executor and session do not read policies directly.
"""

from __future__ import annotations

from pydantic import BaseModel

from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.lifecycle import (
    CleanupSpec,
    Entry,
    LifecycleWorkflow,
    OpSpec,
    Phase,
)
from sensorkit.sensor.selection import IsKind, Supports
from sensorkit.sensor.workflow import DeadlineRule
from sensorkit.std.enclosure import (
    CloseEnclosure,
    OpenEnclosure,
    StandardEnclosure,
)
from sensorkit.std.instrument import ConfigureCameraSensor
from sensorkit.std.mount import FollowTarget, StandardMount
from sensorkit.std.optics import (
    ChangeFocusPosition,
    CloseMirrorCover,
    OpenMirrorCover,
    SetFilter,
)
from sensorkit.std.traits import Connect, Deinit, Home, Init, Stop


class SensorPolicies(BaseModel, frozen=True, extra="forbid"):
    """Site options for generating standard lifecycle tables and deadlines.

    A default deadline covers commands without a matching command-specific
    rule.
    """

    concurrent_dome_and_mount_init: bool = False
    """Allow dome and mount initialization to run concurrently."""

    concurrent_dome_and_mount_deinit: bool = False
    """Allow dome closure and deinitialization to overlap mount
    deinitialization.
    """

    concurrent_dome_init_open: bool = False
    """Allow dome initialization and opening to run concurrently."""

    concurrent_dome_deinit_close: bool = False
    """Allow dome closure and deinitialization to run concurrently."""

    always_deinit_dome: bool = False
    """Continue shutdown and attempt dome deinitialization after other
    command failures.
    """

    dome_open_close_timeout: float = 120.0
    """Timeout in seconds for each dome opening or closing command."""

    dome_init_timeout: float = 300.0
    """Timeout in seconds for the dome initialization command."""

    dome_deinit_timeout: float = 300.0
    """Timeout in seconds for the dome deinitialization command."""

    concurrent_mount_and_mirror_cover_init: bool = False
    """Allow mirror-cover opening to overlap mount initialization."""

    mirror_cover_open_close_timeout: float = 60.0
    """Timeout in seconds for each mirror-cover opening or closing command."""

    mount_init_timeout: float = 30.0
    """Timeout in seconds for the mount initialization command."""

    mount_home_timeout: float = 300.0
    """Timeout in seconds for the mount homing command."""

    mount_deinit_timeout: float = 60.0
    """Timeout in seconds for the mount deinitialization command."""

    stop_timeout: float = 30.0
    """Timeout in seconds for each Stop command."""

    follow_target_timeout: float = 300.0
    """Timeout in seconds for slewing to a target and starting to track it."""

    filter_change_timeout: float = 30.0
    """Timeout in seconds for the filter change command."""

    camera_configure_timeout: float = 30.0
    """Timeout in seconds for the camera sensor configuration command."""

    focus_change_timeout: float = 30.0
    """Timeout in seconds for the focus position command."""

    default_timeout: float = 300.0
    """Timeout in seconds for any command no other deadline rule names."""

    def tables(self) -> tuple[LifecycleWorkflow, ...]:
        """Generate `init`, `standby`, `shutdown` and `recover` tables.

        `standby` uses the same sequence as `init`.
        Unsupported commands are omitted during lowering. `compose_tables`
        removes generated entries that select no equipment and applies authored
        table replacements.
        """
        init = self._init_table()
        every = IsKind(kind="any")
        # Complete reconnect attempts before stopping motion.
        recover = LifecycleWorkflow(
            name="recover",
            fail_fast=False,
            phases=(
                Phase(name="reconnect", entries=(Entry(select=every, ops=_ops(Connect)),)),
                Phase(name="halt", entries=(Entry(select=every, ops=_ops(Stop)),)),
            ),
        )

        return (init, init.model_copy(update={"name": "standby"}), self._shutdown_table(), recover)

    def deadlines(self) -> tuple[DeadlineRule, ...]:
        """Generate command deadlines for any device, and a default for every
        other command.

        Commands whose limit differs by device kind are scoped to a trait, so
        a device failing that trait takes the default. Explicit operation
        timeouts and command-specific device rules can override these.
        """
        scoped = (
            (StandardEnclosure.name, Init, self.dome_init_timeout),
            (StandardEnclosure.name, Deinit, self.dome_deinit_timeout),
            (StandardMount.name, Init, self.mount_init_timeout),
            (StandardMount.name, Home, self.mount_home_timeout),
            (StandardMount.name, Deinit, self.mount_deinit_timeout),
        )
        unscoped = (
            (OpenEnclosure, self.dome_open_close_timeout),
            (CloseEnclosure, self.dome_open_close_timeout),
            (OpenMirrorCover, self.mirror_cover_open_close_timeout),
            (CloseMirrorCover, self.mirror_cover_open_close_timeout),
            (FollowTarget, self.follow_target_timeout),
            (SetFilter, self.filter_change_timeout),
            (ConfigureCameraSensor, self.camera_configure_timeout),
            (ChangeFocusPosition, self.focus_change_timeout),
            (Stop, self.stop_timeout),
        )

        return (
            *(
                DeadlineRule(target=("trait", trait), command=command.model_tag(), seconds=seconds)
                for trait, command, seconds in scoped
            ),
            *(
                DeadlineRule(target=("any", None), command=command.model_tag(), seconds=seconds)
                for command, seconds in unscoped
            ),
            DeadlineRule(target=("any", None), seconds=self.default_timeout),
        )

    def _init_table(self) -> LifecycleWorkflow:
        # A dome lacking Init must still open and close.
        enclosure = Supports(supports=CloseEnclosure)

        # Separate phases permit overlap without duplicate targets in a phase.
        if self.concurrent_dome_init_open:
            dome = (
                Phase(
                    name="enclosure-init",
                    after=(),
                    entries=(Entry(select=enclosure, ops=_ops(Init), id="init-enclosure"),),
                ),
                Phase(
                    name="enclosure-open",
                    after=(),
                    entries=(
                        Entry(select=enclosure, ops=_ops(OpenEnclosure), id="open-enclosure"),
                    ),
                ),
            )
        else:
            dome = (
                Phase(
                    name="enclosure",
                    entries=(
                        Entry(
                            select=enclosure,
                            ops=_ops(Init, OpenEnclosure),
                            id="init-open-enclosure",
                        ),
                    ),
                ),
            )

        opened = tuple(phase.name for phase in dome)
        mount_after = () if self.concurrent_dome_and_mount_init else opened

        # Cover opening either shares mount prerequisites or waits for dome and mount.
        optics_after = (
            mount_after if self.concurrent_mount_and_mirror_cover_init else (*opened, "mount")
        )

        phases = (
            *dome,
            Phase(
                name="mount",
                after=mount_after,
                entries=(
                    Entry(select=Supports(supports=FollowTarget), ops=_ops(Init), id="init-mount"),
                ),
            ),
            Phase(
                name="optics",
                after=optics_after,
                entries=(
                    Entry(
                        select=Supports(supports=CloseMirrorCover),
                        ops=_ops(OpenMirrorCover),
                        id="open-mirror-cover",
                    ),
                ),
            ),
        )

        # Any attempted bring-up command arms one halt on failure or domain abort.
        halt = CleanupSpec(
            name="halt",
            entries=(
                Entry(select=IsKind(kind="any"), ops=_ops(Stop, optional=True, fail_fast=False)),
            ),
            when="failure_or_cancelled",
            armed_by=tuple(
                entry.id for phase in phases for entry in phase.entries if entry.id is not None
            ),
        )

        return LifecycleWorkflow(name="init", phases=phases, fail_fast=True, cleanup=(halt,))

    def _shutdown_table(self) -> LifecycleWorkflow:
        enclosure = Supports(supports=CloseEnclosure)
        always = self.always_deinit_dome

        # When always is set, failed closure must still allow deinitialization.
        if self.concurrent_dome_deinit_close:
            closing = (
                Phase(
                    name="enclosure-close",
                    after=("halt",),
                    entries=(Entry(select=enclosure, ops=_ops(CloseEnclosure)),),
                ),
                Phase(
                    name="enclosure-deinit",
                    after=("halt",),
                    entries=(Entry(select=enclosure, ops=_ops(Deinit)),),
                ),
            )
        else:
            closing = (
                Phase(
                    name="enclosure",
                    after=("halt",),
                    entries=(
                        Entry(
                            select=enclosure,
                            ops=(
                                *_ops(CloseEnclosure),
                                *_ops(Deinit, sequence="completion" if always else "success"),
                            ),
                        ),
                    ),
                ),
            )

        return LifecycleWorkflow(
            name="shutdown",
            fail_fast=not always,
            phases=(
                Phase(
                    name="optics",
                    entries=(
                        Entry(
                            select=Supports(supports=CloseMirrorCover), ops=_ops(CloseMirrorCover)
                        ),
                    ),
                ),
                Phase(
                    name="mount",
                    after=("optics",),
                    entries=(Entry(select=Supports(supports=FollowTarget), ops=_ops(Deinit)),),
                ),
                # Attempt enclosure Stop before closure; failure must not block closing.
                Phase(
                    name="halt",
                    after=(("optics",) if self.concurrent_dome_and_mount_deinit else ("mount",)),
                    entries=(
                        Entry(select=enclosure, ops=_ops(Stop, optional=True, fail_fast=False)),
                    ),
                ),
                *closing,
            ),
        )


def compose_deadlines(
    definition: SensorDefinition, generated: tuple[DeadlineRule, ...]
) -> SensorDefinition:
    """Copy a definition with generated rules appended unless an authored
    rule replaces them.

    Replacement uses the target and command pair. No device facts are required.
    """
    authored = {(rule.target, rule.command) for rule in definition.deadlines}
    kept = tuple(rule for rule in generated if (rule.target, rule.command) not in authored)

    return definition.model_copy(update={"deadlines": definition.deadlines + kept})


def compose_tables(
    definition: SensorDefinition, generated: tuple[LifecycleWorkflow, ...], sensor: BoundSensor
) -> tuple[LifecycleWorkflow, ...]:
    """Return authored tables followed by unmatched generated tables pruned
    to the sensor.

    Authored tables replace generated ones by name and are never pruned. In
    generated tables, remove empty entries but retain phases and their
    ordering. Prune cleanup entries and trigger ids; drop cleanup with no
    entries or with every formerly nonempty trigger removed. Preserve
    unconditional arming.

    Raises:
        ValueError: The definition with composed tables fails validation.
    """
    authored = {table.name for table in definition.tables}
    tables = definition.tables + tuple(
        _pruned(table, sensor) for table in generated if table.name not in authored
    )

    definition.model_copy(update={"tables": tables}).check()

    return tables


def _pruned(table: LifecycleWorkflow, sensor: BoundSensor) -> LifecycleWorkflow:
    """Copy a generated table without empty selections, retaining its
    phases.
    """
    phases = tuple(
        phase.model_copy(
            update={"entries": tuple(entry for entry in phase.entries if entry.targets(sensor))}
        )
        for phase in table.phases
    )
    kept = {entry.id for phase in phases for entry in phase.entries}
    cleanup = tuple(
        spec
        for spec in (_pruned_cleanup(spec, kept, sensor) for spec in table.cleanup)
        if spec is not None
    )

    return table.model_copy(update={"phases": phases, "cleanup": cleanup})


def _pruned_cleanup(
    spec: CleanupSpec, kept: set[str | None], sensor: BoundSensor
) -> CleanupSpec | None:
    """Prune cleanup entries and trigger ids, or return `None` when
    unusable.

    Drop specs with no remaining entries or with all formerly nonempty triggers
    removed. Preserve `None` and explicitly empty arming tuples.
    """
    entries = tuple(entry for entry in spec.entries if entry.targets(sensor))
    armed_by = (
        None if spec.armed_by is None else tuple(name for name in spec.armed_by if name in kept)
    )

    if not entries or (spec.armed_by and not armed_by):
        return None

    return spec.model_copy(update={"entries": entries, "armed_by": armed_by})


def _ops(*commands: type[DeviceCommand], **spec) -> tuple[OpSpec, ...]:
    """Build default-argument commands omitted when the target lacks
    support.
    """
    return tuple(OpSpec(command=command(), unsupported="omit", **spec) for command in commands)
