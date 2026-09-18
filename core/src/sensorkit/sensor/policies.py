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
from sensorkit.sensor.lifecycle import (
    CleanupSpec,
    Entry,
    LifecycleWorkflow,
    OpSpec,
    Phase,
)
from sensorkit.sensor.selection import IsKind, Supports
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
from sensorkit.std.traits import Connect, Deinit, Init, Stop


class SensorPolicies(BaseModel, frozen=True, extra="forbid"):
    """Site options for generating standard lifecycle tables and deadlines.

    Every command has a deadline rule, through a default for commands no
    other rule names.
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

        `standby` currently duplicates `init` under a separate name.
        Unsupported commands are omitted during lowering. `compose_tables`
        removes generated entries that select no equipment and applies authored
        table replacements.
        """
        init = self._init_table()
        every = IsKind(kind="any")
        # Complete reconnect attempts before stopping motion.
        recover = LifecycleWorkflow(
            name="recover", fail_fast=False,
            phases=(
                Phase(name="reconnect", entries=(
                    Entry(select=every, ops=_ops(Connect)),)),
                Phase(name="halt", entries=(
                    Entry(select=every, ops=_ops(Stop)),)),
            ))

        return (init, init.model_copy(update={"name": "standby"}),
                self._shutdown_table(), recover)


    def _init_table(self) -> LifecycleWorkflow:
        # A dome lacking Init must still open and close.
        enclosure = Supports(supports=CloseEnclosure)

        # Separate phases permit overlap without duplicate targets in a phase.
        if self.concurrent_dome_init_open:
            dome = (
                Phase(name="enclosure-init", after=(), entries=(
                    Entry(select=enclosure, ops=_ops(Init), id="init-enclosure"),)),
                Phase(name="enclosure-open", after=(), entries=(
                    Entry(select=enclosure, ops=_ops(OpenEnclosure), id="open-enclosure"),)),
            )
        else:
            dome = (Phase(name="enclosure", entries=(
                Entry(select=enclosure, ops=_ops(Init, OpenEnclosure),
                      id="init-open-enclosure"),)),)

        opened = tuple(phase.name for phase in dome)
        mount_after = () if self.concurrent_dome_and_mount_init else opened

        # Cover opening either shares mount prerequisites or waits for dome and mount.
        optics_after = (mount_after
                        if self.concurrent_mount_and_mirror_cover_init
                        else (*opened, "mount"))

        phases = (
            *dome,
            Phase(name="mount", after=mount_after, entries=(
                Entry(select=Supports(supports=FollowTarget), ops=_ops(Init),
                      id="init-mount"),)),
            Phase(name="optics", after=optics_after, entries=(
                Entry(select=Supports(supports=CloseMirrorCover), ops=_ops(OpenMirrorCover),
                      id="open-mirror-cover"),)),
        )

        # Any attempted bring-up command arms one halt on failure or domain abort.
        halt = CleanupSpec(
            name="halt", entries=(Entry(
                select=IsKind(kind="any"),
                ops=_ops(Stop, optional=True, fail_fast=False)),),
            when="failure_or_cancelled",
            armed_by=tuple(entry.id for phase in phases
                           for entry in phase.entries if entry.id is not None))

        return LifecycleWorkflow(name="init", phases=phases, fail_fast=True,
                                 cleanup=(halt,))

    def _shutdown_table(self) -> LifecycleWorkflow:
        enclosure = Supports(supports=CloseEnclosure)
        always = self.always_deinit_dome

        # Continue after failed closure with completion sequencing and no fail-fast.
        if self.concurrent_dome_deinit_close:
            closing = (
                Phase(name="enclosure-close", after=("halt",), entries=(
                    Entry(select=enclosure, ops=_ops(CloseEnclosure)),)),
                Phase(name="enclosure-deinit", after=("halt",), entries=(
                    Entry(select=enclosure, ops=_ops(Deinit)),)),
            )
        else:
            closing = (Phase(name="enclosure", after=("halt",), entries=(
                Entry(select=enclosure, ops=(
                    *_ops(CloseEnclosure),
                    *_ops(Deinit,
                          sequence="completion" if always else "success"),
                )),)),)

        return LifecycleWorkflow(
            name="shutdown", fail_fast=not always,
            phases=(
                Phase(name="optics", entries=(
                    Entry(select=Supports(supports=CloseMirrorCover), ops=_ops(CloseMirrorCover)),)),
                Phase(name="mount", after=("optics",), entries=(
                    Entry(select=Supports(supports=FollowTarget), ops=_ops(Deinit)),)),
                # Attempt enclosure Stop before closure; failure must not block closing.
                Phase(name="halt",
                      after=(("optics",)
                             if self.concurrent_dome_and_mount_deinit
                             else ("mount",)),
                      entries=(Entry(select=enclosure, ops=_ops(Stop, optional=True,
                                                               fail_fast=False)),)),
                *closing,
            ))










def _ops(*commands: type[DeviceCommand], **spec) -> tuple[OpSpec, ...]:
    """Build default-argument commands omitted when the target lacks
    support.
    """
    return tuple(OpSpec(command=command(), unsupported="omit", **spec)
                 for command in commands)
