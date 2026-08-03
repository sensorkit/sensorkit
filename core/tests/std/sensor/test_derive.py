# SPDX-License-Identifier: Apache-2.0
"""Deriving a workflow SensorPlan from the sensors: section.

Pure data in, pure data out — no event loop, no controller, no devices.

The golden files are where a policy flag's meaning is written down once. Each
renders, per flag combination, the table it produces beside the graph that table
compiles to, so a change to either shows up as a diff rather than as a behavioural
surprise at 2am.

Regenerate deliberately, and read the diff::

    SK_REGEN=1 uv run pytest
"""
from __future__ import annotations

import itertools

import pytest
import yaml

from sensorkit.astro.common import SitePosition
from sensorkit.core.entity import DeviceDetails
from sensorkit.std.instrument import CameraCapture, ConfigureCameraCooler
from sensorkit.std.sensor.config import SensorConfig, SensorDevices, SensorPolicies
from sensorkit.std.sensor.derive import (
    OTA,
    PRIMARY,
    capability_index,
    derive_plan,
    derive_structure,
    derive_tables,
)
from sensorkit.std.traits import Connect, Disconnect
from sensorkit.workflow import (
    DeviceIndex,
    Graph,
    PhaseTable,
    Topology,
    compile_table,
    format_graph,
)

FULL = SensorDevices(mount="tcs-1", camera="cam-1", focuser="foc-1", rotator="rot-1",
                     filter_wheel="fw-1", mirror_cover="cover-1", dome="dome-1")

INIT_FLAGS = ("concurrent_dome_init_open",
              "concurrent_dome_and_mount_init",
              "concurrent_mount_and_mirror_cover_init")

SHUTDOWN_FLAGS = ("concurrent_dome_deinit_close",
                  "concurrent_dome_and_mount_deinit",
                  "always_deinit_dome")


def index(devices: SensorDevices = FULL) -> DeviceIndex:
    return DeviceIndex(derive_structure("MySensor", devices))


def table(name: str, **flags: bool) -> PhaseTable:
    return derive_tables(SensorPolicies(**flags))[name]


def edges(graph: Graph) -> set[tuple[str, str]]:
    """Every (dependency, dependent) pair, as "ref.op" labels."""
    label = {n.id: f"{n.payload.ref}.{n.payload.op}" for n in graph.nodes}
    return {(label[d], label[n.id])
            for n in graph.nodes for d in graph.deps[n.id]}


def render(devices: DeviceIndex, name: str, **flags: bool) -> str:
    t = table(name, **flags)
    body = yaml.safe_dump(t.model_dump(mode="json", exclude_defaults=True),
                          sort_keys=False)
    return f"{body}--- graph ---\n{format_graph(compile_table(devices, t))}"


def sweep(name: str, flags: tuple[str, ...]) -> str:
    """One section per combination of the flags that change this table."""
    devices = index()
    sections = []

    for values in itertools.product((False, True), repeat=len(flags)):
        combo = dict(zip(flags, values, strict=True))
        header = "  ".join(f"{f}={'T' if v else 'F'}" for f, v in combo.items())
        sections.append(f"=== {header} ===\n{render(devices, name, **combo)}")

    return "\n".join(sections)


# ---- golden: the derived structure --------------------------------------

def test_structure_golden(assert_golden):
    cases = {
        "every device": FULL,
        "no enclosure": FULL.model_copy(update={"dome": None}),
        "no camera": FULL.model_copy(update={"camera": None}),
        "mount only": SensorDevices(mount="tcs-1"),
    }
    rendered = "\n".join(
        f"=== {what} ===\n" + yaml.safe_dump(
            derive_structure("MySensor", devices).model_dump(
                mode="json", exclude_defaults=True),
            sort_keys=False)
        for what, devices in cases.items())

    assert_golden("derived-structure", rendered)


# ---- golden: the derived tables -----------------------------------------

def test_init_table_golden(assert_golden):
    assert_golden("derived-init", sweep("init", INIT_FLAGS))


def test_shutdown_table_golden(assert_golden):
    assert_golden("derived-shutdown", sweep("shutdown", SHUTDOWN_FLAGS))


def test_invariant_tables_golden(assert_golden):
    """recover and stop read no policy at all."""
    devices = index()
    assert_golden("derived-invariant", "\n".join(
        f"=== {name} ===\n{render(devices, name)}"
        for name in ("recover", "stop")))


# ---- structure ----------------------------------------------------------

def test_placement_decides_shared_versus_private():
    """What a command reaches is decided by where its device is claimed.

    The wheel is claimed at the leaf, so a filter is the instrument's own; the
    mount and enclosure are claimed at the root, so a target is the step's. The
    optical train sits above the instrument, so it is shared too — with nothing,
    on a one-instrument sensor, which is the same fact.
    """
    view = Topology(derive_structure("MySensor", FULL)).instruments[(OTA, PRIMARY)]

    assert view.private == {"cam-1", "fw-1"}
    assert set(view.shared) == {"tcs-1", "dome-1", "foc-1", "rot-1", "cover-1"}


def test_absent_devices_contribute_no_claims():
    devices = index(SensorDevices(mount="tcs-1", camera="cam-1"))

    assert set(devices.refs) == {"tcs-1", "cam-1"}


def test_no_camera_yields_no_instrument():
    """sensor_collect's own guard, said structurally."""
    assert not Topology(derive_structure("MySensor", FULL.model_copy(
        update={"camera": None}))).instruments


def test_filter_wheel_outlives_a_missing_camera():
    """A wheel with no instrument to own it stays addressable per-device."""
    devices = index(SensorDevices(filter_wheel="fw-1"))

    assert [n.trait for n in devices.claiming("filter_changer")] == ["filter_changer"]


def test_no_optical_devices_yields_no_assembly():
    assert derive_structure("MySensor", SensorDevices(mount="tcs-1")).parts == []


# ---- tables -------------------------------------------------------------

def test_standby_is_init():
    tables = derive_tables(SensorPolicies())

    assert tables["standby"].phases == tables["init"].phases
    assert tables["standby"].name == "standby"


def test_dome_serializes_its_own_ops_by_default():
    """One entry, two ops: serial per device by construction."""
    assert ("dome-1.Init", "dome-1.OpenEnclosure") in edges(
        compile_table(index(), table("init")))


def test_concurrent_dome_init_open_splits_the_entry():
    assert ("dome-1.Init", "dome-1.OpenEnclosure") not in edges(
        compile_table(index(), table("init", concurrent_dome_init_open=True)))


def test_mirror_cover_waits_on_everything_under_way():
    """The wait the mount does not do is still the mirror cover's to do.

    `concurrent_dome_and_mount_init` unhooks the mount from the enclosure, so a
    mirror cover following only the mount would start against a moving dome.
    Today's wait spans every step begun so far, and the phase names both.
    """
    linked = edges(compile_table(
        index(), table("init", concurrent_dome_and_mount_init=True)))

    assert ("tcs-1.Init", "cover-1.OpenMirrorCover") in linked
    assert ("dome-1.OpenEnclosure", "cover-1.OpenMirrorCover") in linked


def test_concurrent_mount_and_mirror_cover_init_shares_the_mount_precondition():
    graph = compile_table(index(), table(
        "init", concurrent_mount_and_mirror_cover_init=True))
    linked = edges(graph)

    assert ("tcs-1.Init", "cover-1.OpenMirrorCover") not in linked
    # Both now start on what the mount was waiting for, and on nothing else.
    assert {d for d, n in linked if n == "cover-1.OpenMirrorCover"} == {
        d for d, n in linked if n == "tcs-1.Init"}


def test_halt_precedes_the_close_under_every_policy():
    """Nothing may abort a close mid-flight, however concurrent the teardown."""
    for concurrent in (False, True):
        graph = compile_table(index(), table(
            "shutdown", concurrent_dome_and_mount_deinit=concurrent))

        assert ("dome-1.Stop", "dome-1.CloseEnclosure") in edges(graph)


def test_concurrent_dome_and_mount_deinit_shares_the_mount_precondition():
    linked = edges(compile_table(index(), table(
        "shutdown", concurrent_dome_and_mount_deinit=True)))

    assert ("tcs-1.Deinit", "dome-1.Stop") not in linked
    assert ("cover-1.CloseMirrorCover", "dome-1.Stop") in linked


@pytest.mark.parametrize(("always", "expected"), [(True, "skip"), (False, "stop")])
def test_always_deinit_dome_is_the_tables_failure_policy(always, expected):
    assert table("shutdown", always_deinit_dome=always).on_failure == expected


def test_recover_gathers_every_connect_before_any_stop():
    """One entry with both ops would serialize per device, letting one device's
    stop precede another's connect."""
    linked = edges(compile_table(index(), table("recover")))

    assert ("tcs-1.Connect", "dome-1.Stop") in linked
    assert ("dome-1.Connect", "tcs-1.Stop") in linked


def test_stop_addresses_only_what_moves():
    graph = compile_table(index(), table("stop"))

    assert {n.payload.ref for n in graph.nodes} == {"tcs-1", "dome-1", "cover-1"}
    assert all(n.optional for n in graph.nodes)


# ---- the plan -----------------------------------------------------------

def test_derive_plan_compiles_every_table():
    """SensorPlan validation compiles each table against the structure, so a
    generator that emits an unsatisfiable entry fails here."""
    plan = derive_plan(SensorConfig(
        controller_name="MySensor",
        devices=FULL,
        site_position=SitePosition(latitude_degrees=0.0, longitude_degrees=0.0,
                                   altitude_km=0.0),
        policies=SensorPolicies(always_deinit_dome=True),
    ))

    assert plan.sensor.name == "MySensor"
    assert set(plan.tables) == {"init", "standby", "shutdown", "recover", "stop"}
    assert all(name == t.name for name, t in plan.tables.items())
    assert plan.compile("shutdown").nodes


def test_derive_plan_survives_a_sensor_with_no_devices():
    plan = derive_plan(SensorConfig(
        controller_name="Empty",
        devices=SensorDevices(),
        site_position=SitePosition(latitude_degrees=0.0, longitude_degrees=0.0,
                                   altitude_km=0.0),
    ))

    assert plan.compile("init").nodes == ()


# ---- the capability index -----------------------------------------------

def test_capability_index_carries_archetype_traits_and_commands():
    details = DeviceDetails(
        supported_commands=frozenset({
            CameraCapture.model_tag(), ConfigureCameraCooler.model_tag(),
            Connect.model_tag(), Disconnect.model_tag()}),
        published_keywords=frozenset(),
    )
    caps = capability_index({"cam-1": details})["cam-1"]

    # The archetype the device matches, plus the sub-traits it also satisfies.
    assert "camera" in caps.traits
    assert "MustConnect" in caps.traits
    assert caps.commands == details.supported_commands
    # No task in this scope carries a keyword predicate, so nothing subscribes.
    assert not caps.keywords


def test_capability_index_of_an_unrecognized_device():
    caps = capability_index({"odd-1": DeviceDetails(
        supported_commands=frozenset(), published_keywords=frozenset())})

    assert caps["odd-1"].traits == frozenset()
