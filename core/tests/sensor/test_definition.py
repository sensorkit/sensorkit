# SPDX-License-Identifier: Apache-2.0
"""Loading a document with no device present, and the names it has to
resolve."""
from __future__ import annotations

import textwrap


from sensorkit.sensor.definition import (
    SensorDefinition,
)
from sensorkit.sensor.selection import Supports
from sensorkit.std.traits import Connect, Init

MINIMAL = """
sensor:
  name: one
  components:
    - device: mount
"""


def document(tables: str) -> str:
    """A loadable document carrying one authored tables block."""
    return MINIMAL + textwrap.dedent(tables)


def test_a_definition_loads_with_no_devices_present(definition):
    assert definition.sensor.name == "demo"
    assert [t.name for t in definition.tables] == ["bring-up", "shutdown"]


def test_a_table_takes_its_name_from_its_mapping_key(definition):
    assert definition.tables[0].name == "bring-up"


def test_a_bare_command_name_is_an_operation_with_no_arguments(definition):
    ops = definition.tables[0].phases[0].entries[0].ops

    assert ops == (type(ops[0])(command=Connect()),)


def test_one_operation_where_a_list_is_meant(definition):
    assert len(definition.tables[0].phases[0].entries[0].ops) == 1


def test_a_parameterized_command_keeps_what_it_was_given(definition):
    initialize = definition.tables[0].phases[1].entries[0].ops[1]

    assert initialize.command == Init()
    assert initialize.timeout_s == 45.0


def test_a_bare_name_is_a_require_clause_with_the_default_join(definition):
    clause = definition.tables[0].phases[2].entries[0].require[0]

    assert (clause.name, clause.join, clause.on) == ("initialize", "all",
                                                     "completion")


def test_a_scalar_stands_for_a_one_name_list(definition):
    mount = definition.sensor.components[0]

    assert mount.traits == ("MustConnect",)
    assert mount.tags == ("primary",)


def test_a_table_authoring_a_capability_predicate_loads(definition):
    home = definition.tables[0].phases[2].entries[0]

    assert home.select == Supports(supports="Home")




def test_an_entry_narrows_with_exclude(definition):
    entry = definition.tables[0].phases[1].entries[1]

    assert entry.exclude is not None


def test_a_cleanup_spec_carries_its_trigger_and_arming(definition):
    halt = definition.tables[0].cleanup[0]

    assert (halt.name, halt.when, halt.armed_by) == ("halt", "failure",
                                                     ("init-instruments",))


def test_deadline_rules_are_namespaced_by_the_key_they_name(definition):
    assert [r.target for r in definition.deadlines] == [
        ("trait", "MustConnect"), ("device", "mount"), ("any", None)]


















def test_a_require_clause_may_name_a_later_peer():
    definition = SensorDefinition.from_yaml(document("""
        tables:
          t:
            fail_fast: true
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
                    require: later
                  - select: {trait: MustConnect}
                    ops: Init
                    id: later
        """))

    assert definition.tables[0].phases[0].entries[0].require[0].name == "later"


def test_a_cleanup_clause_names_an_entry_of_its_own_spec():
    definition = SensorDefinition.from_yaml(document("""
        tables:
          t:
            fail_fast: true
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
            cleanup:
              - name: halt
                entries:
                  - select: {kind: any}
                    ops: Stop
                    require: parked
                  - select: {kind: any}
                    ops: Disconnect
                    id: parked
        """))
    halt = definition.tables[0].cleanup[0]

    assert halt.entries[0].require[0].name == "parked"








def test_two_cleanups_may_use_one_entry_id():
    definition = SensorDefinition.from_yaml(document("""
        tables:
          t:
            fail_fast: true
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
            cleanup:
              - name: halt
                entries:
                  - select: {kind: any}
                    ops: Stop
                    id: same
              - name: park
                entries:
                  - select: {kind: any}
                    ops: Disconnect
                    id: same
        """))

    cleanup = definition.tables[0].cleanup

    assert [spec.name for spec in cleanup] == ["halt", "park"]




















def test_an_omitted_after_follows_the_preceding_phase():
    # Nothing here asserts a compiled edge, only that the implicit reference
    # resolves and the first phase follows nothing.
    definition = SensorDefinition.from_yaml(document("""
        tables:
          t:
            fail_fast: true
            phases:
              - name: first
                entries:
                  - select: {kind: any}
                    ops: Connect
              - name: second
                entries:
                  - select: {trait: MustConnect}
                    ops: Init
        """))

    assert [p.after for p in definition.tables[0].phases] == [None, None]






def test_loading_normalizes_and_does_not_round_trip(definition):
    assert isinstance(definition, SensorDefinition)
    assert not hasattr(definition, "to_yaml")
    assert isinstance(definition.tables, tuple)
