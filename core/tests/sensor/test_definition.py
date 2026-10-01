# SPDX-License-Identifier: Apache-2.0
"""Loading a document with no device present, and the names it has to
resolve."""
from __future__ import annotations

import re
import textwrap

import pytest
from pydantic import ValidationError

from sensorkit.sensor.definition import (
    SensorDefinition,
)
from sensorkit.sensor.selection import Supports
from sensorkit.sensor.workflow import DeadlineRule
from sensorkit.std.traits import Connect, Init

MINIMAL = """
sensor:
  components:
    - device: mount
"""


def document(tables: str) -> str:
    """A loadable document carrying one authored tables block."""
    return MINIMAL + textwrap.dedent(tables)


def test_a_definition_loads_with_no_devices_present(definition):
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


def test_a_selection_is_not_evaluated_at_load(definition, at):
    # Loading answered no capability question, so the predicate is still
    # waiting for facts binding has not established yet.
    entry = definition.tables[0].phases[2].entries[0]

    with pytest.raises(ValueError, match="none was given"):
        entry.select.matches(at("mount"))


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


def test_a_deadline_rule_for_a_device_the_structure_lacks_fails_at_load():
    with pytest.raises(ValueError, match="does not hold: dome, mnt"):
        SensorDefinition.from_yaml(MINIMAL + textwrap.dedent("""
            deadlines:
              - device: mount
                command: Home
                seconds: 300.0
              - device: mnt
                command: Home
                seconds: 300.0
              - device: dome
                command: Connect
                seconds: 30.0
              - trait: NotAPosition
                command: Connect
                seconds: 30.0
            """))


def test_a_deadline_rule_is_checked_in_a_definition_built_in_python(
        definition):
    stray = DeadlineRule(target=("device", "cam-spare"), command="Init",
                         seconds=10.0)

    with pytest.raises(ValueError, match="does not hold: cam-spare"):
        definition.model_copy(
            update={"deadlines": (*definition.deadlines, stray)}).check()


def test_a_malformed_structure_fails_at_load():
    with pytest.raises(ValueError, match="placed twice"):
        SensorDefinition.from_yaml("""
            sensor:
              components:
                - device: mount
                - device: mount
            """)


def test_an_unknown_command_fails_at_parse():
    with pytest.raises(ValidationError, match="no model resolved"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: NeverDeclared
            """))


def test_a_table_must_say_how_far_a_failure_spreads():
    with pytest.raises(ValidationError, match="fail_fast"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
            """))


def test_an_entry_declares_at_least_one_operation():
    with pytest.raises(ValidationError):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: []
            """))


def test_an_unknown_phase_in_after_raises():
    with pytest.raises(ValueError, match="after names nothing: nowhere"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    after: [nowhere]
                    entries:
                      - select: {kind: any}
                        ops: Connect
            """))


def test_an_unresolved_require_clause_raises():
    with pytest.raises(ValueError, match="require names nothing: ghost"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        require: ghost
            """))


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


def test_a_cleanup_clause_naming_a_phase_raises():
    with pytest.raises(ValueError,
                       match="cleanup 'halt' require names entries outside "
                             "it: p"):
        SensorDefinition.from_yaml(document("""
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
                        require: p
            """))


def test_a_cleanup_clause_naming_a_phase_entry_raises():
    with pytest.raises(ValueError,
                       match="cleanup 'halt' require names entries outside "
                             "it: connected"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        id: connected
                cleanup:
                  - name: halt
                    entries:
                      - select: {kind: any}
                        ops: Stop
                        require: connected
            """))


def test_a_cycle_within_one_cleanup_raises():
    with pytest.raises(ValueError, match=re.escape(
            "entry 'one' -> entry 'two' -> entry 'one'")):
        SensorDefinition.from_yaml(document("""
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
                        id: one
                        require: two
                      - select: {trait: MustConnect}
                        ops: Disconnect
                        id: two
                        require: one
            """))


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


def test_one_cleanup_using_an_entry_id_twice_raises():
    with pytest.raises(ValueError,
                       match="cleanup 'halt' uses an entry id twice: same"):
        SensorDefinition.from_yaml(document("""
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
                      - select: {trait: MustConnect}
                        ops: Disconnect
                        id: same
            """))


def test_a_phase_clause_naming_a_cleanup_entry_raises():
    with pytest.raises(ValueError, match="require names nothing: stopped"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        require: stopped
                cleanup:
                  - name: halt
                    entries:
                      - select: {kind: any}
                        ops: Stop
                        id: stopped
            """))


def test_cleanup_arms_on_an_entry_id_the_table_declares():
    with pytest.raises(ValueError, match="armed_by names nothing: ghost"):
        SensorDefinition.from_yaml(document("""
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
                    armed_by: [ghost]
                    entries:
                      - select: {kind: any}
                        ops: Stop
            """))


def test_a_name_used_by_two_entries_raises():
    with pytest.raises(ValueError, match="entry id is used twice: same"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        id: same
                      - select: {trait: MustConnect}
                        ops: Init
                        id: same
            """))


def test_two_phases_of_one_name_raise():
    with pytest.raises(ValueError, match="phase is named twice: p"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Init
            """))


def test_a_phase_and_an_entry_of_one_name_raise():
    with pytest.raises(ValueError, match="phase and an entry share a name: p"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        id: p
                  - name: later
                    entries:
                      - select: {trait: MustConnect}
                        ops: Init
                        require: p
            """))


def test_a_cycle_among_phases_raises():
    with pytest.raises(ValueError, match=re.escape(
            "entry 'a[0]' -> phase 'b' -> entry 'b[0]' -> phase 'a' -> "
            "entry 'a[0]'")):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: a
                    after: [b]
                    entries:
                      - select: {kind: any}
                        ops: Connect
                  - name: b
                    after: [a]
                    entries:
                      - select: {trait: MustConnect}
                        ops: Init
            """))


def test_a_cycle_among_entries_raises():
    with pytest.raises(ValueError, match=re.escape(
            "entry 'first' -> entry 'second' -> entry 'first'")):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        id: first
                        require: second
                      - select: {trait: MustConnect}
                        ops: Init
                        id: second
                        require: first
            """))


def test_an_error_names_the_table_it_was_found_in():
    with pytest.raises(ValueError, match="table 't': require names nothing"):
        SensorDefinition.from_yaml(document("""
            tables:
              t:
                fail_fast: true
                phases:
                  - name: p
                    entries:
                      - select: {kind: any}
                        ops: Connect
                        require: ghost
            """))


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


def test_validate_accepts_a_definition_built_in_python(definition):
    definition.check()


def test_a_definition_rejects_an_unknown_field():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SensorDefinition.from_yaml(MINIMAL + "\naliases: {a: mount}\n")


def test_loading_normalizes_and_does_not_round_trip(definition):
    assert isinstance(definition, SensorDefinition)
    assert not hasattr(definition, "to_yaml")
    assert isinstance(definition.tables, tuple)
