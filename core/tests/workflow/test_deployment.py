# SPDX-License-Identifier: Apache-2.0
"""
The config layer: shorthands, round-tripping, and the promise that
config errors are load-time errors rather than 2am surprises.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from sensorkit.workflow import SensorPlan

MINIMAL = """
sensor:
  name: s
  attachments: {tcs-1: mount}
  parts:
    - name: ota
      parts:
        - {name: cam, instrument: cam-1}
"""


def test_demo_config_round_trips(config):
    assert SensorPlan.from_yaml(config.to_yaml()) == config


def test_table_name_comes_from_its_mapping_key(config):
    assert config.tables["init"].name == "init"
    # ...and is excluded from the dump, so the key stays the one source.
    assert "name: init" not in config.to_yaml()


def test_attachment_mapping_shorthand_expands(config):
    tcs = [a for a in config.sensor.attachments if a.ref == "tcs-1"]
    assert [a.trait for a in tcs] == ["mount"]


def test_unknown_keys_are_load_time_errors():
    with pytest.raises(ValidationError, match="extra_forbidden|Extra inputs"):
        SensorPlan.from_yaml(MINIMAL + "\nwibble: 1\n")


def test_tables_are_compiled_against_the_sensor_at_load():
    with pytest.raises(ValidationError, match="table 'bad'.*unknown device"):
        SensorPlan.from_yaml(MINIMAL + """
tables:
  bad:
    on_failure: stop
    phases:
      - name: p
        entries:
          - {device: not-a-device, ops: connect}
""")


def test_a_table_must_declare_its_failure_policy():
    with pytest.raises(ValidationError, match="on_failure"):
        SensorPlan.from_yaml(MINIMAL + """
tables:
  bad:
    phases:
      - name: p
        entries:
          - {match: all, ops: connect}
""")


def test_aliases_are_cross_checked_against_the_structure():
    with pytest.raises(ValidationError, match="aliases name unknown device"):
        SensorPlan.from_yaml(MINIMAL + """
aliases: {science: cam-2}
""")


def test_compile_is_available_by_table_name(config):
    assert len(config.compile("init").nodes) > 0
    with pytest.raises(KeyError):
        config.compile("nope")
