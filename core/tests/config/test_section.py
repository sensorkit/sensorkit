# SPDX-License-Identifier: Apache-2.0
import pytest
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.config.parser import PARSER_VERSION, parse_config


class DialConfig(BaseModel):
    reading: int = 0


class LampConfig(BaseModel):
    lumens: int = 0


def resolve(**sections):
    """Parse a config carrying the given sections and return the per-entity KV models."""
    config = parse_config({"version": PARSER_VERSION, **sections})
    return dict(config.resolve_dynamic_sections().entity_kv)


def test_by_key_takes_the_id_from_the_file():
    sk.declare_config_section("parse_dial", DialConfig, id_source="by_key")

    entity_kv = resolve(parse_dial={"id": "north_dial", "reading": 4})

    assert entity_kv == {"north_dial": [DialConfig(reading=4)]}


def test_by_key_default_yields_to_the_file():
    sk.declare_config_section(
        "parse_lamp",
        LampConfig,
        id_source="by_key",
        id_default="lamp",
    )

    entity_kv = resolve(parse_lamp={"id": "porch_lamp", "lumens": 800})

    assert entity_kv == {"porch_lamp": [LampConfig(lumens=800)]}


def test_by_key_falls_back_to_the_default():
    sk.declare_config_section(
        "parse_beacon",
        LampConfig,
        id_source="by_key",
        id_default="beacon",
    )

    entity_kv = resolve(parse_beacon={"lumens": 12})

    assert entity_kv == {"beacon": [LampConfig(lumens=12)]}


def test_by_key_reads_a_declared_id_key():
    sk.declare_config_section(
        "parse_meter",
        DialConfig,
        id_source="by_key",
        id_key="target",
    )

    entity_kv = resolve(parse_meter={"target": "flow_meter", "reading": 9})

    assert entity_kv == {"flow_meter": [DialConfig(reading=9)]}


def test_by_subkey_names_each_entry():
    sk.declare_config_section("parse_dials", list[DialConfig], id_source="by_subkey")

    entity_kv = resolve(
        parse_dials=[{"id": "left", "reading": 1}, {"id": "right", "reading": 2}]
    )

    assert entity_kv == {"left": [DialConfig(reading=1)], "right": [DialConfig(reading=2)]}


def test_mapping_key_names_by_its_own_keys():
    sk.declare_config_section("parse_lamps", dict[str, LampConfig], id_source="mapping_key")

    entity_kv = resolve(parse_lamps={"hall": {"lumens": 3}, "porch": {"lumens": 4}})

    assert entity_kv == {"hall": [LampConfig(lumens=3)], "porch": [LampConfig(lumens=4)]}


def test_default_source_admits_no_id_from_the_file():
    sk.declare_config_section(
        "parse_gauge",
        DialConfig,
        id_source="default",
        id_default="gauge",
    )

    entity_kv = resolve(parse_gauge={"reading": 7})

    assert entity_kv == {"gauge": [DialConfig(reading=7)]}


def test_id_mapper_repeats_an_id_across_several_models():
    sk.declare_config_section(
        "parse_racks",
        dict[str, list[LampConfig]],
        id_source="mapping_key",
        id_mapper=lambda raw: (rack for rack, lamps in raw.items() for _ in lamps),
        model_mapper=lambda obj: (lamp for lamps in obj.values() for lamp in lamps),
    )

    entity_kv = resolve(parse_racks={"aisle": [{"lumens": 1}, {"lumens": 2}]})

    assert entity_kv == {"aisle": [LampConfig(lumens=1), LampConfig(lumens=2)]}


def test_id_key_needs_a_keyed_source():
    with pytest.raises(ValueError, match="id_key"):
        sk.declare_config_section(
            "parse_rejected",
            DialConfig,
            id_source="mapping_key",
            id_key="target",
        )


def test_id_default_needs_a_source_that_can_fall_back():
    with pytest.raises(ValueError, match="id_default"):
        sk.declare_config_section(
            "parse_rejected",
            list[DialConfig],
            id_source="by_subkey",
            id_default="dial",
        )


def test_default_source_needs_an_id_default():
    with pytest.raises(ValueError, match="id_default"):
        sk.declare_config_section("parse_rejected", DialConfig, id_source="default")


def test_unknown_id_source_rejected():
    with pytest.raises(ValueError, match="unknown ID source"):
        sk.declare_config_section("parse_rejected", DialConfig, id_source="by_vibes")


def test_duplicate_section_rejected():
    sk.declare_config_section("parse_twice", DialConfig, id_source="by_key")

    with pytest.raises(ValueError, match="already registered"):
        sk.declare_config_section("parse_twice", DialConfig, id_source="by_key")
