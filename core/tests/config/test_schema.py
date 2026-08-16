# SPDX-License-Identifier: Apache-2.0
from typing import Literal

import pydantic
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.config.parser import PARSER_VERSION
from sensorkit.config.schema import SCHEMA_DIALECT, config_json_schema


class WidgetConfig(BaseModel):
    """A widget."""

    size: int = 3


class GadgetConfig(BaseModel):
    port: int = 1


class GizmoConfig(BaseModel):
    target: str


class DoohickeyConfig(BaseModel):
    label: str = "doohickey"


class ThingamajigConfig(BaseModel):
    weight: float = 1.0


class SprocketConfig(BaseModel):
    teeth: int = 8


class HubConfig(BaseModel):
    sprocket: SprocketConfig = pydantic.Field(default_factory=SprocketConfig)


class HopperConfig(BaseModel, extra="allow"):
    tag: str = ""


class LeftValve(BaseModel):
    side: Literal["left"] = "left"
    throw: int = 0


class RightValve(BaseModel):
    side: Literal["right"] = "right"
    lift: int = 0


class ValveConfig(BaseModel):
    valve: LeftValve | RightValve = pydantic.Field(discriminator="side")


def collect_refs(node):
    """Yield every reference reachable from a schema node."""
    if isinstance(node, dict):
        if isinstance(ref := node.get("$ref"), str):
            yield ref

        for value in node.values():
            yield from collect_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from collect_refs(item)


def test_schema_root():
    schema = config_json_schema()

    assert schema["$schema"] == SCHEMA_DIALECT
    assert schema["type"] == "object"

    # A top-level key that no section claims is an error at load time.
    assert schema["additionalProperties"] is False

    assert schema["required"] == ["version"]
    assert schema["properties"]["version"]["const"] == PARSER_VERSION
    assert "default" not in schema["properties"]["version"]

    # Fixed keys of the base model and dynamic sections sit side by side.
    assert {"sensorkit", "services", "globals"} <= set(schema["properties"])
    assert {"automation", "data_flow", "config"} <= set(schema["properties"])


def test_schema_definitions_resolve():
    schema = config_json_schema()
    defs = schema["$defs"]

    assert defs
    assert "SensorKitBaseConfig" not in defs

    for ref in collect_refs(schema):
        assert ref.removeprefix("#/$defs/") in defs


def test_schema_defines_only_what_it_references():
    schema = config_json_schema()
    referenced = {ref.removeprefix("#/$defs/") for ref in collect_refs(schema)}

    assert set(schema["$defs"]) == referenced


def test_schema_closes_objects():
    sk.declare_config_section(
        "schema_hub",
        HubConfig,
        id_source="by_key",
    )

    schema = config_json_schema()

    assert schema["properties"]["schema_hub"]["additionalProperties"] is False
    assert schema["$defs"]["SprocketConfig"]["additionalProperties"] is False


def test_schema_leaves_open_models_open():
    sk.declare_config_section(
        "schema_hopper",
        HopperConfig,
        id_source="by_key",
    )

    assert config_json_schema()["properties"]["schema_hopper"]["additionalProperties"] is True


def test_schema_selects_union_branch_by_discriminator():
    sk.declare_config_section(
        "schema_valve",
        ValveConfig,
        id_source="by_key",
    )

    schema = config_json_schema()
    union = schema["properties"]["schema_valve"]["properties"]["valve"]

    # Trying every branch reports a mismatch against the whole object, so the branch the
    # discriminating property names is the only one that applies.
    assert "oneOf" not in union
    assert union["required"] == ["side"]
    assert union["properties"]["side"]["enum"] == ["left", "right"]

    branches = {
        branch["if"]["properties"]["side"]["const"]: branch["then"]["$ref"]
        for branch in union["allOf"]
    }
    assert branches == {"left": "#/$defs/LeftValve", "right": "#/$defs/RightValve"}

    # The branch carries the closed property list, so only offending keys are reported.
    assert schema["$defs"]["LeftValve"]["additionalProperties"] is False


def test_schema_leaves_mapping_sections_open():
    sk.declare_config_section("schema_bins", dict[str, int], id_source="mapping_key")

    section = config_json_schema()["properties"]["schema_bins"]

    assert section["additionalProperties"] == {"type": "integer"}


def test_schema_titled():
    assert config_json_schema(title="A site")["title"] == "A site"


def test_schema_id_key_on_each_entry():
    sk.declare_config_section("schema_widgets", list[WidgetConfig], id_source="by_subkey")

    schema = config_json_schema()
    section = schema["properties"]["schema_widgets"]

    assert section["type"] == "array"

    # The key joins the model's own properties, leaving an entry that can be closed.
    entry = section["items"]
    assert entry["properties"]["id"]["type"] == "string"
    assert "size" in entry["properties"]
    assert entry["required"] == ["id"]
    assert entry["additionalProperties"] is False

    # Merging consumes the model's definition.
    assert "WidgetConfig" not in schema["$defs"]


def test_schema_id_key_on_the_section():
    sk.declare_config_section("schema_gadget", GadgetConfig, id_source="by_key")

    section = config_json_schema()["properties"]["schema_gadget"]

    assert section["properties"]["id"]["type"] == "string"
    assert "port" in section["properties"]
    assert section["required"] == ["id"]


def test_schema_id_key_with_a_fallback_name():
    sk.declare_config_section(
        "schema_doohickey",
        DoohickeyConfig,
        id_source="by_key",
        id_default="doohickey",
    )

    section = config_json_schema()["properties"]["schema_doohickey"]

    # The section names the entity itself when the file does not, so the key is optional.
    assert section["properties"]["id"]["type"] == "string"
    assert "required" not in section


def test_schema_entity_named_at_declaration():
    sk.declare_config_section(
        "schema_thingamajig",
        ThingamajigConfig,
        id_source="default",
        id_default="thingamajig",
    )

    section = config_json_schema()["properties"]["schema_thingamajig"]

    # The name never appears in the file, so the section accepts no key for it.
    assert section == {"$ref": "#/$defs/ThingamajigConfig"}


def test_schema_id_key_declared_by_the_model():
    sk.declare_config_section(
        "schema_gizmos",
        list[GizmoConfig],
        id_source="by_subkey",
        id_key="target",
    )

    section = config_json_schema()["properties"]["schema_gizmos"]

    assert section == {"type": "array", "items": {"$ref": "#/$defs/GizmoConfig"}}


def test_schema_id_key_from_mapping_keys():
    sk.declare_config_section(
        "schema_doohickeys",
        dict[str, DoohickeyConfig],
        id_source="mapping_key",
    )

    section = config_json_schema()["properties"]["schema_doohickeys"]

    # The names are the mapping's own keys, so the entries take no key of their own.
    assert section["additionalProperties"] == {"$ref": "#/$defs/DoohickeyConfig"}


def test_schema_describes_the_declared_source_not_the_mapper():
    sk.declare_config_section(
        "schema_sprockets",
        dict[str, SprocketConfig],
        id_source="mapping_key",
        id_mapper=list,
    )

    # A hand-written mapper changes which IDs the section produces, not where the file
    # carries them, so the schema still follows the declared source.
    section = config_json_schema()["properties"]["schema_sprockets"]

    assert section["additionalProperties"] == {"$ref": "#/$defs/SprocketConfig"}


def test_schema_separates_models_sharing_a_name():
    for key, field in (("schema_first", "left"), ("schema_second", "right")):
        sk.declare_config_section(
            key,
            pydantic.create_model("SharedName", **{field: (int, ...)}),
            id_source="by_key",
        )

    schema = config_json_schema()

    assert "left" in schema["properties"]["schema_first"]["properties"]
    assert "right" in schema["properties"]["schema_second"]["properties"]
