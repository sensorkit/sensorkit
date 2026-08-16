# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
from collections.abc import Iterator
from typing import Any

from pydantic.json_schema import GenerateJsonSchema

from sensorkit.config.parser import PARSER_VERSION, SensorKitBaseConfig
from sensorkit.config.section import ConfigSection, config_sections

SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_TITLE = "SensorKit configuration"
ID_KEY_DESCRIPTION = "Name of the entity this entry defines."

# Keywords that leave an object accepting properties beyond the ones it lists, either by
# saying so outright or by composing with a schema that lists more.
OPEN_KEYWORDS = frozenset(
    {"additionalProperties", "allOf", "anyOf", "oneOf", "patternProperties", "$ref"}
)

type JsonSchema = dict[str, Any]

# Key standing in for the base config in a definitions pass, where every other key is a
# section name.
BASE = None


def _stitch_id_key(
    section: ConfigSection,
    schema: JsonSchema,
    defs: dict[str, JsonSchema],
) -> JsonSchema:
    """Extend a section's schema with the entity ID key it accepts.

    The key is merged into a copy of the model rather than composed onto a reference to it.
    Composition would put the two property lists in separate subschemas, where neither one
    can be closed against undeclared keys, and would leave the pair beyond the reach of the
    JSON Schema support in common YAML editors.

    Args:
        section: The registered section the schema was generated for.
        schema: The section's generated schema, either a reference to a model or a container
            of them.
        defs: The definitions the schema references.

    Returns:
        The section schema, extended where an ID key applies.
    """
    match section.id_source:
        case "by_key":
            slot = None
        case "by_subkey":
            slot = "items" if schema.get("type") == "array" else "additionalProperties"
        case _:
            # A section taking its IDs from mapping keys, or from an ID fixed at declaration,
            # accepts no key of its own.
            return schema

    item = schema if slot is None else schema.get(slot)
    ref = item.get("$ref") if isinstance(item, dict) else None

    if ref is None:
        return schema

    model = defs[ref.rpartition("/")[2]]
    declared = model.get("properties", {})

    # Sections taking their IDs from a field of their own model need no stitching.
    if section.id_key in declared:
        return schema

    extended: JsonSchema = {
        **model,
        "properties": {
            section.id_key: {"type": "string", "description": ID_KEY_DESCRIPTION},
            **declared,
        },
    }

    if section.id_key_required:
        extended["required"] = [section.id_key, *model.get("required", ())]

    return extended if slot is None else {**schema, slot: extended}


def _conditional_branches(prop: str, mapping: dict[str, str]) -> JsonSchema:
    """Build a union that selects its branch by a discriminating property.

    Args:
        prop: Name of the property naming the branch to apply.
        mapping: Reference to the branch each value of the property selects.

    Returns:
        An object schema applying exactly the branch the property names.
    """
    return {
        "type": "object",
        "properties": {prop: {"enum": list(mapping)}},
        "required": [prop],
        "allOf": [
            {
                "if": {"properties": {prop: {"const": value}}, "required": [prop]},
                "then": {"$ref": ref},
            }
            for value, ref in mapping.items()
        ],
    }


def _expand_discriminators(node: Any):
    """Rewrite discriminated unions to select their branch conditionally, in place.

    Pydantic marks a discriminated union with a `discriminator` annotation, an OpenAPI
    extension that JSON Schema validators do not read. Left with the bare list of branches, a
    validator tries all of them, and an entry matching none reports a single failure against
    the whole object, which editors show against every key in it. Choosing the branch by its
    discriminating property narrows the report to the keys responsible for it.

    Args:
        node: The schema document, or any node within it.
    """
    match node:
        # The branches stay reachable as references, and the definitions they name are walked
        # in their own right, so the rewritten node needs no further visiting.
        case {"oneOf": _, "discriminator": {"propertyName": str(prop), "mapping": dict(mapping)}}:
            node.clear()
            node.update(_conditional_branches(prop, mapping))
        case dict():
            for value in node.values():
                _expand_discriminators(value)
        case list():
            for item in node:
                _expand_discriminators(item)


def _closable(node: JsonSchema) -> bool:
    """Report whether a node names every property it accepts.

    Args:
        node: The schema node to judge.

    Returns:
        True if the node's property list is exhaustive.
    """
    return node.get("type") == "object" and "properties" in node and not OPEN_KEYWORDS & node.keys()


def _close_objects(node: Any):
    """Forbid undeclared properties throughout a schema document, in place.

    Pydantic describes a model as accepting more than its own fields only where the model
    says so, so a model taking the default policy generates a property list an editor reads
    as open. Closing the lists that are exhaustive turns a misspelled key into a reported
    error rather than one that validation quietly drops.

    Args:
        node: The schema document, or any node within it.
    """
    match node:
        case dict():
            if _closable(node):
                node["additionalProperties"] = False

            for value in node.values():
                _close_objects(value)
        case list():
            for item in node:
                _close_objects(item)


def _referenced_defs(node: Any) -> Iterator[str]:
    """Yield the name of every definition referenced from a schema node.

    Args:
        node: The schema node to walk.

    Yields:
        Definition names, with repeats.
    """
    match node:
        case dict():
            if isinstance(ref := node.get("$ref"), str):
                yield ref.rpartition("/")[2]

            for value in node.values():
                yield from _referenced_defs(value)
        case list():
            for item in node:
                yield from _referenced_defs(item)


def _reachable_defs(root: Any, defs: dict[str, JsonSchema]) -> dict[str, JsonSchema]:
    """Select the definitions a document actually reaches.

    Args:
        root: The document to walk, excluding the definitions themselves.
        defs: The generated definitions to select from.

    Returns:
        The definitions reachable from the document, in their generated order.
    """
    reachable: set[str] = set()
    pending = list(_referenced_defs(root))

    while pending:
        name = pending.pop()

        if name in reachable or name not in defs:
            continue

        reachable.add(name)
        pending.extend(_referenced_defs(defs[name]))

    return {name: schema for name, schema in defs.items() if name in reachable}


def config_json_schema(*, title: str = SCHEMA_TITLE) -> JsonSchema:
    """Generate a JSON Schema for a complete unified configuration file.

    The schema covers the fixed top-level keys along with every config section registered so
    far. Sections come from modules, so import the modules a site uses before calling this
    (`import_modules` does so from the environment and the config file itself), or the
    result describes only the core sections.

    Args:
        title: Title recorded in the generated schema.

    Returns:
        A JSON Schema document, ready to serialize.
    """
    # Make sure core sections are imported.
    importlib.import_module("sensorkit.config.core")

    sections = sorted(config_sections().items())
    generator = GenerateJsonSchema()

    # Generating everything in one pass keeps the definitions unified and lets Pydantic
    # disambiguate models that share a name across modules.
    refs, defs = generator.generate_definitions(
        [
            (BASE, "validation", SensorKitBaseConfig.__pydantic_core_schema__),
            *((key, "validation", section.adapter.core_schema) for key, section in sections),
        ]
    )

    # The base config becomes the root of the document, so its definition is consumed rather
    # than referenced.
    base = defs.pop(refs[(BASE, "validation")]["$ref"].rpartition("/")[2])
    properties = dict(base.get("properties", {}))

    # The parser accepts only its own version, which the field default does not convey.
    properties["version"] = {**properties["version"], "const": PARSER_VERSION}
    properties["version"].pop("default", None)

    properties.update(
        {
            key: _stitch_id_key(section, refs[(key, "validation")], defs)
            for key, section in sections
        }
    )

    document = {
        "$schema": SCHEMA_DIALECT,
        "title": title,
        "type": "object",
        "properties": properties,
        "required": ["version"],
        # An unregistered top-level key is a config error, not free-form data.
        "additionalProperties": False,
        # Stitching an ID key consumes the section's own definition.
        "$defs": _reachable_defs(properties, defs),
    }

    _expand_discriminators(document)
    _close_objects(document)

    return document
