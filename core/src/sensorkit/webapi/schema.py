import itertools
from typing import Any

from pydantic import BaseModel, TypeAdapter

from sensorkit import api as sk
from sensorkit.common.keyword import _keyword_index


def _sensorkit_models():
    # Return all models registered in the sensorkit package.
    # FIXME: This is top-level and should probably live in `sensorkit.api`
    return itertools.chain(
        _keyword_index,
        sk.DeviceCommand.registry.entries,
        sk.Task.registry.entries,
    )


def _update_refs(obj):
    """Recursively update $ref from #/$defs/Name to #/components/schemas/Name."""
    if isinstance(obj, dict):
        if "$ref" in obj and isinstance(obj["$ref"], str):
            if obj["$ref"].startswith("#/$defs/"):
                obj["$ref"] = obj["$ref"].replace("#/$defs/", "#/components/schemas/")
        for v in obj.values():
            _update_refs(v)
    elif isinstance(obj, list):
        for item in obj:
            _update_refs(item)


def _add_model_schema(model: type[BaseModel], schemas: dict[str, Any]):
    adapter = TypeAdapter(model)
    schema = adapter.json_schema()

    # Extract internal definitions and move them to global schemas.
    if "$defs" in schema:
        defs = schema.pop("$defs")

        for name, def_schema in defs.items():
            if name not in schemas:
                # Recursively update refs in the definition itself.
                _update_refs(def_schema)
                schemas[name] = def_schema

    # Update refs in the main schema.
    _update_refs(schema)

    # Add the class schema to components/schemas.
    schema_name = model.__name__

    if schema_name not in schemas:
        schemas[schema_name] = schema


def add_sensorkit_schema(schemas: dict[str, Any]):
    # Generate JSON schema for registered models.
    for cls in _sensorkit_models():
        if issubclass(cls, BaseModel):
            _add_model_schema(cls, schemas)
