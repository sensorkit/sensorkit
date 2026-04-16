from __future__ import annotations

from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword
from sensorkit.webapi.schema import _add_model_schema, _update_refs, add_sensorkit_schema


def test_update_refs_replaces_defs():
    obj = {"$ref": "#/$defs/MyModel"}
    _update_refs(obj)
    assert obj["$ref"] == "#/components/schemas/MyModel"


def test_update_refs_nested():
    obj = {
        "properties": {
            "field": {"$ref": "#/$defs/Inner"},
            "other": {"anyOf": [{"$ref": "#/$defs/Another"}]},
        }
    }
    _update_refs(obj)
    assert obj["properties"]["field"]["$ref"] == "#/components/schemas/Inner"
    assert obj["properties"]["other"]["anyOf"][0]["$ref"] == "#/components/schemas/Another"


def test_update_refs_ignores_non_defs():
    obj = {"$ref": "#/components/schemas/Existing"}
    _update_refs(obj)
    assert obj["$ref"] == "#/components/schemas/Existing"


def test_add_model_schema_simple():
    class SimpleModel(BaseModel):
        value: int

    schemas: dict = {}
    _add_model_schema(SimpleModel, schemas)

    assert "SimpleModel" in schemas
    assert schemas["SimpleModel"]["properties"]["value"]["type"] == "integer"


def test_add_model_schema_extracts_defs():
    class Inner(BaseModel):
        x: int

    class Outer(BaseModel):
        inner: Inner

    schemas: dict = {}
    _add_model_schema(Outer, schemas)

    assert "Outer" in schemas
    assert "Inner" in schemas
    # $ref in Outer should point to components/schemas, not $defs
    assert "$defs" not in schemas["Outer"]


def test_add_model_schema_no_duplicate():
    class MyModel(BaseModel):
        value: int

    schemas: dict = {}
    _add_model_schema(MyModel, schemas)
    original = schemas["MyModel"]
    _add_model_schema(MyModel, schemas)
    assert schemas["MyModel"] is original


def test_add_sensorkit_schema_includes_keywords():
    @declare_keyword
    class SchemaTestKeyword(BaseModel):
        reading: float

    schemas: dict = {}
    add_sensorkit_schema(schemas)

    assert "SchemaTestKeyword" in schemas
