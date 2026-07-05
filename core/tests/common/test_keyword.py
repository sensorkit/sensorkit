# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from pydantic import BaseModel, TypeAdapter

from sensorkit.common.keyword import (
    KeywordDict,
    declare_keyword,
    validate_keyword,
    validate_keyword_json,
)


def test_keyword_validation():
    @declare_keyword
    class Foo(BaseModel):
        x: int

    parsed_foo = validate_keyword("Foo", {"x": 11})
    assert isinstance(parsed_foo, Foo) and parsed_foo.x == 11

    @declare_keyword
    @dataclass
    class Bar:
        y: int

    bar = Bar(y=44)
    parsed_bar = validate_keyword_json("Bar", TypeAdapter(Bar).dump_json(bar))
    assert isinstance(parsed_bar, Bar) and parsed_bar == bar


def test_keyword_external():
    @dataclass
    class ExternalBaz:
        z: int

    declare_keyword(ExternalBaz, key="FooBaz")

    baz = ExternalBaz(z=44)
    parsed_baz = validate_keyword_json("FooBaz", TypeAdapter(ExternalBaz).dump_json(baz))
    assert isinstance(parsed_baz, ExternalBaz) and parsed_baz == baz


def test_keyword_dict():
    @declare_keyword
    class Baz(BaseModel):
        x: int

    d = KeywordDict()
    baz = Baz(x=42)

    d.set(baz)
    assert d.get(Baz) is baz
    assert d.get("Baz") is baz
    assert d[Baz] is baz
    assert d["Baz"] is baz

    d["myfoo"] = "bar"
    assert d.get("myfoo") == "bar"
    assert d["myfoo"] == "bar"


def test_keyword_dict_serde():
    @declare_keyword
    class Bot(BaseModel):
        x: int

    d = KeywordDict()
    d["x"] = 42
    d.set(Bot(x=42))

    kd_adapter = TypeAdapter(KeywordDict)
    print(f"{d=}")
    json = kd_adapter.dump_json(d)
    print(f"{json=}")
    parsed = kd_adapter.validate_json(json)
    print(f"{parsed=}")
    assert isinstance(parsed, KeywordDict)
    # assert parsed == d
    print(f"{parsed["Bot"]=}")
    print(f"{type(parsed["Bot"])=}")
    print(f"{id(type(parsed["Bot"]))=}")
    print(f"{id(Bot)=}")
    assert isinstance(parsed["Bot"], Bot)
