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


def test_keyword_dict_set_composite():
    @declare_keyword
    class Part(BaseModel):
        x: int

    @declare_keyword
    class Whole(BaseModel):
        part: Part

        def composed_keywords(self):
            yield self.part

    d = KeywordDict()
    whole = Whole(part=Part(x=42))

    # Setting the composite also sets what it composes, under that keyword's own key.
    d.set(whole)
    assert d[Whole] is whole
    assert d[Part] is whole.part

    # Writes follow argument order, so a part given after the composite replaces the composed
    # one, and a part given before it does not.
    other = Part(x=7)
    d.set(whole, other)
    assert d[Part] is other

    d.set(other, whole)
    assert d[Part] is whole.part


def test_keyword_dict_set_composite_nested():
    @declare_keyword
    class Leaf(BaseModel):
        x: int

    @declare_keyword
    class Branch(BaseModel):
        leaf: Leaf

        def composed_keywords(self):
            yield self.leaf

    @declare_keyword
    class Trunk(BaseModel):
        branch: Branch

        def composed_keywords(self):
            yield self.branch

    d = KeywordDict()
    trunk = Trunk(branch=Branch(leaf=Leaf(x=42)))

    # Expansion is transitive: the leaf is reachable only through the branch, but setting the
    # trunk must still make it available under its own key.
    d.set(trunk)
    assert d[Trunk] is trunk
    assert d[Branch] is trunk.branch
    assert d[Leaf] is trunk.branch.leaf


def test_keyword_dict_set_composite_cycle():
    @declare_keyword
    class Ouroboros(BaseModel):
        x: int
        other: "Ouroboros | None" = None

        def composed_keywords(self):
            if self.other is not None:
                yield self.other

    d = KeywordDict()
    snake = Ouroboros(x=42)
    snake.other = snake

    # A keyword that composes itself terminates rather than recurring forever.
    d.set(snake)
    assert d[Ouroboros] is snake


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
