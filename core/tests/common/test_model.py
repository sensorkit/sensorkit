# SPDX-License-Identifier: Apache-2.0
import random
from dataclasses import dataclass, field
from typing import Annotated, Any, Literal, Union

import pytest
from pydantic import BaseModel, Discriminator, Field, TypeAdapter, ValidationError

from sensorkit.common.model import (
    ModelRegistry,
    ModelRegistryView,
    RegistryDiscriminator,
    RegistryError,
)


@pytest.mark.asyncio
async def test_registry():
    class MyModel(BaseModel):
        type: Literal["my_model"] = "my_model"
    class MyModel2(MyModel):
        type: Literal["my_model2"] = "my_model2"

    registry = ModelRegistry(MyModel, MyModel2, discriminator="type")

    class Container(BaseModel):
        my_model: Annotated[MyModel, registry.discriminator()]

    obj = Container.model_validate({"my_model": {"type": "my_model2"}})
    assert isinstance(obj.my_model, MyModel2)

    with pytest.raises(ValidationError):
        Container.model_validate({"my_model": {}})


@pytest.mark.asyncio
async def test_registry_default_discriminator():
    class MyModel(BaseModel):
        type: Literal["my_model"] = "my_model"
    class MyModel2(MyModel):
        type: Literal["my_model2"] = "my_model2"

    registry = ModelRegistry(
        MyModel,
        MyModel2,
        discriminator="type",
        default_tag="my_model2",
    )

    class Container(BaseModel):
        my_model: Annotated[MyModel, registry.discriminator()]

    obj = Container.model_validate({"my_model": {}})
    assert isinstance(obj.my_model, MyModel2)


@pytest.mark.asyncio
async def test_registry_with_context():
    class MyModel(BaseModel):
        type: Literal["my_model"] = Field(default="my_model", exclude=True)
    class MyModel2(MyModel):
        type: Literal["my_model2"] = Field(default="my_model2", exclude=True)
        val: int

    registry = ModelRegistry(MyModel, MyModel2, discriminator="type")

    class Container(BaseModel):
        my_model: Annotated[MyModel, registry.discriminator()]

    obj0 = Container(my_model=MyModel2(val=42))
    obj1 = Container.model_validate({"my_model": {"val": 42}}, context={"discriminator": "my_model2"})
    assert isinstance(obj1.my_model, MyModel2)
    assert obj0 == obj1

    # Discriminator present in both data and context with the same value is allowed.
    Container.model_validate(
        {"my_model": {"type": "my_model"}}, context={"discriminator": "my_model"}
    )

    with pytest.raises(ValidationError):
        Container.model_validate(
            {"my_model": {"type": "my_model"}}, context={"discriminator": "my_model2"}
        )


@pytest.mark.asyncio
async def test_registry_with_external_discriminator():
    class MyModel(BaseModel): ...
    class MyModel2(BaseModel):
        val: int

    registry = ModelRegistry()
    registry.add(MyModel, tag="my_model")
    registry.add(MyModel2, tag="my_model2")

    adapter = TypeAdapter(Annotated[MyModel, registry.discriminator()])
    obj0 = adapter.validate_python({}, context={"discriminator": "my_model"})
    obj1 = adapter.validate_python({"val": 42}, context={"discriminator": "my_model2"})
    assert isinstance(obj0, MyModel)
    assert isinstance(obj1, MyModel2)


@pytest.mark.parametrize("mode", ["control", "str", "context"])
def test_registry_performance(  # noqa: C901
    mode: Literal["control", "str", "context"],
    num_models: int = 1000,
    num_trials: int = 10000,
):
    match mode:
        case "str":
            registry = ModelRegistry(discriminator="model_id")
            disc = registry.discriminator()
        case "context":
            registry = ModelRegistry()
            disc = registry.discriminator()
        case _:
            registry = None
            disc = None

    class Base(BaseModel):
        model_id: Literal[None] | Any = None

    models = []
    index = {}

    for i in range(num_models):
        new_id = f"model{i}"
        cls = type(
            new_id,
            (Base,),
            {
                "model_id": new_id,
                "__annotations__": {"model_id": Literal[new_id]},  # noqa
            }
            if mode != "context"
            else {},
        )
        models.append(cls)
        index[new_id] = cls

        if registry is not None:
            registry.add(cls, tag=new_id)

    match mode:
        case "control":
            union_type = Union[tuple(models)]
            adapter = TypeAdapter(Annotated[union_type, Discriminator("model_id")])

            def validate_func(model_id):
                return adapter.validate_python({"model_id": model_id, "x": 42})
        case "str":
            adapter = TypeAdapter(Annotated[Base, disc])

            def validate_func(model_id):
                return adapter.validate_python({"model_id": model_id, "x": 42})
        case "context":
            adapter = TypeAdapter(Annotated[Base, disc])

            def validate_func(model_id):
                return adapter.validate_python(
                    {"x": 42}, context={ModelRegistryView.DISCRIMINATOR_CONTEXT: model_id}
                )
        case _:
            raise RuntimeError

    for _ in range(num_trials):
        i = random.randint(0, num_models - 1)
        model_id = f"model{i}"
        cls = index[model_id]
        assert isinstance(validate_func(model_id), cls)


def test_namespace_aware_add():
    """_entries tracks namespace metadata on each add() call."""

    class ModelA(BaseModel): ...
    class ModelB(BaseModel): ...

    registry = ModelRegistry()
    registry.add(ModelA, tag="tag_a", namespace="ns1")
    registry.add(ModelB, tag="tag_b")  # no namespace → DEFAULT_NAMESPACE

    assert len(registry._entries) == 2

    entry_a = registry._entries[ModelA]
    assert entry_a.model_type is ModelA
    assert entry_a.tag == "tag_a"
    assert entry_a.namespace == "ns1"

    entry_b = registry._entries[ModelB]
    assert entry_b.model_type is ModelB
    assert entry_b.tag == "tag_b"
    assert entry_b.namespace is None


def test_view_no_conflict_passthrough():
    """Non-overlapping tags across namespaces all pass through without precedence."""

    class ModelA(BaseModel):
        val: int = 0

    class ModelB(BaseModel):
        val: str = ""

    registry = ModelRegistry()
    registry.add(ModelA, tag="a", namespace="ns1")
    registry.add(ModelB, tag="b", namespace="ns2")  # different tag — no conflict

    view = ModelRegistryView(registry, precedence=[])  # no precedence needed

    assert ModelA in view
    assert ModelB in view


def test_view_same_namespace_duplicate_error():
    """Same tag registered twice in the same namespace raises RegistryError."""

    class ModelA(BaseModel): ...
    class ModelB(BaseModel): ...

    registry = ModelRegistry()
    registry.add(ModelA, tag="x", namespace="ns1")

    with pytest.raises(RegistryError, match="ns1"):
        registry.add(ModelB, tag="x", namespace="ns1")  # same namespace, same tag


def test_view_resolve_with_precedence():
    """Higher-precedence namespace wins a cross-namespace tag conflict."""

    class ModelA(BaseModel):
        val: int = 1

    class ModelB(BaseModel):
        val: int = 2

    registry = ModelRegistry()
    registry.add(ModelA, tag="x", namespace="ns1")
    registry.add(ModelB, tag="x", namespace="ns2")

    # ns1 has higher precedence (lower index)
    view = ModelRegistryView(registry, precedence=["ns1", "ns2"])

    assert ModelA in view
    assert ModelB not in view

    # ns2 has higher precedence
    view2 = ModelRegistryView(registry, precedence=["ns2", "ns1"])
    assert ModelB in view2
    assert ModelA not in view2


def test_view_validation_via_context():
    """DynamicDiscriminator routes to ModelRegistryView supplied in pydantic context."""

    class ModelA(BaseModel):
        val: int = 0

    class ModelB(BaseModel):
        val: str = ""

    registry = ModelRegistry()
    registry.add(ModelA, tag="a")
    registry.add(ModelB, tag="b")

    view = ModelRegistryView(registry, precedence=[])
    adapter = TypeAdapter(Annotated[object, RegistryDiscriminator(registry)])

    result_a = adapter.validate_python(
        {"val": 42},
        context={
            ModelRegistryView.DISCRIMINATOR_CONTEXT: "a",
            ModelRegistryView.REGISTRY_CONTEXT: view,
        },
    )
    assert isinstance(result_a, ModelA)
    assert result_a.val == 42

    result_b = adapter.validate_python(
        {"val": "hello"},
        context={
            ModelRegistryView.DISCRIMINATOR_CONTEXT: "b",
            ModelRegistryView.REGISTRY_CONTEXT: view,
        },
    )
    assert isinstance(result_b, ModelB)
    assert result_b.val == "hello"


def test_view_validation_via_contextvar():
    """DynamicDiscriminator routes to ModelRegistryView installed via as_current()."""

    class ModelA(BaseModel):
        val: int = 0

    class ModelB(BaseModel):
        val: str = ""

    registry = ModelRegistry()
    registry.add(ModelA, tag="a")
    registry.add(ModelB, tag="b")

    view = ModelRegistryView(registry, precedence=[])
    adapter = TypeAdapter(Annotated[object, RegistryDiscriminator(registry)])

    with view.as_current():
        assert isinstance(
            adapter.validate_python(
                {"val": 42}, context={ModelRegistryView.DISCRIMINATOR_CONTEXT: "a"}
            ),
            ModelA,
        )
        assert isinstance(
            adapter.validate_python(
                {"val": "hello"}, context={ModelRegistryView.DISCRIMINATOR_CONTEXT: "b"}
            ),
            ModelB,
        )

    # ContextVar is reset after the context manager exits.
    from sensorkit.common.model import _registry_var
    assert _registry_var.get() is None


def test_view_fallback_no_view():
    """When no registry is active, DynamicDiscriminator falls back to catalog behavior."""

    class ModelA(BaseModel):
        val: int = 0

    class ModelB(BaseModel):
        val: str = ""

    registry = ModelRegistry()
    registry.add(ModelA, tag="a")
    registry.add(ModelB, tag="b")

    adapter = TypeAdapter(Annotated[object, RegistryDiscriminator(registry)])

    # No model_registry in context, no ContextVar set → fallback path.
    assert isinstance(
        adapter.validate_python(
            {"val": 42}, context={ModelRegistryView.DISCRIMINATOR_CONTEXT: "a"}
        ),
        ModelA,
    )
    assert isinstance(
        adapter.validate_python(
            {"val": "hello"}, context={ModelRegistryView.DISCRIMINATOR_CONTEXT: "b"}
        ),
        ModelB,
    )


def test_view_field_tag_extraction():
    """Field-discriminated registrations are resolved via field default."""

    class EventA(BaseModel):
        event_type: Literal["event_a"] = "event_a"

    class EventB(BaseModel):
        event_type: Literal["event_b"] = Field(default="event_b")

    @dataclass
    class EventC:
        event_type: Literal["event_c"] = field(default="event_c")

    registry = ModelRegistry(discriminator="event_type")
    registry.add(EventA)
    registry.add(EventB)
    registry.add(EventC)

    view = ModelRegistryView(registry)

    assert EventA in view
    assert EventB in view
    assert EventC in view

    adapter = TypeAdapter(Annotated[object, registry.discriminator()])

    assert isinstance(
        adapter.validate_python(
            {"event_type": "event_a"}, context={ModelRegistryView.REGISTRY_CONTEXT: view}
        ),
        EventA,
    )
    assert isinstance(
        adapter.validate_python(
            {"event_type": "event_b"}, context={ModelRegistryView.REGISTRY_CONTEXT: view}
        ),
        EventB,
    )
    assert isinstance(
        adapter.validate_python(
            {"event_type": "event_c"}, context={ModelRegistryView.REGISTRY_CONTEXT: view}
        ),
        EventC,
    )


def test_view_field_tag_conflict_resolved_by_precedence():
    """Cross-namespace conflict on field tags is resolved by precedence."""

    class EventA(BaseModel):
        event_type: Literal["shared"] = "shared"

    class EventB(BaseModel):
        event_type: Literal["shared"] = "shared"

    registry = ModelRegistry(discriminator="event_type")
    registry.add(EventA, namespace="core")
    registry.add(EventB, namespace="plugin")
    adapter = TypeAdapter(Annotated[object, registry.discriminator()])

    assert EventA in registry
    assert EventB in registry

    with pytest.raises(ValidationError):
        # Both EventA and EventB have the same tag, so this will fail.
        adapter.validate_python({"event_type": "shared"})

    view = ModelRegistryView(registry, precedence=["core", "plugin"])

    # Namespace resolution discards EventB to remove the conflict.
    assert EventA in view
    assert EventB not in view
    assert isinstance(
        adapter.validate_python({"event_type": "shared"}, context={view.REGISTRY_CONTEXT: view}),
        EventA,
    )
