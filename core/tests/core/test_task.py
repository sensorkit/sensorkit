from __future__ import annotations

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.task import TaskContexts


def test_contexts_get():
    contexts = TaskContexts(
        all=KeywordDict({"a": 1, "b": 2}),
        init=KeywordDict({"b": 3, "c": 4})
    )

    init_context = contexts.get("init")
    assert init_context["a"] == 1  # From "all"
    assert init_context["b"] == 3  # From "init", overriding "all"
    assert init_context["c"] == 4  # From "init"

    standby_context = contexts.get("standby")
    assert standby_context["a"] == 1
    assert standby_context["b"] == 2
    assert "c" not in standby_context

def test_contexts_propagate():
    parent = TaskContexts(all=KeywordDict({"a": 1, "b": 2}))
    child = TaskContexts(all=KeywordDict({"b": 3, "c": 4}))

    parent.propagate(child)

    assert child.all["a"] == 1  # Propagated from parent
    assert child.all["b"] == 3  # Kept from child (child has precedence)
    assert child.all["c"] == 4  # Kept from child

    parent = TaskContexts(init=KeywordDict({"a": 1, "b": 2}))
    child = TaskContexts(init=KeywordDict({"b": 3, "c": 4}))

    parent.propagate(child)

    assert child.init["a"] == 1  # Propagated from parent
    assert child.init["b"] == 3  # Kept from child
    assert child.init["c"] == 4  # Kept from child

    # If a keyword is in child.all, it should not be copied from parent.specific to child.specific
    parent = TaskContexts(init=KeywordDict({"a": 1}))
    child = TaskContexts(all=KeywordDict({"a": 2}))

    parent.propagate(child)

    assert "a" not in child.init
    # The effective context for init should have a=2 from child.all
    assert child.get("init")["a"] == 2

def test_contexts_propagate_complex():
    parent = TaskContexts(
        all=KeywordDict({"common": "parent", "only_parent": 1}),
        init=KeywordDict({"init_val": "parent", "conflict": "parent"})
    )
    child = TaskContexts(
        all=KeywordDict({"common": "child", "only_child": 2}),
        init=KeywordDict({"init_val": "child"})
    )

    parent.propagate(child)

    # Check "all" propagation
    assert child.all["common"] == "child"
    assert child.all["only_parent"] == 1
    assert child.all["only_child"] == 2

    # Check "init" propagation
    assert child.init["init_val"] == "child"
    assert child.init["conflict"] == "parent"

    # Check effective context
    eff = child.get("init")
    assert eff["common"] == "child"
    assert eff["only_parent"] == 1
    assert eff["only_child"] == 2
    assert eff["init_val"] == "child"
    assert eff["conflict"] == "parent"
