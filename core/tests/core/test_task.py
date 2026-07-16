# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword
from sensorkit.core.task import TaskContextOverlay


@declare_keyword
class _MergeCameraConfig(BaseModel):
    exposure_time: float = 1.0
    binning: int = 1


def test_contexts_get():
    contexts = TaskContextOverlay(
        all={"a": 1, "b": 2},
        init={"b": 3, "c": 4},
    ).build()

    init_context = contexts.get("init")
    assert init_context["a"] == 1  # From "all"
    assert init_context["b"] == 3  # From "init", overriding "all"
    assert init_context["c"] == 4  # From "init"

    standby_context = contexts.get("standby")
    assert standby_context["a"] == 1
    assert standby_context["b"] == 2
    assert "c" not in standby_context


def test_contexts_get_unmentioned_type_falls_back_to_defaults():
    # A task type never named in config still receives the flattened `all` context.
    contexts = TaskContextOverlay(all={"a": 1, "b": 2}).build()

    context = contexts.get("collect")
    assert context["a"] == 1
    assert context["b"] == 2


def test_contexts_propagate():
    parent = TaskContextOverlay(all={"a": 1, "b": 2})
    child = TaskContextOverlay(all={"b": 3, "c": 4})

    parent.propagate(child)

    assert child.all["a"] == 1  # Propagated from parent
    assert child.all["b"] == 3  # Kept from child (child has precedence)
    assert child.all["c"] == 4  # Kept from child

    parent = TaskContextOverlay(init={"a": 1, "b": 2})
    child = TaskContextOverlay(init={"b": 3, "c": 4})

    parent.propagate(child)

    assert child.init["a"] == 1  # Propagated from parent
    assert child.init["b"] == 3  # Kept from child
    assert child.init["c"] == 4  # Kept from child

    # If a keyword is in child.all, it should not be copied from parent.specific to child.specific
    parent = TaskContextOverlay(init={"a": 1})
    child = TaskContextOverlay(all={"a": 2})

    parent.propagate(child)

    assert "a" not in child.init
    # The effective context for init should have a=2 from child.all
    assert child.build().get("init")["a"] == 2


def test_contexts_propagate_complex():
    parent = TaskContextOverlay(
        all={"common": "parent", "only_parent": 1},
        init={"init_val": "parent", "conflict": "parent"},
    )
    child = TaskContextOverlay(
        all={"common": "child", "only_child": 2},
        init={"init_val": "child"},
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
    eff = child.build().get("init")
    assert eff["common"] == "child"
    assert eff["only_parent"] == 1
    assert eff["only_child"] == 2
    assert eff["init_val"] == "child"
    assert eff["conflict"] == "parent"


def test_build_merges_keyword_payloads_field_wise():
    # A keyword split across `all` and a task type combines field-wise rather than clobbering,
    # and the merged payload validates into a real keyword instance.
    contexts = TaskContextOverlay(
        all={"_MergeCameraConfig": {"exposure_time": 5.0, "binning": 1}},
        init={"_MergeCameraConfig": {"binning": 2}},
    ).build()

    camera = contexts.get("init")[_MergeCameraConfig]
    assert isinstance(camera, _MergeCameraConfig)
    assert camera.exposure_time == 5.0  # preserved from `all`
    assert camera.binning == 2  # overridden by `init`
