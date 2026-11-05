# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from typing import Literal

import pytest
import uuid_utils.compat as uuid
from pydantic import BaseModel

from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.core.task import CollectTask, TaskContextOverlay, TaskExecution


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


class ProbeCollectTask(CollectTask):
    task_type: Literal["probe_collect"] = "probe_collect"
    exposures: int


@pytest.mark.parametrize(
    "source_model",
    [TaskExecution, TaskExecution[CollectTask]],
    ids=("unparameterized_source", "parent_specialized_source"),
)
@pytest.mark.parametrize(
    "model",
    [TaskExecution, TaskExecution[ProbeCollectTask]],
    ids=("unparameterized", "typed"),
)
def test_execution_round_trip_keeps_task_subtype(model, source_model):
    execution = source_model(
        task=ProbeCollectTask(exposures=3),
        task_id=uuid.uuid7(),
        controller_id="mycontroller",
        context=KeywordDict({"probe": "value"}),
    )

    wire = execution.model_dump_json()
    restored = model.model_validate_json(wire)

    assert json.loads(wire)["task"] == {"task_type": "probe_collect", "exposures": 3}
    assert type(restored.task) is ProbeCollectTask
    assert restored.task == execution.task
    assert restored.task_id == execution.task_id
    assert restored.context["probe"] == "value"
    assert restored.model_dump_json() == wire


def test_task_reused_across_executions():
    task = ProbeCollectTask(exposures=3)
    first, second = (
        TaskExecution(task=task, task_id=uuid.uuid7(), controller_id="mycontroller")
        for _ in range(2)
    )

    assert first.task is second.task
    assert first.task == ProbeCollectTask(exposures=3)
    assert first.task != ProbeCollectTask(exposures=4)


@pytest.mark.parametrize(
    "submitted_context",
    [None, KeywordDict(), KeywordDict({"positions": [[10], [20]]})],
    ids=("snapshot", "empty", "nested"),
)
@pytest.mark.parametrize("model", [TaskExecution, TaskExecution[ProbeCollectTask]])
def test_execution_submitted_context_round_trips(submitted_context, model):
    execution = model(
        task=ProbeCollectTask(exposures=3),
        task_id=uuid.uuid7(),
        controller_id="mycontroller",
        context=KeywordDict({"frame": 1}),
        submitted_context=submitted_context,
    )

    restored = model.model_validate_json(execution.model_dump_json())

    expected = execution.context if submitted_context is None else submitted_context
    assert execution.submitted_context == expected
    assert restored.submitted_context == expected
    assert restored.context == {"frame": 1}


@pytest.mark.parametrize(
    "context_fields",
    [{}, {"context": None}, {"context": {}}],
    ids=("omitted", "null", "empty"),
)
def test_execution_context_defaults(context_fields):
    fields = {
        "task": {"task_type": "collect"},
        "task_id": str(uuid.uuid7()),
        "controller_id": "mycontroller",
        **context_fields,
    }

    executions = (
        TaskExecution(**fields),
        TaskExecution.model_validate_json(json.dumps(fields)),
    )

    for execution in executions:
        assert execution.context == KeywordDict()
        assert execution.submitted_context == KeywordDict()
        assert execution.submitted_context is not execution.context


@pytest.mark.parametrize(
    "submitted_fields",
    [{}, {"submitted_context": None}],
    ids=("omitted", "null"),
)
def test_missing_submitted_context_snapshots_working_context(submitted_fields):
    fields = {
        "task": {"task_type": "collect"},
        "task_id": str(uuid.uuid7()),
        "controller_id": "mycontroller",
        "context": {"positions": [[10], [20]]},
        **submitted_fields,
    }

    executions = (
        TaskExecution(**fields),
        TaskExecution.model_validate_json(json.dumps(fields)),
    )

    for execution in executions:
        execution.context["positions"][0].append(30)
        assert execution.submitted_context["positions"] == [[10], [20]]


@pytest.mark.parametrize(
    "submitted",
    [KeywordDict(), KeywordDict({"positions": [[10], [20]]})],
    ids=("empty", "nested"),
)
def test_execution_retains_explicit_submitted_context(submitted):
    execution = TaskExecution(
        task=CollectTask(),
        task_id=uuid.uuid7(),
        controller_id="mycontroller",
        context=KeywordDict({"frame": 1}),
        submitted_context=submitted,
    )

    assert execution.submitted_context is submitted
    assert execution.context == {"frame": 1}
