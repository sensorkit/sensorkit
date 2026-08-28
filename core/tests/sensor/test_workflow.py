# SPDX-License-Identifier: Apache-2.0
"""What a compiler decides, and the graph `lower` builds from it.

Steps are hand built here rather than compiled, since neither compiler exists
yet and `lower` is what both stand on. The sensor is `sensor.yaml`, so what a
device supports is real and an omission is reachable.

Node ids follow emission order, and the tests read them that way.
"""
from __future__ import annotations


import pytest

from sensorkit.common.dag import DagRunner
from sensorkit.sensor.workflow import Dependency, Operation, Origin, PlannedStep, lower
from sensorkit.std.traits import Connect, Deinit, Disconnect, Home, MoveToPark


def step(name: str, target=None, command=None, origin=None,
         **kw) -> PlannedStep:
    """One planned step, named after itself."""
    return PlannedStep(
        name=name, origin=origin or Origin(source="test", path=(name,)),
        group="phase", target=target, command=command, **kw)


def built(facts, *steps: PlannedStep, **kw):
    """One workflow over these steps."""
    return lower("run", steps, facts, **kw)


# The step list holds together


def test_a_dependency_may_name_a_step_emitted_later(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect(),
                          deps=(Dependency(on="b"),)),
                     step("b", at("dome"), Connect()))

    assert workflow.graph.hard[0] == frozenset({1})


def test_two_steps_sharing_a_name_is_an_error(facts, at):
    with pytest.raises(ValueError, match="two steps are named 'a'"):
        built(facts, step("a", at("mount"), Connect()),
              step("a", at("dome"), Connect()))


def test_a_dependency_naming_no_step_is_an_error(facts, at):
    with pytest.raises(ValueError, match="nothing emitted: ghost"):
        built(facts, step("a", at("mount"), Connect(),
                          deps=(Dependency(on="ghost"),)))


def test_a_command_and_its_target_are_set_together(facts, at):
    with pytest.raises(ValueError, match="step 'a' has no target"):
        built(facts, step("a", command=Connect()))

    with pytest.raises(ValueError, match="step 'a' has no command"):
        built(facts, step("a", target=at("mount")))


# What each step becomes


def test_an_ordering_step_emits_a_node_carrying_no_operation(facts, at):
    workflow = built(facts,
                     step("align", delay_s=2.5,
                          origin=Origin(source="test", reason="midpoint")),
                     step("a", at("mount"), Connect(),
                          deps=(Dependency(on="align"),)))

    ordering = workflow.graph.nodes[0]

    assert ordering.payload is None
    assert ordering.delay_s == 2.5
    assert ordering.label == "midpoint"
    assert workflow.graph.hard[1] == frozenset({0})


def test_an_unsupported_required_command_is_an_error(facts, at):
    with pytest.raises(ValueError,
                       match="'cam-acq' does not support 'Disconnect'"):
        built(facts, step("a", at("cam-acq"), Disconnect()))


def test_an_unsupported_omitted_command_emits_no_node(facts, at):
    workflow = built(facts, step("a", at("cam-acq"), Disconnect(),
                                 unsupported="omit"))
    omission = workflow.omissions[0]

    assert workflow.graph.nodes == ()
    assert omission.target == at("cam-acq")
    assert omission.command == "Disconnect"
    assert omission.origin.source == "test"


def test_a_supported_command_emits_an_operation(facts, at):
    workflow = built(facts, step("a", at("mount"), Connect()))
    operation = workflow.graph.nodes[0].payload

    assert isinstance(operation, Operation)
    assert operation.target == at("mount")
    assert workflow.omissions == ()


# Dependencies through an omission


def omitted(name: str, at, **kw) -> PlannedStep:
    """A step that omits, since `cam-acq` cannot disconnect."""
    return step(name, at("cam-acq"), Disconnect(), unsupported="omit", **kw)


def test_a_dependent_inherits_an_omitted_step_dependencies(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect()),
                     omitted("b", at, deps=(Dependency(on="a"),)),
                     step("c", at("dome"), Connect(),
                          deps=(Dependency(on="b"),)))

    # a is node 0 and c is node 1, b having emitted nothing.
    assert workflow.graph.hard[1] == frozenset({0})


def test_inheritance_is_transitive_through_a_chain_of_omissions(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect()),
                     omitted("b", at, deps=(Dependency(on="a"),)),
                     omitted("c", at, deps=(Dependency(on="b"),)),
                     step("d", at("dome"), Connect(),
                          deps=(Dependency(on="c"),)))

    assert workflow.graph.hard[1] == frozenset({0})


def test_an_omitted_step_with_no_dependencies_leaves_one_fewer(facts, at):
    workflow = built(facts,
                     omitted("b", at),
                     step("c", at("dome"), Connect(),
                          deps=(Dependency(on="b"),)))

    # Not a skip, which would excuse the work that waited on it.
    assert workflow.graph.deps[0] == frozenset()
    assert workflow.graph.nodes[0].override is None


@pytest.mark.parametrize("through,stated,hard", [
    ("success", "success", True),
    ("success", "completion", False),
    ("completion", "success", False),
    ("completion", "completion", False),
])
def test_an_inherited_dependency_composes_both_links(facts, at, through,
                                                     stated, hard):
    """Success only where every link asked for one."""
    workflow = built(
        facts,
        step("a", at("mount"), Connect()),
        omitted("b", at, deps=(Dependency(on="a", kind=stated),)),
        step("c", at("dome"), Connect(),
             deps=(Dependency(on="b", kind=through),)))

    assert workflow.graph.deps[1] == frozenset({0})
    assert workflow.graph.hard[1] == (frozenset({0}) if hard else frozenset())


def test_one_completion_anywhere_on_a_chain_weakens_the_result(facts, at):
    workflow = built(
        facts,
        step("a", at("mount"), Connect()),
        omitted("b", at, deps=(Dependency(on="a"),)),
        omitted("c", at, deps=(Dependency(on="b", kind="completion"),)),
        step("d", at("dome"), Connect(), deps=(Dependency(on="c"),)))

    assert workflow.graph.deps[1] == frozenset({0})
    assert workflow.graph.hard[1] == frozenset()


@pytest.mark.parametrize("order", [("weak", "strong"), ("strong", "weak")])
def test_success_wins_where_two_paths_reach_one_predecessor(facts, at, order):
    workflow = built(
        facts,
        step("a", at("mount"), Connect()),
        omitted("strong", at, deps=(Dependency(on="a"),)),
        omitted("weak", at, deps=(Dependency(on="a", kind="completion"),)),
        step("c", at("dome"), Connect(),
             deps=tuple(Dependency(on=name) for name in order)))

    assert workflow.graph.hard[1] == frozenset({0})


def test_two_weakened_paths_leave_one_ordering_edge(facts, at):
    workflow = built(
        facts,
        step("a", at("mount"), Connect()),
        omitted("b", at, deps=(Dependency(on="a", kind="completion"),)),
        omitted("c", at, deps=(Dependency(on="a", kind="completion"),)),
        step("d", at("dome"), Connect(),
             deps=(Dependency(on="b"), Dependency(on="c"))))

    assert workflow.graph.deps[1] == frozenset({0})
    assert workflow.graph.hard[1] == frozenset()


@pytest.mark.asyncio
async def test_a_weakened_dependency_outlives_the_failure(facts, at):
    """What the composition rule is for.

    `c` asked only that `b` be attempted, and `b` omitted. Inheriting b's
    requirement of a's success would make c wait on a run that failed, for a
    success c never asked anything about.
    """
    workflow = built(
        facts,
        step("a", at("mount"), Connect(), fail_fast=False),
        omitted("b", at, deps=(Dependency(on="a"),)),
        step("c", at("dome"), Connect(),
             deps=(Dependency(on="b", kind="completion"),)))

    async def dispatch(node):
        if node.id == 0:
            raise RuntimeError("the mount is off")

        return None

    report = await DagRunner(dispatch).execute(workflow.graph, name="run")

    assert report.results[0].status == "failed"
    assert report.results[1].status == "ok"


def test_omitted_steps_depending_on_each_other_raise(facts, at):
    with pytest.raises(ValueError, match="omitted steps depend on each other"):
        built(facts,
              omitted("b", at, deps=(Dependency(on="c"),)),
              omitted("c", at, deps=(Dependency(on="b"),)))


# Edges and failure policy


def test_success_is_a_hard_edge_and_completion_a_soft_one(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect()),
                     step("b", at("dome"), Connect()),
                     step("c", at("cover"), Connect(),
                          deps=(Dependency(on="a"),
                                Dependency(on="b", kind="completion"))))

    assert workflow.graph.hard[2] == frozenset({0})
    assert workflow.graph.deps[2] == frozenset({0, 1})


def test_fail_fast_lowers_to_stop_and_its_absence_to_skip(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect()),
                     step("b", at("dome"), Connect(), fail_fast=False))

    assert workflow.graph.nodes[0].on_failure == "stop"
    assert workflow.graph.nodes[1].on_failure == "skip"


def test_no_node_forgives_a_hard_edge(facts, at):
    workflow = built(facts,
                     step("a", at("mount"), Connect(), fail_fast=False),
                     step("b", at("dome"), Connect(), optional=True))

    assert all(n.on_failure != "continue" for n in workflow.graph.nodes)




def test_the_planned_command_is_copied(facts, at):
    from sensorkit.std.optics import SetFilter

    command = SetFilter(filter="r")
    workflow = built(facts, step("a", at("wheel"), command,
                                 unsupported="omit"),
                     step("b", at("mount"), Connect()))
    command.filter = "g"

    # `wheel` does not set filters, so only the mount step emitted.
    assert workflow.graph.nodes[0].payload.command is not command
    assert workflow.omissions[0].command == "SetFilter"


# Operator rules












# Deadlines














# Cleanup
















# The compiled workflow holds together


















def test_provenance_and_name_are_carried(facts, at):
    workflow = lower("bring-up", (step("a", at("mount"), Connect()),), facts,
                     provenance="facts taken at noon")

    assert workflow.name == "bring-up"
    assert workflow.provenance == "facts taken at noon"


def test_a_command_nothing_supports_omits_wherever_it_lands(facts, at):
    workflow = built(facts, step("a", at("mount"), MoveToPark(),
                                 unsupported="omit"))

    assert workflow.graph.nodes == ()
    assert workflow.omissions[0].command == "MoveToPark"


def test_two_steps_of_one_line_on_one_device_are_distinguishable(facts, at):
    # Same origin path would mean the same operation id, which validation
    # rejects rather than letting two operations answer to one name.
    with pytest.raises(ValueError, match="operations share an id"):
        built(facts,
              PlannedStep(name="a", origin=Origin(source="t"), group="g",
                          target=at("mount"), command=Connect()),
              PlannedStep(name="b", origin=Origin(source="t"), group="g",
                          target=at("mount"), command=Deinit(),
                          unsupported="omit"),
              PlannedStep(name="c", origin=Origin(source="t"), group="g",
                          target=at("mount"), command=Home()))
