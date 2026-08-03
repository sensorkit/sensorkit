# SPDX-License-Identifier: Apache-2.0
"""
Overrides: selection over ops, the three effects, and what each one
does to a run. The motivating case is a daytime test with the enclosure
shut — an init table compiled with the dome's ops answered for, and
everything downstream proceeding against a closed dome.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from sensorkit.workflow import (
    Entry,
    LifecycleError,
    LifecycleRunner,
    Op,
    OpContext,
    Override,
    Phase,
    PhaseTable,
    compile_table,
    format_graph,
)

# The daytime set: the enclosure never moves, in either direction. Both
# tables need it — a teardown that closes a dome nobody opened is the
# same mistake in reverse.
DAYTIME = (Override(trait="dome", ops=["open", "close"], outcome="ok",
                    reason="daytime test with the dome shut"),)


def table(*phases: Phase, on_failure: str = "stop") -> PhaseTable:
    return PhaseTable(name="t", phases=phases, on_failure=on_failure)


def hook(*, failures: frozenset[tuple[str, str]] = frozenset(),
         seen: list | None = None):
    async def perform(ctx: OpContext) -> object:
        if seen is not None:
            seen.append((ctx.op.ref, ctx.op.op))
        if (ctx.op.ref, ctx.op.op) in failures:
            raise RuntimeError("simulated fault")
        return None
    return perform


def op(ref="dome-1", name="open", **kw) -> Op:
    return Op(ref=ref, op=name, **kw)


# ---- selection ----------------------------------------------------------

def test_trait_clause_reads_the_capability_the_op_addresses():
    rule = Override(trait="dome", outcome="ok", reason="r")
    assert rule.matches(op(trait="dome", match="trait"))
    assert not rule.matches(op(trait="mount", match="trait"))


def test_trait_clause_falls_back_to_every_claim_for_a_per_device_op():
    """A `match: all` connect addresses no capability, so "nothing on
    the dome" has to reach it through the ref's claims."""
    rule = Override(trait="dome", outcome="ok", reason="r")
    assert rule.matches(op(name="connect", traits=("dome",), match="all"))
    assert not rule.matches(op(ref="tcs-1", name="connect",
                               traits=("focuser", "mount"), match="all"))


def test_a_multi_trait_device_is_narrowed_by_the_capability_addressed():
    """tcs-1 is the mount and the main OTA's focuser. Overriding the
    mount must not reach the focuser's ops."""
    rule = Override(trait="mount", outcome="ok", reason="r")
    assert rule.matches(op(ref="tcs-1", name="unpark", trait="mount",
                           traits=("focuser", "mount"), match="trait"))
    assert not rule.matches(op(ref="tcs-1", name="home", trait="focuser",
                               traits=("focuser", "mount"), match="trait"))


def test_selection_fields_conjoin():
    rule = Override(device="dome-1", ops="open", outcome="ok", reason="r")
    assert rule.matches(op())
    assert not rule.matches(op(name="close"))
    assert not rule.matches(op(ref="dome-2"))


def test_empty_ops_addresses_every_verb_on_the_selection():
    rule = Override(device="dome-1", outcome="ok", reason="r")
    assert rule.matches(op()) and rule.matches(op(name="close"))


def test_first_match_wins(devices):
    graph = compile_table(devices, table(Phase(name="p", entries=(
        Entry(trait="dome", ops="open"),))), (
        Override(device="dome-1", outcome="skipped", reason="specific"),
        Override(trait="dome", outcome="ok", reason="general"),
    ))
    assert graph.nodes[0].override.reason == "specific"


# ---- the model rejects what it cannot mean ------------------------------

def test_a_rule_must_select_something():
    with pytest.raises(ValidationError, match="selects nothing"):
        Override(outcome="ok", reason="r")


def test_a_rule_must_change_something():
    with pytest.raises(ValidationError, match="changes nothing"):
        Override(trait="dome", reason="r")


def test_outcome_excludes_a_failure_policy():
    """A step that will not be dispatched cannot fail, so declaring what
    its failure costs is a misunderstanding, not a nuance."""
    with pytest.raises(ValidationError, match="cannot fail"):
        Override(trait="dome", outcome="ok", optional=True, reason="r")


def test_the_reason_is_required():
    with pytest.raises(ValidationError):
        Override(trait="dome", outcome="ok")


# ---- outcome: the rung no author has ------------------------------------

def test_init_under_the_daytime_set(devices, init_table, assert_golden):
    assert_golden("init-daytime",
                  format_graph(compile_table(devices, init_table, DAYTIME)))


@pytest.mark.asyncio
async def test_an_overridden_step_never_reaches_the_hook(devices, init_table):
    seen: list = []
    report = await LifecycleRunner(hook(seen=seen)).run(
        devices, init_table, overrides=DAYTIME)
    assert ("dome-1", "open") not in seen
    assert ("dome-1", "home_shutters") in seen       # the rule named one op
    assert report.ok
    assert [(n.payload.op, why) for n, why in report.overridden] == [
        ("open", "daytime test with the dome shut")]


@pytest.mark.asyncio
async def test_dependents_of_an_ok_override_run(devices):
    """The whole point: the covers open, the focuser homes, the
    instruments cool, all against a dome that never moved."""
    seen: list = []
    report = await LifecycleRunner(hook(seen=seen)).run(devices, table(
        Phase(name="a", entries=(Entry(id="dome", trait="dome", ops="open"),)),
        Phase(name="b", entries=(
            Entry(trait="mirror_cover", ops="open", require="dome"),))),
        overrides=DAYTIME)
    assert ("cover-1", "open") in seen
    assert report.ok


@pytest.mark.asyncio
async def test_a_skipped_outcome_cascades_without_failing_the_run(devices):
    """The other rung: the plant is offline, so its steps do not run and
    neither does what they gate — and none of it is a failed run."""
    report = await LifecycleRunner(hook()).run(devices, table(
        Phase(name="a", entries=(Entry(id="chill", trait="chiller",
                                       ops="start"),)),
        Phase(name="b", entries=(
            Entry(match="instrument", ops="start_cooling", require="chill"),))),
        overrides=(Override(trait="chiller", outcome="skipped",
                            reason="plant offline; running warm"),))
    assert not report.failures
    assert {n.payload.op for n, _ in report.overridden} == {
        "start", "start_cooling"}


@pytest.mark.asyncio
async def test_a_genuine_failure_among_the_causes_is_still_a_failure(devices):
    """The excusal is not a laundering service: a node skipped because
    one dependency was overridden and another actually failed is an
    ordinary skip, and the run still raises."""
    with pytest.raises(LifecycleError):
        await LifecycleRunner(hook(failures=frozenset({("tcs-1", "unpark")}))).run(
            devices, table(
                Phase(name="a", entries=(
                    Entry(id="dome", trait="dome", ops="open"),
                    Entry(id="mount", trait="mount", ops="unpark"))),
                Phase(name="b", entries=(
                    Entry(trait="mirror_cover", ops="open",
                          require=("dome", "mount")),))),
            overrides=(Override(trait="dome", outcome="skipped",
                                reason="enclosure locked out"),))


@pytest.mark.asyncio
async def test_a_failed_dependency_beats_an_override_on_the_same_node(devices):
    """Skips resolve first, so an override can only remove work — it can
    never stand in front of a real failure and report success."""
    with pytest.raises(LifecycleError, match="simulated fault"):
        await LifecycleRunner(
            hook(failures=frozenset({("dome-1", "home_shutters")}))).run(
            devices, table(Phase(name="p", entries=(
                Entry(trait="dome", ops=("home_shutters", "open")),))),
            overrides=DAYTIME)


# ---- on_failure and optional: the two questions, bound late -------------

@pytest.mark.asyncio
async def test_optional_can_be_granted_by_a_caller(devices):
    """The guider is away at the vendor; its steps still run and still
    fail, and the run no longer cares."""
    report = await LifecycleRunner(
        hook(failures=frozenset({("cam-guide", "start_cooling")}))).run(
        devices, table(Phase(name="p", entries=(
            Entry(match="instrument", ops="start_cooling"),))),
        overrides=(Override(device="cam-guide", optional=True,
                            reason="guider away for repair"),))
    assert not report.failures
    assert [n.payload.ref for n, _ in report.degraded] == ["cam-guide"]


@pytest.mark.asyncio
async def test_on_failure_can_be_demoted_below_a_table_that_stops(devices):
    """`stop` halts dispatching for the whole run; demoting one step to
    `skip` confines the blast to what actually depended on it."""
    seen: list = []
    with pytest.raises(LifecycleError):
        await LifecycleRunner(
            hook(failures=frozenset({("dome-1", "open")}), seen=seen)).run(
            devices, table(
                Phase(name="a", entries=(Entry(trait="dome", ops="open"),)),
                Phase(name="b", entries=(
                    Entry(trait="mirror_cover", ops="open"),))),
            overrides=(Override(trait="dome", on_failure="skip",
                                reason="enclosure faults are not fatal today"),))
    assert ("cover-1", "open") in seen


# ---- reporting ----------------------------------------------------------

@pytest.mark.asyncio
async def test_the_summary_separates_not_attempted_from_not_working(
        devices, init_table):
    report = await LifecycleRunner(hook()).run(
        devices, init_table, overrides=DAYTIME)
    summary = report.summary()
    assert "overridden=1" in summary
    assert "daytime test with the dome shut" in summary
