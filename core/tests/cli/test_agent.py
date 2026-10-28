# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime, timedelta

import pytest

from sensorkit.auto.agent import (
    AgentControllerInfo,
    AgentOperatingState,
    AgentRuntimeState,
    AgentSchedulerState,
    AgentState,
    ControllerRuntimeState,
)
from sensorkit.auto.constraint import ConstraintState
from sensorkit.auto.failure import FailureStateSnapshot
from sensorkit.auto.lifecycle import LifecycleState
from sensorkit.auto.procedures import LifecycleStep
from sensorkit.cli.agent import status


def agent_state(*controllers: str, runtime_state: AgentRuntimeState | None = None):
    return AgentState(
        operating_state=AgentOperatingState(
            global_control_enabled=True,
            controllers={name: AgentControllerInfo.default() for name in controllers},
        ),
        scheduler_state=AgentSchedulerState(scheduling_enabled=True),
        runtime_state=runtime_state or AgentRuntimeState(),
    )


def runtime_state(failure_state: FailureStateSnapshot, **kwargs):
    return ControllerRuntimeState(
        lifecycle=LifecycleState(
            step=kwargs.get("step", LifecycleStep.WAIT),
            failure_state=failure_state,
            last_reactor_error=kwargs.get("last_reactor_error"),
        ),
        constrained=kwargs.get("constrained", False),
        constraints=kwargs.get("constraints", []),
    )


async def render(kit, state: AgentState, capsys) -> str:
    await kit.entity("agent").kv_put_model(state)
    await status.callback.__wrapped__(kit, entity="agent")
    return capsys.readouterr().out


@pytest.mark.asyncio
async def test_status_distinguishes_missing_runtime_state_from_zero_failures(kit, capsys):
    runtime = AgentRuntimeState(controllers={"ctrl1": runtime_state(FailureStateSnapshot())})
    sampled = datetime.fromtimestamp(runtime.timestamp() / 1000, UTC)

    output = await render(kit, agent_state("ctrl1", "ctrl2", runtime_state=runtime), capsys)
    first, second = output.split("Controller 2:")

    assert f"Runtime sampled:   {sampled.isoformat()}" in output
    assert "Lifecycle step:  WAIT" in first
    assert "Failures:        0 of 2 before recovery" in first
    assert "Recovery:        0 of 6 failed attempts\n" in first
    assert "Runtime state:   UNAVAILABLE" in second
    assert "Failures:" not in second


@pytest.mark.asyncio
async def test_status_without_runtime_state_reports_no_sample(kit, capsys):
    output = await render(kit, agent_state("ctrl1"), capsys)

    assert "Runtime sampled:" not in output
    assert "Runtime state:   UNAVAILABLE" in output


@pytest.mark.asyncio
async def test_status_renders_failures_and_constraints(kit, capsys):
    runtime = AgentRuntimeState(controllers={})
    sampled = datetime.fromtimestamp(runtime.timestamp() / 1000, UTC)
    retry_at = sampled + timedelta(seconds=20)
    failure_state = FailureStateSnapshot(
        failures=2,
        attempts=1,
        retry_at=retry_at,
        failure="ControllerFailure: lost",
        settled_at=sampled - timedelta(seconds=30),
    )
    constraints = [
        ConstraintState(kind="conditional", state="active", reason="weather.Rain = Wet"),
        ConstraintState(kind="conditional", state="not_ready"),
    ]
    runtime = runtime.model_copy(
        update=dict(
            controllers={
                "ctrl1": runtime_state(
                    failure_state,
                    step=LifecycleStep.RECOVER,
                    last_reactor_error="RuntimeError: monitor lost",
                    constrained=True,
                    constraints=constraints,
                )
            }
        )
    )

    output = await render(kit, agent_state("ctrl1", runtime_state=runtime), capsys)

    assert "Lifecycle step:  RECOVER" in output
    assert "Failures:        2 of 2 before recovery" in output
    assert "Recovery:        1 of 6 failed attempts, recovery due" in output
    assert f"Retry at:        {retry_at.isoformat()}" in output
    assert "Settled for:     30 of 900 s" in output
    assert "Procedure failure:  ControllerFailure: lost" in output
    assert "Last reactor error: RuntimeError: monitor lost" in output
    assert "Constrained:     YES" in output
    assert (
        "    - conditional: ACTIVE (weather.Rain = Wet)\n    - conditional: NOT_READY\n" in output
    )
