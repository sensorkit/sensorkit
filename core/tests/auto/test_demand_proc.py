# SPDX-License-Identifier: Apache-2.0
"""Tests for demand procedure completion and error inspection."""

import asyncio

import pytest
import pytest_asyncio

from sensorkit.auto.lifecycle import ControllerLifecycle, DemandProc, DemandProcError, DemandState


class GatedProc(DemandProc):
    """Run demand procedure logic with a controlled outcome."""

    def __init__(self):
        super().__init__(ControllerLifecycle(), DemandState(state=None))
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.outcome: Exception | None = None

    async def state_logic(self):
        self.started.set()
        await self.release.wait()

        if self.outcome is not None:
            raise self.outcome


@pytest_asyncio.fixture
async def proc():
    procedure = GatedProc()
    procedure.start()

    try:
        async with asyncio.timeout(5.0):
            await procedure.started.wait()

        yield procedure
    finally:
        procedure.cancel()

        async with asyncio.timeout(5.0):
            await asyncio.wait([procedure.future()])

        if not procedure.future().cancelled():
            procedure.future().exception()


@pytest.mark.asyncio
async def test_error_while_running(proc):
    """A running procedure has no terminal error."""
    assert not proc.done()
    assert proc.error() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [None, DemandProcError("tasking failed", kind="program"), RuntimeError("logic failed")],
    ids=["success", "program_error", "ordinary_error"],
)
async def test_error_after_completion(proc, outcome):
    """Completion preserves the original exception and its category."""
    proc.outcome = outcome
    proc.release.set()

    async with asyncio.timeout(5.0):
        await asyncio.wait([proc.future()])

    assert proc.done()
    assert proc.error() is outcome

    if isinstance(outcome, DemandProcError):
        assert outcome.kind == "program"


@pytest.mark.asyncio
async def test_error_after_cancellation(proc):
    """Inspecting a cancelled procedure returns without raising cancellation."""
    proc.cancel()

    async with asyncio.timeout(5.0):
        await asyncio.wait([proc.future()])

    assert proc.done()
    assert proc.future().cancelled()
    assert proc.error() is None
