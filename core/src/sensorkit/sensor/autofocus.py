# SPDX-License-Identifier: Apache-2.0
"""Plan a fixed autofocus sweep and read back its analysis.

Each position requests a focuser move followed by one light frame.
Frames carry `FocusSweepContext` identifying the sweep and sample.
The configured provider publishes `FocusSweepAnalysis` for the controller
to select the final focuser position.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

import sensorkit.api as sk
from sensorkit.backend.base import KeyNotFound
from sensorkit.common.aio import cleanup_future, defer_cancellation
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
)
from sensorkit.sensor.lifecycle import Entry, LifecycleWorkflow, OpSpec, Phase
from sensorkit.sensor.selection import IsRef, Supports
from sensorkit.sensor.standard_task import capture_settings, instrument_selection
from sensorkit.sensor.topology import Placement
from sensorkit.std.focus import (
    AutoFocus,
    ChangeFocusPosition,
    FocusAnalysisSource,
    FocusPosition,
    FocusProvider,
    FocusSweepAnalysis,
    FocusSweepContext,
)


def translate_autofocus(
    task: AutoFocus,
    positions: list[float],
    *,
    sweep_id: UUID,
    focuser: str,
    instruments: Mapping[str, Placement],
    readout_margin_s: float = 60.0,
) -> CollectIntent:
    """Translate a fixed focus sweep into one epoch per position.

    Preserve the supplied position order, pointing, and tracking.
    Each frame carries `FocusSweepContext`.
    Capture timeouts allow the exposure duration plus the readout margin.

    Raises:
        ValueError: The instrument name is unknown.
    """
    selection = instrument_selection(task.instrument, instruments)
    camera = capture_settings(
        task.filter_name, binning=task.binning, gain=task.gain, readout_mode=task.readout_mode
    )
    count = len(positions)

    def sample(index: int, position: float) -> RequestEpoch:
        move = CommandRequest(command=ChangeFocusPosition(position=position), device=focuser)
        request = InstrumentRequest(
            id=f"sample-{index}",
            assignment="sweep",
            acquisition=AcquisitionRequest(
                integration_time_s=task.integration_time_seconds,
                timeout_s=task.integration_time_seconds + readout_margin_s,
            ),
            keywords=(
                FocusSweepContext(
                    sweep_id=sweep_id,
                    sample_index=index,
                    sample_count=count,
                    commanded_position=position,
                ),
            ),
            select=selection,
            settings=(move, *camera),
        )

        return RequestEpoch(units=(request,))

    return CollectIntent(
        name=task.task_type,
        epochs=tuple(sample(index, position) for index, position in enumerate(positions)),
    )


@dataclass(frozen=True)
class Focuser:
    """The one focuser on an instrument's chain and its starting position."""

    device: str
    start: float

    @classmethod
    def on(
        cls,
        sensor: BoundSensor,
        instrument: str,
        instruments: Mapping[str, Placement],
        contexts: Mapping[str, sk.Context],
    ) -> Focuser:
        """Find the instrument's focuser and its reported position.

        Raises:
            ValueError: The instrument is unknown, its chain has no single
                focuser, or the focuser reports no finite position.
        """
        # Reject an unknown name with the error collect uses.
        instrument_selection(instrument, instruments)
        chain = sensor.topology.chain(instruments[instrument])

        match Supports(supports=ChangeFocusPosition).refs(chain, sensor):
            case (device,):
                pass
            case ():
                raise ValueError(f"Instrument '{instrument}' has no focuser")
            case found:
                raise ValueError(
                    f"Instrument '{instrument}' has focusers {', '.join(found)}; "
                    f"an autofocus moves exactly one"
                )

        reported = contexts[device].get(FocusPosition)

        if reported is None or not math.isfinite(reported.position):
            raise ValueError(f"Focuser '{device}' reports no position to return to")

        return cls(device=device, start=reported.position)

    def move(self, position: float) -> LifecycleWorkflow:
        """Request one move, failing on its first error."""
        return LifecycleWorkflow(
            name="autofocus",
            fail_fast=True,
            phases=(
                Phase(
                    name="focus",
                    entries=(
                        Entry(
                            select=IsRef(device=self.device),
                            ops=(OpSpec(command=ChangeFocusPosition(position=position)),),
                        ),
                    ),
                ),
            ),
        )


async def focus_source(controller: sk.ControllerImpl) -> sk.EntityClient:
    """Resolve the controller's configured focus analysis entity.

    Raises:
        ValueError: No source is configured, or it is offline or does not
            publish focus analysis.
    """
    try:
        configured = await controller.kv_get_model(FocusAnalysisSource)
    except KeyNotFound:
        raise ValueError("No FocusAnalysisSource names a focus analysis entity") from None

    name = configured.entity.name
    source = controller.sensorkit().entity(name)

    if not await source.is_online():
        raise ValueError(f"Focus analysis source '{name}' is offline")

    try:
        info = await source.kv_get_model(sk.EntityInfo)
    except KeyNotFound:
        info = None

    if info is None or not FocusProvider.match(info.details):
        raise ValueError(f"'{name}' does not publish focus analysis")

    return source


@contextlib.asynccontextmanager
async def first_sweep_analysis(
    source: sk.EntityClient, *, controller: str, instrument: str, sweep_id: UUID
) -> AsyncIterator[asyncio.Task[FocusSweepAnalysis]]:
    """Yield a task resolving to the first analysis of one sweep from `source`.

    The subscription is live before this yields and is released on exit.
    """
    identity = (controller, instrument, sweep_id)

    def wanted(result: FocusSweepAnalysis) -> bool:
        return (result.controller_id.name, result.instrument, result.sweep_id) == identity

    subscribed = asyncio.get_running_loop().create_future()
    waiter = asyncio.create_task(_receive(source, wanted, subscribed))

    try:
        await asyncio.wait({subscribed, waiter}, return_when=asyncio.FIRST_COMPLETED)

        if not subscribed.done():
            waiter.result()

        yield waiter
    finally:
        waiter.cancel()
        await defer_cancellation(asyncio.wait({waiter}))
        cleanup_future(waiter)


async def _receive(
    source: sk.EntityClient,
    wanted: Callable[[FocusSweepAnalysis], bool],
    subscribed: asyncio.Future[None],
) -> FocusSweepAnalysis:
    """Return the first wanted analysis, resolving `subscribed` once results can arrive."""
    results = await source.monitor(FocusSweepAnalysis)

    async with contextlib.aclosing(results):
        subscribed.set_result(None)

        async for _, analysis in results:
            if wanted(analysis):
                return analysis

    raise RuntimeError(f"'{source.entity}' stopped publishing focus analysis")


def final_focus_position(analysis: FocusSweepAnalysis, positions: list[float]) -> float:
    """Return the final focuser position from the sweep analysis.

    Return the estimated best focus within the sweep, or the sampled boundary
    in the direction of best focus.

    Raises:
        RuntimeError: The analysis failed or puts its position outside the sweep.
    """
    low, high = min(positions), max(positions)

    match analysis:
        case FocusSweepAnalysis(outcome="failure", reason=reason):
            raise RuntimeError(f"Focus sweep analysis failed: {reason}")
        case FocusSweepAnalysis(position=float(position)) if low <= position <= high:
            return position
        case FocusSweepAnalysis(position=position):
            raise RuntimeError(
                f"Focus position {position} lies outside the swept range {low} to {high}"
            )
