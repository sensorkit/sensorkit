from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Literal, final, override

from loguru import logger
from pydantic import BaseModel

from sensorkit.common.condition import AnyCondition, resolve_field
from sensorkit.core.client import SensorKit
from sensorkit.models.safety import BasicSafety
from sensorkit.std.weather import BasicWeather

type AnyConstraint = WeatherConstraint | SafetyConstraint | GenericConstraint


class Constraint(BaseModel, ABC):
    """Abstract operating constraint that runs a background monitoring task."""
    kind: str

    @final
    def make_evaluator(self):
        """Create and return a ConstraintEvaluator bound to this constraint."""
        return ConstraintEvaluator(self)

    # FIXME: Use Context or similar instead of **kwargs
    @abstractmethod
    async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
        """Long-running coroutine that sets or clears *evaluator.active* as conditions change."""
        ...


class ConstraintEvaluator:
    """Runs a Constraint's monitoring task and exposes ``ready`` and ``active`` events."""

    def __init__(self, constraint: Constraint):
        self.constraint = constraint
        self.ready = asyncio.Event()
        self.active = asyncio.Event()
        self._task: asyncio.Task | None = None

    def start(self, *, task_group: asyncio.TaskGroup | None = None, **kwargs):
        """Launch the constraint monitoring task in *task_group* (or the running loop)."""
        assert not self._task
        task_group = task_group or asyncio
        self._task = task_group.create_task(self.constraint.check_task(self, **kwargs))


class WeatherConstraint(Constraint):
    """Constraint that monitors a weather provider and activates when conditions exceed thresholds."""
    kind: Literal["weather"] = "weather"
    provider: str
    humidity_max: float = float("inf")
    humidity_deadband: float = 0.0
    wind_max: float = float("inf")
    wind_deadband: float = 0.0
    rain_max: float = float("inf")
    rain_deadband: float = 0.0
    time_to_live: float = 30.0
    hold_duration: float = 0.0

    def check_weather(self, weather: BasicWeather, was_active: bool = False) -> bool:
        """Returns True to activate, False to clear, or previous state (deadband)."""
        if (
            weather.humidity is None
            or weather.wind_speed is None
            or weather.rain_rate is None
        ):
            return True
        elif (
            weather.humidity > self.humidity_max
            or weather.wind_speed > self.wind_max
            or weather.rain_rate > self.rain_max
        ):
            return True
        elif (
            weather.humidity < self.humidity_max - self.humidity_deadband
            or weather.wind_speed < self.wind_max - self.wind_deadband
            or weather.rain_rate < self.rain_max - self.rain_deadband
        ):
            return False
        return was_active

    async def _hold_and_clear(self, active: asyncio.Event):
        await asyncio.sleep(self.hold_duration)
        logger.info("Clearing weather constraint (hold period expired)")
        active.clear()

    def _apply_result(
        self,
        result: bool,
        active: asyncio.Event,
        hold_task: asyncio.Task | None,
        from_data: bool,
    ) -> tuple[asyncio.Task | None, bool]:
        """Apply a weather check result. Returns (hold_task, from_data)."""
        if result is True:
            if hold_task is not None and not hold_task.done():
                hold_task.cancel()
                hold_task = None
            if not active.is_set():
                logger.info("Setting weather constraint")
            active.set()
            return hold_task, True
        elif result is False and active.is_set():
            if self.hold_duration > 0 and from_data:
                if hold_task is None or hold_task.done():
                    logger.info(f"Weather cleared, holding constraint for {self.hold_duration}s")
                    hold_task = asyncio.create_task(self._hold_and_clear(active))
            else:
                logger.info("Clearing weather constraint")
                active.clear()
        return hold_task, from_data

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        logger.debug("monitoring weather constraints")

        # Get a client to the configured weather provider and monitor the weather stream. An error
        # here is fatal, so we propagate the exception to blow up the calling ThreadGroup.
        provider = kit.entity(self.provider)
        stream = await provider.monitor(BasicWeather)
        hold_task: asyncio.Task | None = None
        from_data = False  # tracks whether active was set by a measurement vs. a disconnect

        while True:
            try:
                async with asyncio.timeout(self.time_to_live) as timeout:
                    async for _, weather in stream:
                        hold_task, from_data = self._apply_result(
                            self.check_weather(weather, evaluator.active.is_set()), evaluator.active, hold_task, from_data
                        )

                        # Mark as ready once we have received at least one weather record.
                        evaluator.ready.set()

                        # Update our timeout.
                        timeout.reschedule(asyncio.get_running_loop().time() + self.time_to_live)
            except TimeoutError:
                from_data = False
                if hold_task is not None and not hold_task.done():
                    hold_task.cancel()
                    hold_task = None
                if not evaluator.active.is_set():
                    logger.info(f"Setting weather constraint (no data for {self.time_to_live}s)")
                evaluator.active.set()

            # Mark as ready.
            evaluator.ready.set()

            # FIXME: We rebuild the subscription after every timeout because it is not possible to
            #        cleanly "resume" iterating an async generator after cancellation. The solution
            #        is to build the observer/multiplexer functionality into telemetry streams like
            #        we already do for event streams. Then we can just get an `asyncio.Queue` from
            #        the API.
            stream = await provider.monitor(BasicWeather)
            await anext(stream)  # consume the first value, which is not necessarily fresh


class SafetyConstraint(Constraint):
    """Constraint that monitors a BasicSafety provider and activates when conditions are unsafe."""
    kind: Literal["safety"] = "safety"
    provider: str
    time_to_live: float = 30.0

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        logger.debug(f"monitoring safety constraint from {self.provider}")

        provider = kit.entity(self.provider)
        stream = await provider.monitor(BasicSafety)

        while True:
            try:
                async with asyncio.timeout(self.time_to_live) as timeout:
                    async for _, safety in stream:
                        if not safety.is_safe:
                            if not evaluator.active.is_set():
                                logger.info(f"Setting safety constraint from {self.provider}")
                            evaluator.active.set()
                        else:
                            if evaluator.active.is_set():
                                logger.info(f"Clearing safety constraint from {self.provider}")
                            evaluator.active.clear()

                        evaluator.ready.set()

                        timeout.reschedule(asyncio.get_running_loop().time() + self.time_to_live)
            except TimeoutError:
                if not evaluator.active.is_set():
                    logger.info(f"Setting safety constraint from {self.provider} (no data for {self.time_to_live}s)")

                evaluator.active.set()

            stream = await provider.monitor(BasicSafety)
            await anext(stream)

class GenericConstraint(Constraint):
    """Generic constraint driven by a Condition evaluated against any entity keyword."""

    kind: Literal["conditional"] = "conditional"
    entity: str
    keyword: str
    field: str | None = None
    condition: AnyCondition
    time_to_live: float = 30.0

    def _apply(
        self, evaluator: ConstraintEvaluator, current: object, previous: object, was_active: bool, label: str
    ) -> tuple[object, bool]:
        """Evaluate the condition and update the evaluator. Returns (current, was_active)."""
        _, is_active = self.condition.evaluate(current, previous, was_active)

        if is_active:
            if not evaluator.active.is_set():
                logger.info(f"Setting conditional constraint on {label}")
            evaluator.active.set()
        else:
            if evaluator.active.is_set():
                logger.info(f"Clearing conditional constraint on {label}")
            evaluator.active.clear()

        return current, is_active

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        label = f"{self.entity}.{self.keyword}"
        if self.field:
            label += f".{self.field}"
        logger.debug(f"monitoring conditional constraint on {label}")

        _UNSET = object()
        previous: object = _UNSET
        was_active = False

        client = kit.entity(self.entity)
        consumer = await client._stream.consume(include_latest=True)

        while True:
            try:
                async with asyncio.timeout(self.time_to_live) as timeout:
                    async for msg in consumer:
                        if msg.subject.prop != self.keyword:
                            continue

                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue

                        current = resolve_field(data, self.field) if self.field else data

                        if previous is not _UNSET:
                            previous, was_active = self._apply(
                                evaluator, current, previous, was_active, label
                            )
                            evaluator.ready.set()
                        else:
                            previous = current

                        timeout.reschedule(
                            asyncio.get_running_loop().time() + self.time_to_live
                        )

            except TimeoutError:
                if not evaluator.active.is_set():
                    logger.info(
                        f"Setting conditional constraint on {label} "
                        f"(no data for {self.time_to_live}s)"
                    )
                evaluator.active.set()

            consumer = await client._stream.consume(include_latest=True)
