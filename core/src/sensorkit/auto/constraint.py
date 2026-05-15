from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Literal, final, override

from loguru import logger
from pydantic import BaseModel

from sensorkit.common.condition import AnyCondition, resolve_field
from sensorkit.common.keyword import declare_keyword
from sensorkit.core.client import SensorKit
from sensorkit.models.safety import BasicSafety
from sensorkit.std.weather import BasicWeather

type AnyConstraint = WeatherConstraint | SafetyConstraint | GenericConstraint


@declare_keyword
class ConstraintStatus(BaseModel):
    """Published when a constraint is set or cleared."""

    constraint_kind: str  # "weather", "safety", "conditional"
    active: bool
    reason: str
    provider: str | None = None


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
        # Fail-closed default: a constraint starts active and is only cleared once positive,
        # fresh data confirms the condition is safe. This way, a check_task that crashes
        # before its first observation (or never gets one) leaves the controller constrained
        # rather than free to operate.
        self.active = asyncio.Event()
        self.active.set()
        self._task: asyncio.Task | None = None
        self._on_change: Callable[[ConstraintStatus], Awaitable[None]] | None = None

    def set_on_change(self, callback: Callable[[ConstraintStatus], Awaitable[None]]):
        """Register a callback invoked when the constraint state changes."""
        self._on_change = callback

    async def _notify(self, status: ConstraintStatus):
        """Fire the on_change callback if registered."""
        if self._on_change is not None:
            try:
                await self._on_change(status)
            except Exception:
                logger.debug("Failed to publish ConstraintStatus", exc_info=True)

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

    def check_weather(self, weather: BasicWeather, was_active: bool = False) -> tuple[bool, str]:
        """Returns (active, reason) describing whether the constraint should be set."""
        if (
            weather.humidity is None
            or weather.wind_speed is None
            or weather.rain_rate is None
        ):
            missing = []
            if weather.humidity is None:
                missing.append("humidity")
            if weather.wind_speed is None:
                missing.append("wind_speed")
            if weather.rain_rate is None:
                missing.append("rain_rate")
            return True, f"missing data ({', '.join(missing)})"

        exceeded = []
        if weather.humidity > self.humidity_max:
            exceeded.append(f"humidity {weather.humidity:.0f}% > {self.humidity_max:.0f}%")
        if weather.wind_speed > self.wind_max:
            exceeded.append(f"wind {weather.wind_speed:.1f} > {self.wind_max:.1f}")
        if weather.rain_rate > self.rain_max:
            exceeded.append(f"rain {weather.rain_rate:.2f} > {self.rain_max:.2f}")
        if exceeded:
            return True, ", ".join(exceeded)

        # Clear only when ALL parameters are below their deadband-adjusted thresholds.
        # If any parameter is in the deadband zone, stay in the current state. The original
        # version of this check used `or` instead of `and` — that would clear the constraint
        # as soon as any single metric dropped below its deadband, even while another metric
        # was still over its max. Don't reintroduce that.
        all_cleared = (
            weather.humidity < self.humidity_max - self.humidity_deadband
            and weather.wind_speed < self.wind_max - self.wind_deadband
            and weather.rain_rate < self.rain_max - self.rain_deadband
        )
        if all_cleared:
            reasons = []
            reasons.append(f"humidity {weather.humidity:.0f}% < {self.humidity_max - self.humidity_deadband:.0f}%")
            reasons.append(f"wind {weather.wind_speed:.1f} < {self.wind_max - self.wind_deadband:.1f}")
            reasons.append(f"rain {weather.rain_rate:.2f} < {self.rain_max - self.rain_deadband:.2f}")
            return False, ", ".join(reasons)
        return was_active, "deadband"

    async def _hold_and_clear(self, active: asyncio.Event, evaluator: ConstraintEvaluator):
        await asyncio.sleep(self.hold_duration)
        logger.info("Clearing weather constraint (hold period expired)")
        active.clear()
        await evaluator._notify(ConstraintStatus(
            constraint_kind=self.kind, active=False, reason="hold period expired", provider=self.provider,
        ))

    async def _apply_result(
        self,
        result: tuple[bool, str],
        evaluator: ConstraintEvaluator,
        hold_task: asyncio.Task | None,
        from_data: bool,
    ) -> tuple[asyncio.Task | None, bool]:
        """Apply a weather check result. Returns (hold_task, from_data)."""
        is_active, reason = result
        active = evaluator.active
        if is_active is True:
            if hold_task is not None and not hold_task.done():
                hold_task.cancel()
                hold_task = None
                logger.info(f"Weather hold reset: conditions worsened again ({reason})")
            was_clear = not active.is_set()
            if was_clear:
                logger.info(f"Setting weather constraint ({reason})")
                await evaluator._notify(ConstraintStatus(
                    constraint_kind=self.kind, active=True, reason=reason, provider=self.provider,
                ))
            active.set()
            # Only flip from_data=True on a real clear→active transition. A confirming sample
            # on an already-set constraint (fail-closed init, stale-cache replay, or repeated
            # exceedance) must not poison the flag, or the next clear would be held back by
            # hold_duration on data we never actually saw fall and rise.
            return hold_task, True if was_clear else from_data
        elif is_active is False and active.is_set():
            if self.hold_duration > 0 and from_data:
                if hold_task is None or hold_task.done():
                    logger.info(f"Weather cleared ({reason}), holding constraint for {self.hold_duration}s")
                    hold_task = asyncio.create_task(self._hold_and_clear(active, evaluator))
            else:
                logger.info(f"Clearing weather constraint ({reason})")
                active.clear()
                await evaluator._notify(ConstraintStatus(
                    constraint_kind=self.kind, active=False, reason=reason, provider=self.provider,
                ))
        return hold_task, from_data

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        logger.debug("monitoring weather constraints")

        # Get a client to the configured weather provider and monitor the weather stream. An error
        # here is fatal, so we propagate the exception to blow up the calling ThreadGroup. The
        # timeout bound below ensures we fail-fast on a hung provider rather than wait forever.
        provider = kit.entity(self.provider)
        stream = await asyncio.wait_for(
            provider.monitor(BasicWeather), timeout=self.time_to_live
        )
        hold_task: asyncio.Task | None = None
        from_data = False  # tracks whether active was set by a measurement vs. a disconnect

        while True:
            try:
                async with asyncio.timeout(self.time_to_live) as timeout:
                    async for _, weather in stream:
                        hold_task, from_data = await self._apply_result(
                            self.check_weather(weather, evaluator.active.is_set()), evaluator, hold_task, from_data
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
                reason = f"no data for {self.time_to_live}s"
                if not evaluator.active.is_set():
                    logger.info(f"Setting weather constraint ({reason})")
                    await evaluator._notify(ConstraintStatus(
                        constraint_kind=self.kind, active=True, reason=reason, provider=self.provider,
                    ))
                evaluator.active.set()

            # Mark as ready.
            evaluator.ready.set()

            # FIXME: We rebuild the subscription after every timeout because it is not possible to
            #        cleanly "resume" iterating an async generator after cancellation. The solution
            #        is to build the observer/multiplexer functionality into telemetry streams like
            #        we already do for event streams. Then we can just get an `asyncio.Queue` from
            #        the API.
            # Each reopen call is bounded by time_to_live so a hung provider can't deadlock us.
            # The constraint is already active from the timeout handler above, so we keep it
            # active across reopen attempts.
            while True:
                try:
                    stream = await asyncio.wait_for(
                        provider.monitor(BasicWeather), timeout=self.time_to_live
                    )
                    # Consume the first value, which is not necessarily fresh.
                    await asyncio.wait_for(anext(stream), timeout=self.time_to_live)
                    break
                except TimeoutError:
                    logger.warning(
                        f"weather provider '{self.provider}' unresponsive during reopen; "
                        f"keeping constraint active and retrying"
                    )
                    await asyncio.sleep(min(self.time_to_live, 5.0))


class SafetyConstraint(Constraint):
    """Constraint that monitors a BasicSafety provider and activates when conditions are unsafe."""
    kind: Literal["safety"] = "safety"
    provider: str
    time_to_live: float = 30.0
    hold_duration: float = 0.0

    async def _hold_and_clear(self, evaluator: ConstraintEvaluator):
        await asyncio.sleep(self.hold_duration)
        logger.info(f"Clearing safety constraint from {self.provider} (hold period expired)")
        evaluator.active.clear()
        await evaluator._notify(ConstraintStatus(
            constraint_kind=self.kind, active=False, reason="hold period expired", provider=self.provider,
        ))

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        logger.debug(f"monitoring safety constraint from {self.provider}")

        provider = kit.entity(self.provider)
        # Bounded initial open: fail-fast on a hung provider, propagating to the
        # TaskGroup so the agent restarts rather than wedging forever.
        stream = await asyncio.wait_for(
            provider.monitor(BasicSafety), timeout=self.time_to_live
        )
        hold_task: asyncio.Task | None = None
        from_data = False  # tracks whether active was set by a measurement vs. a timeout

        while True:
            try:
                async with asyncio.timeout(self.time_to_live) as timeout:
                    async for _, safety in stream:
                        if not safety.is_safe:
                            # Cancel any pending hold timer
                            if hold_task is not None and not hold_task.done():
                                hold_task.cancel()
                                hold_task = None
                                logger.info(
                                    f"Safety hold reset: provider {self.provider} reported unsafe again"
                                )
                            was_clear = not evaluator.active.is_set()
                            if was_clear:
                                logger.info(f"Setting safety constraint from {self.provider}")
                                await evaluator._notify(ConstraintStatus(
                                    constraint_kind=self.kind, active=True, reason="unsafe", provider=self.provider,
                                ))
                            evaluator.active.set()
                            # Only flip from_data on a real clear→active transition. A confirming
                            # sample on an already-set constraint (fail-closed init, stale-cache
                            # replay, repeated unsafe) must not poison the flag.
                            if was_clear:
                                from_data = True
                        else:
                            if evaluator.active.is_set():
                                if self.hold_duration > 0 and from_data:
                                    if hold_task is None or hold_task.done():
                                        logger.info(
                                            f"Safety cleared from {self.provider}, "
                                            f"holding constraint for {self.hold_duration}s"
                                        )
                                        hold_task = asyncio.create_task(self._hold_and_clear(evaluator))
                                else:
                                    logger.info(f"Clearing safety constraint from {self.provider}")
                                    await evaluator._notify(ConstraintStatus(
                                        constraint_kind=self.kind, active=False, reason="safe", provider=self.provider,
                                    ))
                                    evaluator.active.clear()
                                    from_data = False

                        evaluator.ready.set()

                        timeout.reschedule(asyncio.get_running_loop().time() + self.time_to_live)
            except TimeoutError:
                # The active state we're about to assert came from a timeout, not a measurement,
                # so the next safe sample shouldn't be held back by hold_duration.
                from_data = False
                if hold_task is not None and not hold_task.done():
                    hold_task.cancel()
                    hold_task = None
                reason = f"no data for {self.time_to_live}s"
                if not evaluator.active.is_set():
                    logger.info(f"Setting safety constraint from {self.provider} ({reason})")
                    await evaluator._notify(ConstraintStatus(
                        constraint_kind=self.kind, active=True, reason=reason, provider=self.provider,
                    ))
                evaluator.active.set()

            # Bounded reopen — a hung provider shouldn't deadlock us. Constraint stays
            # active across retries.
            while True:
                try:
                    stream = await asyncio.wait_for(
                        provider.monitor(BasicSafety), timeout=self.time_to_live
                    )
                    await asyncio.wait_for(anext(stream), timeout=self.time_to_live)
                    break
                except TimeoutError:
                    logger.warning(
                        f"safety provider '{self.provider}' unresponsive during reopen; "
                        f"keeping constraint active and retrying"
                    )
                    await asyncio.sleep(min(self.time_to_live, 5.0))


class GenericConstraint(Constraint):
    """Generic constraint driven by a Condition evaluated against any entity keyword."""

    kind: Literal["conditional"] = "conditional"
    entity: str
    keyword: str
    field: str | None = None
    condition: AnyCondition
    time_to_live: float = 30.0
    hold_duration: float = 0.0
    activate_on_timeout: bool = True
    """If True (default), activate the constraint when no data arrives within time_to_live.
    If False, the constraint stays inactive when data is absent — useful for optional sensors."""

    async def _hold_and_clear(self, evaluator: ConstraintEvaluator, label: str):
        await asyncio.sleep(self.hold_duration)
        logger.info(f"Clearing conditional constraint on {label} (hold period expired)")
        evaluator.active.clear()
        await evaluator._notify(ConstraintStatus(
            constraint_kind=self.kind, active=False, reason=f"{label} hold period expired", provider=self.entity,
        ))

    async def _apply(
        self,
        evaluator: ConstraintEvaluator,
        current: object,
        previous: object,
        was_active: bool,
        label: str,
        hold_task: asyncio.Task | None,
        from_data: bool,
    ) -> tuple[object, bool, asyncio.Task | None, bool]:
        """Evaluate the condition and update the evaluator.

        Returns (current, was_active, hold_task, from_data). ``from_data`` tracks whether
        the constraint's current active state was set by a real clear→active transition on
        live data. The hold_duration only applies when from_data is True — that way the
        fail-closed startup default and stale-cache replays don't hold the controller behind
        a hold_duration timer for data we never actually saw fall and rise.
        """
        _, is_active = self.condition.evaluate(current, previous, was_active)

        if is_active:
            reason = f"{label} = {current}"
            # Cancel any pending hold timer
            if hold_task is not None and not hold_task.done():
                hold_task.cancel()
                hold_task = None
                logger.info(
                    f"Conditional hold reset on {label}: condition retriggered ({reason})"
                )
            was_clear = not evaluator.active.is_set()
            if was_clear:
                logger.info(f"Setting conditional constraint on {reason}")
                await evaluator._notify(ConstraintStatus(
                    constraint_kind=self.kind, active=True, reason=reason, provider=self.entity,
                ))
            evaluator.active.set()
            # Only flip from_data on a real clear→active transition; see WeatherConstraint
            # for the same reasoning.
            if was_clear:
                from_data = True
        else:
            if evaluator.active.is_set():
                reason = f"{label} = {current}"
                if self.hold_duration > 0 and from_data:
                    if hold_task is None or hold_task.done():
                        logger.info(f"Conditional constraint cleared ({reason}), holding for {self.hold_duration}s")
                        hold_task = asyncio.create_task(self._hold_and_clear(evaluator, label))
                else:
                    logger.info(f"Clearing conditional constraint on {reason}")
                    await evaluator._notify(ConstraintStatus(
                        constraint_kind=self.kind, active=False, reason=reason, provider=self.entity,
                    ))
                    evaluator.active.clear()
                    from_data = False

        return current, is_active, hold_task, from_data

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        label = f"{self.entity}.{self.keyword}"
        if self.field:
            label += f".{self.field}"
        logger.debug(f"monitoring conditional constraint on {label}")

        _UNSET = object()
        previous: object = _UNSET
        was_active = False
        hold_task: asyncio.Task | None = None
        skip_replay = False
        # Tracks whether the current active state was set by a live clear→active transition.
        # The hold_duration only applies when this is True — protects against the fail-closed
        # init and stale-cache replays holding the controller behind a hold_duration timer.
        from_data = False

        if not self.activate_on_timeout:
            # Optional-sensor mode: absence of data is NOT a reason to constrain. Opt out
            # of the global fail-closed default (set in ConstraintEvaluator.__init__) so
            # the controller can operate while we're still waiting for the first sample.
            evaluator.active.clear()
            evaluator.ready.set()

        client = kit.entity(self.entity)
        # Bounded initial open: fail-fast on a hung backend so the TaskGroup can restart us
        # instead of wedging forever.
        consumer = await asyncio.wait_for(
            client._stream.consume(include_latest=True), timeout=self.time_to_live
        )

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
                            if skip_replay:
                                skip_replay = False
                                previous = current
                            else:
                                previous, was_active, hold_task, from_data = await self._apply(
                                    evaluator, current, previous, was_active, label,
                                    hold_task, from_data,
                                )
                                evaluator.ready.set()
                        else:
                            previous = current
                            # First message: we don't evaluate the condition (most conditions
                            # need a transition), but we *have* seen data, so the startup gate
                            # can proceed. Without this, a stream that only ever produces one
                            # value (e.g. a one-shot publish) would block startup forever.
                            evaluator.ready.set()

                        timeout.reschedule(
                            asyncio.get_running_loop().time() + self.time_to_live
                        )

            except TimeoutError:
                reason = f"{label} (no data for {self.time_to_live}s)"
                if self.activate_on_timeout:
                    if not evaluator.active.is_set():
                        logger.info(f"Setting conditional constraint on {reason}")
                        await evaluator._notify(ConstraintStatus(
                            constraint_kind=self.kind, active=True, reason=reason, provider=self.entity,
                        ))
                    evaluator.active.set()
                else:
                    if evaluator.active.is_set():
                        logger.info(f"Clearing conditional constraint on {reason}")
                        await evaluator._notify(ConstraintStatus(
                            constraint_kind=self.kind, active=False, reason=reason, provider=self.entity,
                        ))
                    evaluator.active.clear()
                    evaluator.ready.set()

                # The next consumer reopen with include_latest=True will replay
                # the cached pre-timeout value. That value is the same one we
                # already processed before the silence window, so re-evaluating
                # it would just undo whatever the timeout branch above just
                # decided (flapping). Skip it regardless of activate_on_timeout.
                skip_replay = True
                # The (re)activation above came from a timeout, not from a measurement,
                # so the next clear should NOT be held back by hold_duration.
                from_data = False

            # Bounded reopen — a hung backend shouldn't deadlock us. We retain
            # whatever active/clear state the timeout handler above just decided.
            while True:
                try:
                    consumer = await asyncio.wait_for(
                        client._stream.consume(include_latest=True),
                        timeout=self.time_to_live,
                    )
                    break
                except TimeoutError:
                    logger.warning(
                        f"entity '{self.entity}' stream reopen unresponsive; retrying "
                        f"(constraint {'active' if evaluator.active.is_set() else 'clear'})"
                    )
                    await asyncio.sleep(min(self.time_to_live, 5.0))
