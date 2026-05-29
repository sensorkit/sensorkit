from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, override

from loguru import logger
from pydantic import model_validator

from sensorkit.common.aio import cleanup_future
from sensorkit.common.condition import AnyCondition, resolve_field
from sensorkit.common.model import ModelRegistry, RegistryBaseModel
from sensorkit.core.client import SensorKit

_constraint_registry = ModelRegistry(discriminator="kind")
_details_registry = ModelRegistry(discriminator="kind")


class Constraint(RegistryBaseModel, ABC):
    """Abstract operating constraint that runs a background monitoring task."""

    kind: str
    ttl: float = 30.0
    hold: float = 0.0
    optional: bool = False

    @classmethod
    def model_registry(cls):
        return _constraint_registry

    # FIXME: Use Context or similar instead of **kwargs
    @abstractmethod
    async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs) -> None:
        """Long-running coroutine that monitors the constraint and updates the evaluator."""

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "hold_duration" in data:
                data["hold"] = data.pop("hold_duration")
            if "activate_on_timeout" in data:
                data["optional"] = not data.pop("activate_on_timeout")

        return data


class ConstraintDetails(RegistryBaseModel):
    """Base for structured constraint state details. Subclasses auto-register."""

    kind: str

    @classmethod
    def model_registry(cls):
        return _details_registry


@dataclass
class ConstraintState:
    """Per-constraint state."""

    kind: str
    state: Literal["not_ready", "clear", "holding", "active"] = "not_ready"
    reason: str = ""
    details: ConstraintDetails | None = None


class ConstraintEvaluator:
    """State interface for one check_task run. Created fresh by ConstraintManager on each restart."""

    _QUEUE_MAXSIZE = 10

    def __init__(self, constraint: Constraint, *, timeout: asyncio.Timeout):
        self.constraint = constraint
        self._timeout = timeout
        self._ready = asyncio.Event()
        self._hold_task: asyncio.Task | None = None

        if self.constraint.optional:
            self.intended_state = ConstraintState(
                kind=constraint.kind, state="clear", reason="optional constraint"
            )
            self._ready.set()
        else:
            self.intended_state = ConstraintState(
                kind=constraint.kind, state="not_ready", reason="awaiting data"
            )

        self.actual_state = self.intended_state
        self._state_updated = asyncio.Event()

    @property
    def is_active(self) -> bool:
        return self.actual_state.state != "clear"

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set()

    async def wait_ready(self):
        await self._ready.wait()

    async def next_update(self) -> ConstraintState:
        await self._state_updated.wait()
        state = self.actual_state
        self._state_updated.clear()
        return state

    def ready(self):
        """Mark constraint ready."""
        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)

        if not self._ready.is_set():
            self._ready.set()
            self._update_state()

    def constrain(self, reason: str, *, details: ConstraintDetails | None = None):
        """Set constraint active.

        Returns:
            bool: True if state changed (clear → active)

        Raises:
            QueueFull: if the status queue is full
        """
        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)
        self._cancel_hold()
        self.intended_state = ConstraintState(
            kind=self.constraint.kind,
            state="active",
            reason=reason,
            details=details,
        )
        self._update_state()

    def clear(self, reason: str = "", *, details: ConstraintDetails | None = None):
        """Set constraint inactive. If hold_duration > 0, defers the clear until the hold expires.

        Raises:
            QueueFull: if the status queue is full
        """
        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)

        if self.constraint.hold > 0 and self.intended_state.state == "active":
            self._begin_hold()

        self.intended_state = ConstraintState(
            kind=self.constraint.kind,
            state="clear",
            reason=reason,
            details=details,
        )
        self._update_state()

    async def cleanup(self):
        """Cancel any pending hold task."""
        self._cancel_hold()

    def _update_state(self):
        if not self._ready.is_set():
            state = ConstraintState(kind=self.constraint.kind, state="not_ready")
        elif self._hold_task is not None and not self._hold_task.done():
            state = ConstraintState(kind=self.constraint.kind, state="holding")
        else:
            state = self.intended_state

        self.actual_state = state
        self._state_updated.set()

    async def _do_hold(self, wait_secs: float):
        logger.debug(f"holding {self.constraint.kind} constraint for {wait_secs}s")
        await asyncio.sleep(wait_secs)

        # It's safe to clear `_hold_task` early here since there are no further awaits.
        self._hold_task = None
        self._update_state()

    def _begin_hold(self):
        if self._hold_task is None or self._hold_task.done():
            self._hold_task = asyncio.create_task(self._do_hold(self.constraint.hold))
            self._hold_task.add_done_callback(cleanup_future)

    def _cancel_hold(self):
        if self._hold_task is not None and not self._hold_task.done():
            self._hold_task.cancel()

        self._hold_task = None


class ConstraintManager:
    """Manages the lifecycle of all constraints."""

    CONSTRAINT_RESTART_GRACE = 5.0
    CONSTRAINT_RESTART_DELAY = 5.0

    def __init__(self, constraints: list[Constraint]):
        self._constraints = constraints
        self._entries: list[ConstraintState] = [ConstraintState(kind=c.kind) for c in constraints]
        self._constrained_set: set[int] = set()
        self._ready_set: set[int] = set()
        self._ready_event = asyncio.Event()

        if not constraints:
            self._ready_event.set()

    @property
    def entries(self) -> Sequence[ConstraintState]:
        """Current constraint state in config order."""
        return self._entries

    def is_constrained(self) -> bool:
        return len(self._constrained_set) > 0

    async def start(
        self,
        *,
        task_group: asyncio.TaskGroup,
        ready_timeout: float | None = None,
        **kwargs,
    ) -> bool:
        """Start all constraint monitoring tasks.

        Creates supervisor tasks for each constraint that will monitor and update
        their states continuously. Waits for all constraints to become ready or
        until the ready_timeout expires.

        Args:
            task_group: The asyncio.TaskGroup to create constraint supervisor tasks in.
            ready_timeout: Maximum time in seconds to wait for all constraints to become ready.
                          If None, waits indefinitely.
            **kwargs: Additional keyword arguments passed to each constraint's check_task method.

        Returns:
            bool: True if all constraints became ready within the timeout, False if one or more
                  constraints remain unready.
        """
        logger.debug(f"ConstraintManager starting with {len(self._constraints)} constraints")

        # Start a supervisor task to manage each constraint's evaluation loop.
        for i, constraint in enumerate(self._constraints):
            task_group.create_task(self._constraint_supervisor(i, constraint, **kwargs))

        try:
            async with asyncio.timeout(ready_timeout):
                await self._ready_event.wait()
        except asyncio.TimeoutError:
            return False

        return True

    def _set_state(self, idx: int, current: ConstraintState):
        prev = self._entries[idx]
        self._entries[idx] = current

        if current.state != prev.state:
            state_str = current.state.replace("_", " ")
            reason = f"(reason: {current.reason})" if current.reason else ""
            logger.info(f"{current.kind.capitalize()} constraint is {state_str} {reason}")

        if current.state == "not_ready":
            self._ready_set.discard(idx)
            self._constrained_set.add(idx)
        else:
            self._ready_set.add(idx)

            if current.state == "clear":
                self._constrained_set.discard(idx)
            else:
                self._constrained_set.add(idx)

        if len(self._ready_set) == len(self._entries):
            self._ready_event.set()
        else:
            self._ready_event.clear()

    async def _constraint_supervisor(self, idx: int, constraint: Constraint, **kwargs):
        restarting = False

        while True:
            try:
                initial_timeout = (
                    None
                    if constraint.ttl is None
                    else self.CONSTRAINT_RESTART_GRACE + constraint.ttl
                )

                async with asyncio.timeout(initial_timeout) as timeout:
                    # Create a constraint evaluator and set the initial state.
                    evaluator = ConstraintEvaluator(constraint, timeout=timeout)
                    self._set_state(idx, evaluator.actual_state)

                    # Sleep if this was a restart.
                    if restarting:
                        timeout.reschedule(
                            None
                            if (when := timeout.when()) is None
                            else when + self.CONSTRAINT_RESTART_DELAY
                        )
                        await asyncio.sleep(self.CONSTRAINT_RESTART_DELAY)

                    # Run the constraint and process updates.
                    await self._constraint_task(idx, constraint, evaluator, **kwargs)
            except asyncio.CancelledError:
                # Propagate only direct cancellation.
                if asyncio.current_task().cancelling():
                    raise

                logger.error(f"{constraint.kind.capitalize()} constraint cancelled unexpectedly")
            except asyncio.TimeoutError:
                # Only log timeouts for non-optional constraints.
                if not constraint.optional:
                    logger.error(f"{constraint.kind.capitalize()} constraint timed out")
            except Exception:
                logger.exception(f"{constraint.kind.capitalize()} constraint error")

            restarting = True

    async def _constraint_task(
        self,
        idx: int,
        constraint: Constraint,
        evaluator: ConstraintEvaluator,
        **kwargs,
    ):
        if not constraint.optional:
            logger.debug(f"starting {constraint.kind} constraint check")

        # Start the constraint check task.
        check_task = asyncio.create_task(constraint.check_task(evaluator, **kwargs))
        update_task: asyncio.Task | None = None

        try:
            # Wait for the check task to signal that it is ready.
            await evaluator.wait_ready()

            # Read events emitted by the check task.
            while True:
                update_task: asyncio.Task = asyncio.create_task(evaluator.next_update())
                done, _ = await asyncio.wait(
                    {update_task, check_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if update_task in done:
                    state = update_task.result()
                    self._set_state(idx, state)

                if check_task in done:
                    # Re-raise check task exception, if any.
                    check_task.result()
                    break
        finally:
            aws = [evaluator.cleanup(), check_task]
            check_task.cancel()

            if update_task:
                update_task.cancel()
                aws.append(update_task)

            await asyncio.gather(*aws, return_exceptions=True)


class GenericConstraint(Constraint):
    """Generic constraint driven by a Condition evaluated against any entity keyword."""

    kind: Literal["conditional"] = "conditional"
    entity: str
    keyword: str
    field: str | None = None
    condition: AnyCondition
    time_to_live: float = 30.0
    activate_on_timeout: bool = True
    """If True (default), activate the constraint when no data arrives within time_to_live.
    If False, the constraint stays inactive when data is absent — useful for optional sensors."""

    def _apply(
        self,
        evaluator: ConstraintEvaluator,
        current: object,
        previous: object,
        was_active: bool,
        label: str,
    ) -> tuple[object, bool]:
        """Evaluate the condition and update the evaluator."""
        _, is_active = self.condition.evaluate(current, previous, was_active)

        if is_active:
            reason = f"{label} = {current}"
            changed = evaluator.constrain(reason)

            if changed:
                logger.info(f"Setting conditional constraint on {reason}")
        else:
            if evaluator.is_active:
                reason = f"{label} = {current}"
                logger.info(f"Clearing conditional constraint on {reason}")
                evaluator.clear(reason)

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
        skip_replay = False

        if not self.activate_on_timeout:
            # Optional-sensor mode: absence of data is NOT a reason to constrain. Opt out
            # of the global fail-closed default (set in ConstraintEvaluator.__init__) so
            # the controller can operate while we're still waiting for the first sample.
            evaluator.clear("optional sensor: no data yet")
            evaluator.ready()

        client = kit.entity(self.entity)
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
                                previous, was_active = self._apply(
                                    evaluator,
                                    current,
                                    previous,
                                    was_active,
                                    label,
                                )
                                evaluator.ready()
                        else:
                            # First message: the condition can't be evaluated yet (most
                            # conditions need a transition). Signal the current fail-closed
                            # state so the manager knows we're alive, then mark ready.
                            evaluator.constrain("startup: awaiting first transition")
                            previous = current
                            evaluator.ready()

                        timeout.reschedule(
                            asyncio.get_running_loop().time() + self.time_to_live
                        )

            except TimeoutError:
                reason = f"{label} (no data for {self.time_to_live}s)"
                if self.activate_on_timeout:
                    changed = evaluator.constrain(reason)
                    if changed:
                        logger.info(f"Setting conditional constraint on {reason}")
                else:
                    if evaluator.is_active:
                        logger.info(f"Clearing conditional constraint on {reason}")
                        evaluator.clear(reason)
                evaluator.ready()

                # The next consumer reopen with include_latest=True will replay the cached
                # pre-timeout value. That value was already processed before the silence
                # window, so re-evaluating it would just undo whatever the timeout branch
                # above just decided (flapping). Skip it regardless of activate_on_timeout.
                skip_replay = True

            consumer = await asyncio.wait_for(
                client._stream.consume(include_latest=True),
                timeout=self.time_to_live,
            )
