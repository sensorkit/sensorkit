from __future__ import annotations

import asyncio
import contextlib
import json
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, override

from loguru import logger
from pydantic import model_validator

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

    @classmethod
    def model_registry(cls):
        return _constraint_registry

    # FIXME: Use Context or similar instead of **kwargs
    @abstractmethod
    async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs) -> None:
        """Long-running coroutine that monitors the constraint and updates the evaluator."""

    @model_validator(mode="before")
    @classmethod
    def _hold_compat(cls, data: Any) -> Any:
        if isinstance(data, dict) and "hold_duration" in data:
            data["hold"] = data.pop("hold_duration")
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
    active: bool = True
    reason: str = ""
    details: ConstraintDetails | None = None
    ready: bool = False


class ConstraintEvaluator:
    """State interface for one check_task run. Created fresh by ConstraintManager on each restart."""

    _QUEUE_MAXSIZE = 10

    def __init__(self, constraint: Constraint, *, timeout: asyncio.Timeout):
        self.constraint = constraint
        self._timeout = timeout
        self._active = asyncio.Event()
        self._ready = asyncio.Event()
        self._queue: asyncio.Queue[ConstraintState] = asyncio.Queue(maxsize=self._QUEUE_MAXSIZE)
        self._hold_task: asyncio.Task | None = None

        # Constraint starts active. The implementation must call `clear()` prior to `ready()` if
        # the constraint is initially inactive.
        self._active.set()

    @property
    def is_active(self) -> bool:
        return self._active.is_set()

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def next_update(self) -> ConstraintState:
        state = await self._queue.get()

        while not self._queue.empty():
            state = self._queue.get_nowait()

        if state.active != self._active.is_set():
            raise RuntimeError(f"Constraint {self.constraint.kind} state mismatch")

        return state

    def ready(self):
        """Mark constraint ready."""
        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)

        if not self._ready.is_set():
            self._queue.put_nowait(
                ConstraintState(
                    kind=self.constraint.kind,
                    active=self._active.is_set(),
                    reason="Constraint ready",
                    details=None,
                    ready=True,
                )
            )
            self._ready.set()

    def constrain(self, reason: str, *, details: ConstraintDetails | None = None) -> bool:
        """Set constraint active.

        Returns:
            bool: True if state changed (clear → active)

        Raises:
            QueueFull: if the status queue is full
        """
        changed = not self._active.is_set()

        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)
        self._cancel_hold()
        self._active.set()
        self._queue.put_nowait(
            ConstraintState(
                kind=self.constraint.kind,
                active=True,
                reason=reason,
                details=details,
                ready=self._ready.is_set(),
            )
        )

        return changed

    def clear(self, reason: str = "", *, details: ConstraintDetails | None = None) -> bool:
        """Set constraint inactive. If hold_duration > 0, defers the clear until the hold expires.

        Returns:
            bool: True if state changed immediately (active → clear). False if deferred via hold
            or already inactive.

        Raises:
            QueueFull: if the status queue is full
        """
        self._timeout.reschedule(asyncio.get_event_loop().time() + self.constraint.ttl)

        if self.constraint.hold > 0 and self._active.is_set() and self._ready.is_set():
            self._begin_hold(reason, details)
            return False

        return self._clear(reason, details)

    async def cancel(self):
        """Cancel any pending hold."""
        if t := self._cancel_hold():
            with contextlib.suppress(asyncio.CancelledError):
                await t

    def _clear(self, reason: str = "", details: ConstraintDetails | None = None) -> bool:
        changed = self._active.is_set()
        self._queue.put_nowait(
            ConstraintState(
                kind=self.constraint.kind,
                active=False,
                reason=reason,
                details=details,
                ready=self._ready.is_set(),
            )
        )
        self._active.clear()
        return changed

    async def _hold_and_clear(self, reason: str, details: ConstraintDetails | None):
        logger.debug(f"holding {self.constraint.kind} constraint for {self.constraint.hold}s")
        await asyncio.sleep(self.constraint.hold)
        logger.debug(f"clearing {self.constraint.kind} constraint after hold")
        self._clear(reason, details)

    def _begin_hold(self, reason: str, details: ConstraintDetails | None):
        if self._hold_task is None or self._hold_task.done():
            self._hold_task = asyncio.create_task(self._hold_and_clear(reason, details))

    def _cancel_hold(self) -> asyncio.Task | None:
        if self._hold_task is not None and not self._hold_task.done():
            self._hold_task.cancel()

        hold_task = self._hold_task
        self._hold_task = None
        return hold_task


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

    def _set_state(self, idx: int, state: ConstraintState):
        self._entries[idx] = state

        if state.active:
            if idx not in self._constrained_set:
                logger.info(f"Constraint {state.kind} is active (reason: {state.reason or "none"})")

            self._constrained_set.add(idx)
        else:
            if idx in self._constrained_set:
                logger.info(f"Constraint {state.kind} is inactive")

            self._constrained_set.discard(idx)

        if state.ready:
            if idx not in self._ready_set:
                logger.debug(f"constraint {state.kind} is ready")

            self._ready_set.add(idx)
        else:
            if idx in self._ready_set:
                logger.debug(f"constraint {state.kind} is not ready")

            self._ready_set.discard(idx)

        if len(self._ready_set) == len(self._entries):
            self._ready_event.set()
        else:
            self._ready_event.clear()

    async def _constraint_supervisor(self, idx: int, constraint: Constraint, **kwargs):
        self._set_state(
            idx,
            ConstraintState(
                kind=constraint.kind,
                active=True,
                reason="awaiting initial state",
                ready=False,
            ),
        )

        while True:
            logger.debug(f"starting {constraint.kind} constraint check task")

            try:
                initial_timeout = self.CONSTRAINT_RESTART_GRACE + constraint.ttl

                async with asyncio.timeout(initial_timeout) as timeout:
                    await self._constraint_task(idx, constraint, timeout, **kwargs)
            except Exception:
                logger.exception(f"error in {constraint.kind} constraint evaluation")
            finally:
                # Reset to fail-closed before restart.
                self._set_state(
                    idx,
                    ConstraintState(
                        kind=constraint.kind,
                        active=True,
                        reason="evaluation error",
                        ready=False,
                    ),
                )

            logger.debug(f"restarting {constraint.kind} constraint after delay")
            await asyncio.sleep(self.CONSTRAINT_RESTART_DELAY)

    async def _constraint_task(
        self,
        idx: int,
        constraint: Constraint,
        timeout: asyncio.Timeout,
        **kwargs,
    ):
        # Create an evaluator and start the check task.
        evaluator = ConstraintEvaluator(constraint, timeout=timeout)
        check_task = asyncio.create_task(constraint.check_task(evaluator, **kwargs))
        update_task: asyncio.Task | None = None

        try:
            # Wait for the check task to signal that it is ready.
            await evaluator.wait_ready()

            # Read events emitted by the check task.
            while True:
                update_task: asyncio.Task = asyncio.ensure_future(evaluator.next_update())
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
            aws = [evaluator.cancel(), check_task]
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
