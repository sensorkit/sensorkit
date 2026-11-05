# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import copy
import uuid
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, override

from pydantic import BaseModel, Field, PrivateAttr, SerializeAsAny, model_validator

from sensorkit.common.keyword import KeywordDict, declare_keyword, validated_items
from sensorkit.common.model import ModelRegistry, RegistryBaseModel

if TYPE_CHECKING:
    from sensorkit.core.controller import InternalControllerState, TaskExecutionResult


class Task(RegistryBaseModel):
    """Base task definition.

    This is the user-extensible part of the task system: subclasses add domain-specific fields and
    register automatically via the `task_type` discriminator. Identity and execution-envelope data
    (`task_id`, `controller_id`, `context`, `expiry_time`) live separately on `TaskExecution`,
    which the controller mints when a task is submitted for execution.
    """

    task_type: Literal[None] = None

    # Task model registry.
    registry: ClassVar[ModelRegistry[Task]] = ModelRegistry(discriminator="task_type")

    @classmethod
    def model_registry(cls):
        return cls.registry

    def target_state(self) -> InternalControllerState | None:
        """Return the state-transition target if this is a lifecycle task.

        Returns:
            The target controller state, or `None` for non-lifecycle tasks.
        """
        # FIXME: This mapping should be defined by other means in controller.py.
        return None

    def default_expiry(self) -> datetime | timedelta:
        """Return the default execution expiry for this task.

        Used by the tasking loop when the task factory supplies no explicit `expiry_time`. A
        `timedelta` is interpreted relative to dispatch time; a `datetime` is absolute. Subclasses
        may override to express a domain-specific deadline.

        Returns:
            The default expiry as an absolute time or a duration from dispatch.
        """
        return timedelta(seconds=300)

    def submit(
        self,
        *,
        context: KeywordDict | None = None,
        expiry_time: datetime | None = None,
    ) -> TaskSubmission:
        """Bundle this task with execution parameters into a `TaskSubmission`.

        A convenience for task factories: `yield task.submit(expiry_time=...)` reads more fluently
        than constructing a `TaskSubmission` by hand. Yielding a bare task is equivalent to
        `task.submit()` with no parameters.

        Args:
            context: Optional keyword context to attach to the execution.
            expiry_time: Optional time after which the execution should be considered expired.

        Returns:
            A `TaskSubmission` wrapping this task and the given parameters.
        """
        return TaskSubmission(task=self, context=context, expiry_time=expiry_time)


class InitTask(Task):
    """Init Task"""

    task_type: Literal["init"] = "init"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState

        return InternalControllerState.OPERATE


class StandbyTask(Task):
    """Standby Task"""

    task_type: Literal["standby"] = "standby"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState

        return InternalControllerState.STANDBY


class ShutdownTask(Task):
    """Shutdown Task"""

    task_type: Literal["shutdown"] = "shutdown"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState

        return InternalControllerState.SHUTDOWN


class CalibrateTask(Task):
    """Calibrate Task"""

    task_type: Literal["calibrate"] = "calibrate"


class RecoverTask(Task):
    """Recover Task"""

    task_type: Literal["recover"] = "recover"


class CollectTask(Task):
    """Collect Task"""

    task_type: Literal["collect"] = "collect"


@declare_keyword
class TaskInfo(BaseModel):
    """Keyword describing a task that has been executed on a controller."""
    task: Task
    task_id: uuid.UUID
    controller_id: str

    def get_fits_cards(self):
        yield "SKTASK", (self.task.task_type, "SensorKit task type")
        yield "SKTASKID", (str(self.task_id), "SensorKit task ID")
        yield "SKCTRL", (self.controller_id, "SensorKit controller name")


class TaskExecution[T: Task](BaseModel):
    """Execution envelope wrapping a `Task`.

    Carries server-minted identity (`task_id`, `controller_id`), submitted context,
    working context, and expiry. The embedded `task` defines the extensible task data.

    A handler annotated with `TaskExecution[T]` receives execution metadata and a task
    of type `T`. Controller-produced envelopes use the unparameterized model.
    Serialization preserves all fields of the concrete task subclass.
    Client executions returned by `ControllerClient.start_task` can be awaited for
    their result.
    """

    task: SerializeAsAny[T]
    task_id: uuid.UUID
    controller_id: str
    context: KeywordDict = Field(default_factory=KeywordDict)
    """Working context, including task identity and values added during execution."""
    submitted_context: KeywordDict | None = None
    """Submitted context, captured from `context` when omitted or None.

    Explicit mappings are retained without copying.
    """
    expiry_time: datetime | None = None

    # Live result future, bound only in the context that owns the in-flight call (the client
    # tasking loop today). Transient runtime state: excluded from serialization and equality, so a
    # deserialized execution carries no result future and is not awaitable.
    _result: asyncio.Future[TaskExecutionResult] | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _capture_submitted_context(self) -> Self:
        if self.submitted_context is None:
            self.submitted_context = copy.deepcopy(self.context)

        return self

    def bind_result(self, result: asyncio.Future[TaskExecutionResult]) -> None:
        """Bind the live result future for this execution.

        The dispatching context binds the in-flight call's future so that holders of this execution
        can await its completion or inspect the result.

        Args:
            result: The future resolving to this execution's final result.

        Raises:
            RuntimeError: If a result future is already bound.
        """
        if self._result is not None:
            raise RuntimeError("execution already has a bound result")

        self._result = result

    def _require_result(self) -> asyncio.Future[TaskExecutionResult]:
        if self._result is None:
            raise RuntimeError("execution is not awaitable in this context")

        return self._result

    def __await__(self) -> Generator[Any, None, TaskExecutionResult]:
        """Await the final result of this execution."""
        return self._require_result().__await__()

    def done(self) -> bool:
        """Return True if the execution has completed (result or error)."""
        return self._require_result().done()

    def result(self) -> TaskExecutionResult:
        """Return the final result, raising if the execution has not yet completed."""
        return self._require_result().result()


class TaskSubmission(BaseModel):
    """A Task bundled with client-supplied execution parameters.

    A task factory may return this in place of a bare `Task` to attach per-instance execution
    parameters (`context`, `expiry_time`) that the controller records on the minted
    `TaskExecution`. Returning a bare `Task` is equivalent to a `TaskSubmission` with no parameters;
    `Task.submit` is the convenient way to build one.
    """

    task: Task
    context: KeywordDict | None = None
    expiry_time: datetime | None = None


type RawKeywords = dict[str, Any]
"""A sparse, unvalidated mapping of keyword key to raw payload, as declared in config."""


def _deep_merge(base: Any, override: Any) -> Any:
    """Recursively merge `override` onto `base`, with `override` winning on conflict.

    Dicts are merged key-by-key (so a keyword payload's fields combine across layers); any
    other value, including a list, is replaced wholesale. A `None` override keeps `base`,
    which lets a layer contribute a key another layer omits.
    """
    if override is None:
        return base

    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)

        for key, value in override.items():
            merged[key] = _deep_merge(base.get(key), value)

        return merged

    return override


class TaskContextOverlay(BaseModel, extra="allow"):
    """Raw, sparse, layered task context as declared in config, prior to validation.

    The `all` field provides keywords that apply to every task type. Additional fields
    (`init`, `standby`, `shutdown`, or any custom task type name) provide type-specific
    overrides. Extra fields (via `extra="allow"`) support custom task types.

    Values are kept as raw payloads and merged field-wise across layers; validation into
    keyword instances is deferred to `build`, so overlapping keywords combine rather than
    clobber and every payload is validated exactly once.
    """

    all: RawKeywords = Field(default_factory=dict)
    init: RawKeywords = Field(default_factory=dict)
    standby: RawKeywords = Field(default_factory=dict)
    shutdown: RawKeywords = Field(default_factory=dict)
    __pydantic_extra__: dict[str, RawKeywords] = Field(init=False)

    def _task_type_items(self):
        for field, keywords in self:
            if field != "all":
                yield field, keywords

    def propagate(self, into: TaskContextOverlay):
        """Merge this (parent) overlay into `into` (child), with the child taking precedence.

        A keyword present in both layers is merged field-wise, so a child override fills in
        rather than replaces the parent's fields. A parent's type-specific keyword is skipped
        when the child already carries that keyword in its `all`, since the child's `all`
        already supplies it for every task type.
        """
        for task_type, keywords in self._task_type_items():
            target = getattr(into, task_type, None)

            if target is None:
                target = {}
                setattr(into, task_type, target)

            for key, payload in keywords.items():
                if key in into.all:
                    continue

                target[key] = _deep_merge(payload, target.get(key))

        into.all = _deep_merge(self.all, into.all)

    def build(self) -> TaskContextMap:
        """Flatten the `all` layer into each task type and validate every payload as a keyword."""
        return TaskContextMap(
            defaults=KeywordDict(validated_items(self.all)),
            by_type={
                task_type: KeywordDict(validated_items(_deep_merge(self.all, keywords)))
                for task_type, keywords in self._task_type_items()
            },
        )


class TaskContextMap(BaseModel):
    """Flattened, validated task contexts keyed by task type."""

    defaults: KeywordDict = Field(default_factory=KeywordDict)
    by_type: dict[str, KeywordDict] = Field(default_factory=dict)

    def get(self, task_type: str) -> KeywordDict:
        """Return the effective context for `task_type`, falling back to `defaults`."""
        return self.by_type.get(task_type, self.defaults).copy()
