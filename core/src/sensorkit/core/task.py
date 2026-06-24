from __future__ import annotations

import asyncio
import uuid
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, override

from pydantic import BaseModel, Field, PrivateAttr

from sensorkit.common.keyword import KeywordDict
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

    # The execution envelope is associated by the controller immediately before a task handler is
    # invoked. It is excluded from serialization and only meaningful server-side within a handler.
    _execution: TaskExecution | None = PrivateAttr(default=None)

    # Task model registry.
    registry: ClassVar[ModelRegistry[Task]] = ModelRegistry(discriminator="task_type")

    @classmethod
    def model_registry(cls):
        return cls.registry

    @property
    def execution(self) -> TaskExecution:
        """The `TaskExecution` envelope associated with this task.

        The controller associates the envelope just before invoking a task handler, so it is
        always available from within a handler.

        Returns:
            The associated execution envelope.

        Raises:
            RuntimeError: If accessed when no execution has been associated (e.g. outside a task
                handler).
        """
        if self._execution is None:
            raise RuntimeError("task has no execution context (accessed outside a task handler?)")

        return self._execution

    def associate_execution(self, execution: TaskExecution) -> None:
        """Associate an execution envelope with this task.

        The controller calls this immediately before invoking a task handler so the handler can
        reach the envelope via [`execution`][sensorkit.core.task.Task.execution].

        Args:
            execution: The execution envelope to associate with this task.

        Raises:
            RuntimeError: If an execution is already associated with this task.
        """
        if self._execution is not None:
            raise RuntimeError("task already has an associated execution")

        self._execution = execution

    def __eq__(self, other: object) -> bool:
        # The execution back-link is transient runtime state (excluded from serialization), so it
        # must not affect equality: two tasks with the same semantic content are equal whether or
        # not either is currently associated with an execution.
        if not isinstance(other, Task):
            return NotImplemented

        return (
            type(self) is type(other)
            and self.__dict__ == other.__dict__
            and self.__pydantic_extra__ == other.__pydantic_extra__
        )

    __hash__ = None

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

        A convenience for task factories: ``yield task.submit(expiry_time=...)`` reads more fluently
        than constructing a `TaskSubmission` by hand. Yielding a bare task is equivalent to
        ``task.submit()`` with no parameters.

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


class TaskExecution(BaseModel):
    """Execution envelope wrapping a `Task`.

    Carries server-minted identity (`task_id`, `controller_id`) and client-supplied execution
    parameters (`context`, `expiry_time`). This type is not user-extensible; the extensible
    semantic definition is the embedded `task`.
    """

    task: Task
    task_id: uuid.UUID
    controller_id: str
    context: KeywordDict | None = None
    expiry_time: datetime | None = None

    # Live result future, bound only in the context that owns the in-flight call (the client
    # tasking loop today). Transient runtime state: excluded from serialization and equality, so a
    # deserialized execution carries no result future and is not awaitable.
    _result: asyncio.Future[TaskExecutionResult] | None = PrivateAttr(default=None)

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

    def set_context(self, context: KeywordDict):
        """Merge keyword context into this execution.

        Args:
            context: Keyword context to merge into any context already present.
        """
        if self.context is None:
            self.context = context
        else:
            self.context.update(context)

    def get_context(self) -> KeywordDict:
        """Return the keyword context associated with this execution.

        Returns:
            The associated context, or an empty `KeywordDict` if none is set.
        """
        return self.context or KeywordDict()


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


class TaskContexts(BaseModel, extra="allow"):
    """Per-task-type keyword context bundles passed to a Program when starting tasking.

    The ``all`` field provides keywords that apply to every task type.  Additional fields
    (``init``, ``standby``, ``shutdown``, or any custom task type name) provide
    type-specific overrides.  Extra fields (via ``extra="allow"``) support custom task types.
    """

    all: KeywordDict = Field(default_factory=KeywordDict)
    init: KeywordDict = Field(default_factory=KeywordDict)
    standby: KeywordDict = Field(default_factory=KeywordDict)
    shutdown: KeywordDict = Field(default_factory=KeywordDict)
    __pydantic_extra__: dict[str, KeywordDict] = Field(init=False)

    def propagate(self, into: TaskContexts):
        """Merge this context into ``into``, without overwriting keys already present in ``into``."""
        for field, context in self:
            if context is self.all:
                continue

            other_context: KeywordDict = getattr(into, field, None)

            if not other_context:
                other_context = KeywordDict()
                setattr(into, field, other_context)

            for kw, value in context.items():
                if kw not in into.all and kw not in other_context:
                    other_context[kw] = value

        all_context = self.all.copy()
        all_context.update(into.all)
        into.all = all_context

    def get(self, task_type: str) -> KeywordDict:
        """Return the effective task context for a given task type.

        Returns:
            A new KeywordDict containing the "all" context combined with the type-specific context.
        """
        context = self.all.copy()

        if task_context := getattr(self, task_type, None):
            context.update(task_context)

        return context
