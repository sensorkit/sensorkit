from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Literal, override

from pydantic import BaseModel, Field

from sensorkit.common.keyword import KeywordDict
from sensorkit.common.model import ModelRegistry, RegistryBaseModel

if TYPE_CHECKING:
    from sensorkit.core.controller import InternalControllerState


class ControllerTask(RegistryBaseModel):
    """Base Controller Task"""

    task_type: Literal[None] = None
    task_id: uuid.UUID
    controller_id: str
    context: KeywordDict | None = None
    end_time: datetime | None = None

    registry: ClassVar[ModelRegistry[ControllerTask]] = ModelRegistry(discriminator="task_type")

    @classmethod
    def model_registry(cls):
        return cls.registry

    def target_state(self) -> InternalControllerState | None:
        """Return the state transition target if this is a lifecycle task."""
        # FIXME: This mapping should be defined by other means in controller.py.
        return None

    def set_context(self, context: KeywordDict):
        """Associate context with the task."""
        if self.context is None:
            self.context = context
        else:
            self.context.update(context)

    def get_context(self):
        """Return the context associated with this task."""
        return self.context or KeywordDict()


class InitTask(ControllerTask):
    """Init Task"""
    task_type: Literal["init"] = "init"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState
        return InternalControllerState.OPERATE


class StandbyTask(ControllerTask):
    """Standby Task"""
    task_type: Literal["standby"] = "standby"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState
        return InternalControllerState.STANDBY


class ShutdownTask(ControllerTask):
    """Shutdown Task"""
    task_type: Literal["shutdown"] = "shutdown"

    @override
    def target_state(self):
        from sensorkit.core.controller import InternalControllerState
        return InternalControllerState.SHUTDOWN


class CalibrateTask(ControllerTask):
    """Calibrate Task"""
    task_type: Literal["calibrate"] = "calibrate"


class RecoverTask(ControllerTask):
    """Recover Task"""
    task_type: Literal["recover"] = "recover"


class CollectTask(ControllerTask):
    """Collect Task"""
    task_type: Literal["collect"] = "collect"


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
