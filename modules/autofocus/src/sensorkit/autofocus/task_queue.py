from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from loguru import logger

import sensorkit.api as sk
from sensorkit.std.collect import StandardCollectTask


@dataclass
class QueuedTask:
    """A queued collect task paired with a client-side ID and submission context.

    Attributes:
        task: The queued collect task (one V-curve step).
        id: Client-side identifier for this queued task.
        context: Submission context attached at dispatch, preserved by the controller onto the
            frames so the analyzer can regroup the sweep.
    """

    task: StandardCollectTask
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    context: sk.KeywordDict | None = None


class TaskQueue:
    """A queue of collect tasks integrated with the autofocus program."""

    def __init__(self, program_binding):
        self.tasks: list[QueuedTask] = []
        self.program_binding = program_binding
        self._lock = asyncio.Lock()

    async def push_task(
        self, task: StandardCollectTask, context: sk.KeywordDict | None = None
    ) -> QueuedTask:
        """Add a task to the queue and update offers."""
        async with self._lock:
            queued = QueuedTask(task=task, context=context)
            self.tasks.append(queued)
            # Sort by end_time to maintain priority.
            self.tasks.sort(key=lambda q: q.task.end_time)
            logger.debug(f"task {queued.id} queued: end_time={task.end_time}")
            await self._update_offers()
            return queued

    async def pop_task(self) -> QueuedTask | None:
        """Remove and return the next available task."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks.
            while self.tasks and self.tasks[0].task.end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")

            if self.tasks:
                queued = self.tasks.pop(0)
                await self._update_offers()
                return queued

            return None

    async def peek_task(self) -> QueuedTask | None:
        """View the next task without removing it."""
        async with self._lock:
            now = datetime.now(UTC)

            while self.tasks and self.tasks[0].task.end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")

            return self.tasks[0] if self.tasks else None

    async def _update_offers(self):
        """Update the offer window based on current tasks."""
        self.program_binding.clear_offers()

        if self.tasks:
            now = datetime.now(UTC)
            for queued in self.tasks:
                if queued.task.end_time <= now:
                    continue
                self.program_binding.add_offer(
                    start=now,
                    end=queued.task.end_time,
                    obj=queued.id,
                )

        await self.program_binding.publish_offers()

    def __len__(self):
        return len(self.tasks)
