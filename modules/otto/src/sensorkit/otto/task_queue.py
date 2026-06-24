import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from loguru import logger

from sensorkit.std.collect import StandardCollectTask


@dataclass
class QueuedTask:
    """A queued collect task paired with a client-side id for queue bookkeeping.

    The controller mints the execution ``task_id`` only at dispatch, so the queue keeps its own
    stable id for offer tracking, removal, and operator-facing references while a task waits.

    Attributes:
        task: The queued collect task.
        id: Otto's client-side identifier for this queued task.
    """

    task: StandardCollectTask
    id: uuid.UUID = field(default_factory=uuid.uuid4)


class TaskQueue:
    """A queue of tasks to be executed, integrated with Otto."""

    def __init__(self, program_binding):
        self.tasks: list[QueuedTask] = []
        self.program_binding = program_binding
        self._lock = asyncio.Lock()

    async def push_task(self, task: StandardCollectTask) -> QueuedTask:
        """Add a task to the queue and update offers.

        Args:
            task: The collect task to enqueue.

        Returns:
            The queued task, including its assigned client-side id.
        """
        async with self._lock:
            queued = QueuedTask(task=task)
            self.tasks.append(queued)
            # Sort tasks by end_time to maintain priority
            self.tasks.sort(key=lambda q: q.task.end_time)
            logger.debug(f"task {queued.id} queued: end_time={task.end_time}")
            await self._update_offers()
            return queued

    async def pop_task(self) -> QueuedTask | None:
        """Remove and return the next available task."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].task.end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")

            # Return next valid task
            if self.tasks:
                queued = self.tasks.pop(0)
                await self._update_offers()
                return queued

            return None

    async def peek_task(self) -> QueuedTask | None:
        """View the next task without removing it."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].task.end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")

            return self.tasks[0] if self.tasks else None

    async def flush_expired(self) -> int:
        """Remove all expired tasks from the queue."""
        async with self._lock:
            now = datetime.now(UTC)
            original_len = len(self.tasks)
            self.tasks = [q for q in self.tasks if q.task.end_time and q.task.end_time > now]
            removed = original_len - len(self.tasks)
            if removed:
                logger.debug(f"flushed {removed} expired tasks from queue")
                await self._update_offers()
            return removed

    async def remove_task(self, task_id: uuid.UUID) -> bool:
        """Remove a specific task by its queued id."""
        async with self._lock:
            original_len = len(self.tasks)
            self.tasks = [q for q in self.tasks if q.id != task_id]
            removed = original_len - len(self.tasks)
            if removed:
                await self._update_offers()
                return True
            return False

    async def _update_offers(self):
        """Update the offer window based on current tasks."""
        self.program_binding.clear_offers()

        if self.tasks:
            # Offer window spans from now to the latest task end time
            now = datetime.now(UTC)

            # Add individual offers for each task
            for queued in self.tasks:
                self.program_binding.add_offer(
                    start=now,
                    end=queued.task.end_time,
                    obj=queued.id,
                )

        await self.program_binding.publish_offers()

    def __len__(self):
        return len(self.tasks)