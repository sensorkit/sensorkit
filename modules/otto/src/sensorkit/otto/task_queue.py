import asyncio
from datetime import UTC, datetime

from loguru import logger

from sensorkit.std.collect import StandardCollectTask


class TaskQueue:
    """A queue of tasks to be executed, integrated with Otto."""

    def __init__(self, program_binding):
        self.tasks: list[StandardCollectTask] = []
        self.program_binding = program_binding
        self._lock = asyncio.Lock()

    async def push_task(self, task: StandardCollectTask):
        """Add a task to the queue and update offers."""
        async with self._lock:
            self.tasks.append(task)
            # Sort tasks by end_time to maintain priority
            self.tasks.sort(key=lambda t: t.end_time)
            logger.debug(f"task {task.task_id} queued: end_time={task.end_time}")
            await self._update_offers()

    async def pop_task(self) -> StandardCollectTask | None:
        """Remove and return the next available task."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.task_id} expired")

            # Return next valid task
            if self.tasks:
                task = self.tasks.pop(0)
                await self._update_offers()
                return task

            return None

    async def peek_task(self) -> StandardCollectTask | None:
        """View the next task without removing it."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.task_id} expired")

            return self.tasks[0] if self.tasks else None

    async def flush_expired(self) -> int:
        """Remove all expired tasks from the queue."""
        async with self._lock:
            now = datetime.now(UTC)
            original_len = len(self.tasks)
            self.tasks = [t for t in self.tasks if t.end_time and t.end_time > now]
            removed = original_len - len(self.tasks)
            if removed:
                logger.debug(f"flushed {removed} expired tasks from queue")
                await self._update_offers()
            return removed

    async def remove_task(self, task_id: str) -> bool:
        """Remove a specific task by ID."""
        async with self._lock:
            original_len = len(self.tasks)
            self.tasks = [t for t in self.tasks if t.task_id != task_id]
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
            for task in self.tasks:
                self.program_binding.add_offer(
                    start=now,
                    end=task.end_time,
                    obj=task.task_id,
                )

        await self.program_binding.publish_offers()

    def __len__(self):
        return len(self.tasks)