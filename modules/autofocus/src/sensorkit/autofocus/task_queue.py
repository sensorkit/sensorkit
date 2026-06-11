from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from loguru import logger

from sensorkit.autofocus.calibrate import FocusCalibrateTask  # TEMPORARY: see calibrate.py


class TaskQueue:
    """A queue of focus calibration tasks integrated with the autofocus program."""

    def __init__(self, program_binding):
        self.tasks: list[FocusCalibrateTask] = []
        self.program_binding = program_binding
        self._lock = asyncio.Lock()

    async def push_task(self, task: FocusCalibrateTask):
        """Add a task to the queue and update offers."""
        async with self._lock:
            self.tasks.append(task)
            self.tasks.sort(key=lambda t: t.end_time)
            logger.debug(f"Focus calibrate task {task.task_id} queued: end_time={task.end_time}")
            await self.update_offers()

    async def pop_task(self) -> FocusCalibrateTask | None:
        """Remove and return the next available task."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Focus calibrate task {expired.task_id} expired")

            if self.tasks:
                return self.tasks.pop(0)

            return None

    async def peek_task(self) -> FocusCalibrateTask | None:
        """View the next task without removing it."""
        async with self._lock:
            now = datetime.now(UTC)

            while self.tasks and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Focus calibrate task {expired.task_id} expired")

            return self.tasks[0] if self.tasks else None

    async def update_offers(self):
        """Update the offer window based on current tasks."""
        self.program_binding.clear_offers()

        if self.tasks:
            now = datetime.now(UTC)
            for task in self.tasks:
                self.program_binding.add_offer(
                    start=now,
                    end=task.end_time,
                    obj=task.task_id,
                )

        await self.program_binding.publish_offers()

    def __len__(self):
        return len(self.tasks)
