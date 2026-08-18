# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from loguru import logger
from unifieddatalibrary.types import CollectRequestFull


class TaskQueue:
    """A queue of CollectRequests to be executed, integrated with UDL.

    Stores CollectRequestFull objects from the UDL API. Conversion to
    StandardCollectTask happens in the program's task generator.
    """

    def __init__(self, program_binding, on_expired=None, executing_deadband_s: float = 0.0):
        self.tasks: list[CollectRequestFull] = []
        self.program_binding = program_binding
        self._lock = asyncio.Lock()

        # Async callback invoked for each request whose window ends while it is still queued.
        self._on_expired = on_expired

        # The popped request currently executing. Its offer is kept standing
        # until remove_task: if the offer set goes empty mid-execution, the
        # agent tears the lifecycle down and its init task displaces the
        # running collect. The offer extends past the request window by the
        # same deadband the task itself is granted, so a deadband overrun
        # doesn't lose its offer mid-run.
        self._executing: CollectRequestFull | None = None
        self._executing_deadband = timedelta(seconds=executing_deadband_s)

    async def push_task(self, request: CollectRequestFull) -> None:
        """Add a CollectRequest to the queue and update offers."""
        async with self._lock:
            self.tasks.append(request)
            # Sort by (start_time, end_time) to maintain priority
            self.tasks.sort(key=lambda r: (
                r.start_time or datetime.min.replace(tzinfo=UTC),
                r.end_time or datetime.max.replace(tzinfo=UTC),
            ))
            logger.debug(f"task {request.id} queued: start={request.start_time}, end={request.end_time}")
            await self._update_offers()

    async def pop_task(self) -> CollectRequestFull | None:
        """Remove and return the next available CollectRequest."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].end_time and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")
                if self._on_expired:
                    await self._on_expired(expired)

            # Return next valid task
            if self.tasks:
                task = self.tasks.pop(0)
                self._executing = task
                await self._update_offers()
                return task

            return None

    async def peek_task(self) -> CollectRequestFull | None:
        """View the next CollectRequest without removing it."""
        async with self._lock:
            now = datetime.now(UTC)

            # Remove expired tasks
            while self.tasks and self.tasks[0].end_time and self.tasks[0].end_time <= now:
                expired = self.tasks.pop(0)
                logger.warning(f"Task {expired.id} expired")
                if self._on_expired:
                    await self._on_expired(expired)

            return self.tasks[0] if self.tasks else None

    async def remove_task(self, task_id: str) -> bool:
        """Remove a specific CollectRequest by ID."""
        async with self._lock:
            finished_executing = self._executing is not None and self._executing.id == task_id
            if finished_executing:
                self._executing = None

            original_len = len(self.tasks)
            self.tasks = [t for t in self.tasks if t.id != task_id]
            removed = original_len - len(self.tasks)
            if removed or finished_executing:
                await self._update_offers()
                return True
            return False

    async def _update_offers(self) -> None:
        """Update the offer window from the executing request plus queued tasks."""
        self.program_binding.clear_offers()

        now = datetime.now(UTC)
        if self._executing is not None:
            request = self._executing
            self.program_binding.add_offer(
                start=request.start_time or now,
                end=request.end_time + self._executing_deadband if request.end_time else None,
                obj=request.id,
            )

        for request in self.tasks:
            self.program_binding.add_offer(
                start=request.start_time or now,
                end=request.end_time,
                obj=request.id,
            )

        await self.program_binding.publish_offers()

    def iter(self):
        return iter(self.tasks)

    def __len__(self) -> int:
        return len(self.tasks)

    def __bool__(self) -> bool:
        return bool(self.tasks)
