# SPDX-License-Identifier: Apache-2.0
"""Track controller failures and retry and recovery timing."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, PrivateAttr, computed_field


class FailureState(BaseModel):
    """Track controller failures and schedule retries, recovery, and emergency shutdown.

    Repeated failures trigger recovery unless the controller stayed settled for the stable
    period between them. Exhausting recovery attempts triggers an emergency shutdown. Each
    procedure counts at most one failure, so one incident seen by several observers counts once.

    Lifecycle bookkeeping uses monotonic timestamps and exception objects. Published snapshots
    use UTC datetimes and exception text.
    """

    recover_after_failures: int = 2
    """Failures that trigger recovery."""
    retry_delay: float = 5.0
    """Seconds before retrying after a failure."""
    recovery_attempts: int = 6
    """Recovery attempts before an emergency shutdown."""
    attempt_delay: float = 20.0
    """Seconds between failed recovery attempts."""
    cycle_delay: float = 120.0
    """Seconds after a failed emergency shutdown before recovery resumes."""
    stable_period: float = 900.0
    """Seconds a confirmed state must hold before a new failure starts a fresh count."""
    failures: int = 0
    """Failures counted toward recovery. Successful recovery resets this to one."""
    attempts: int = 0
    """Failed recovery attempts in the current recovery cycle."""

    # Event loop times and the live exception stay out of serialized state.
    _retry_at: float | None = PrivateAttr(default=None)
    _error: BaseException | None = PrivateAttr(default=None)
    _settled_at: float | None = PrivateAttr(default=None)

    @property
    def error(self) -> BaseException | None:
        """First controller failure counted for the current procedure, if any."""
        return self._error

    @computed_field
    @property
    def recovery_due(self) -> bool:
        """Whether the next command is a recovery attempt."""
        return self.failures >= self.recover_after_failures

    @computed_field
    @property
    def emergency_due(self) -> bool:
        """Whether the next recovery attempt is an emergency shutdown."""
        return self.attempts >= self.recovery_attempts

    def waiting(self, now: float) -> float | None:
        """Return the pending retry deadline in event loop time, or None if no delay remains."""
        if self._retry_at is not None and now < self._retry_at:
            return self._retry_at

        return None

    def snapshot(self, monotonic: float) -> FailureStateSnapshot:
        """Return a detached copy with UTC datetimes and exception text.

        Args:
            monotonic: The shared monotonic reference for evaluating retry deadlines.
        """
        utc_now = datetime.now(UTC) - timedelta(seconds=time.monotonic() - monotonic)

        def project(time: float | None):
            return None if time is None else utc_now + timedelta(seconds=time - monotonic)

        error = self._error

        return FailureStateSnapshot(
            **self.model_dump(include=set(FailureState.model_fields)),
            failure=None if error is None else f"{type(error).__name__}: {error}",
            retry_at=project(self.waiting(monotonic)),
            settled_at=project(self._settled_at),
        )

    def begin_procedure(self):
        """Clear the current failure and retry delay without clearing accumulated counts."""
        self._error = None
        self._retry_at = None

    def record_initial_error(self):
        """Count a controller error found at startup once, without delaying the first command."""
        if self.failures == 0:
            self.failures = 1
            self._settled_at = None

    def record_failure(self, now: float, error: BaseException):
        """Record and count the first controller failure for the current procedure."""
        if self._error is not None:
            return

        self._error = error
        self.set_settled(False, now)
        self.failures += 1

        # Only the first failure schedules a retry delay.
        if not self.recovery_due:
            self._retry_at = now + self.retry_delay

    def set_settled(self, settled: bool, now: float):
        """Track continuous settled time and reset failure counts when earned."""
        if settled:
            if self._settled_at is None:
                self._settled_at = now
            return

        if self._settled_at is not None:
            if now - self._settled_at >= self.stable_period:
                self.failures = 0
            self._settled_at = None

    def delay_retry(self, now: float):
        """Delay the next command without counting a controller failure."""
        self._retry_at = now + self.retry_delay

    def record_recovery(self, now: float):
        """Record a successful recovery.

        Another failure before the stable period ends triggers recovery again.
        """
        self.failures = 1
        self.attempts = 0
        self._retry_at = now + self.retry_delay

    def record_failed_attempt(self, now: float):
        """Count a failed recovery attempt."""
        self.attempts += 1

        # Exhausted attempts fall back to emergency shutdown without waiting.
        if not self.emergency_due:
            self._retry_at = now + self.attempt_delay

    def record_failed_emergency(self, now: float):
        """Resume recovery attempts after a delay."""
        self.attempts = 0
        self._retry_at = now + self.cycle_delay

    def clear(self):
        """Reset escalation counters and timing after emergency shutdown."""
        self.failures = 0
        self.attempts = 0
        self._retry_at = None
        self._settled_at = None


class FailureStateSnapshot(FailureState):
    """Detached failure state with UTC times and failure text."""

    failure: str | None = None
    """First controller failure counted for the current procedure, if any."""
    retry_at: datetime | None = None
    """Earliest retry or recovery time, or None if no delay remains."""
    settled_at: datetime | None = None
    """Time when the current settled interval began."""
