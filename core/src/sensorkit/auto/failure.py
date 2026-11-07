# SPDX-License-Identifier: Apache-2.0
"""Track controller failures and retry and recovery timing."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, PrivateAttr, computed_field


class FailureState(BaseModel):
    """Track controller failures and schedule retries, recovery, and emergency shutdown.

    Repeated failures trigger recovery unless the stable period passed without a failure or
    recovery attempt between them. Exhausting recovery attempts, successful or failed, triggers an
    emergency shutdown. Each procedure counts at most one failure, so one incident seen by several
    observers counts once.

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
    """Seconds without a failure or recovery attempt before failure and recovery counts reset."""
    failures: int = 0
    """Failures counted toward recovery. Successful recovery resets this to one."""
    attempts: int = 0
    """Completed recovery attempts since the counts last reset or an emergency shutdown failed."""

    # Event loop times and the live exception stay out of serialized state.
    _retry_at: float | None = PrivateAttr(default=None)
    _error: BaseException | None = PrivateAttr(default=None)
    _stable_since: float | None = PrivateAttr(default=None)

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
        """Whether the next command is an emergency shutdown."""
        return self.recovery_due and self.attempts >= self.recovery_attempts

    def waiting(self, now: float) -> float | None:
        """Return the pending retry deadline in event loop time, or None if no delay remains."""
        if self._retry_at is not None and now < self._retry_at:
            return self._retry_at

        return None

    def snapshot(self, monotonic: float) -> FailureStateSnapshot:
        """Return a detached copy with UTC datetimes, exception text, and expired counts reset.

        Args:
            monotonic: The shared monotonic reference for evaluating retry deadlines.
        """
        utc_now = datetime.now(UTC) - timedelta(seconds=time.monotonic() - monotonic)

        def project(time: float | None):
            return None if time is None else utc_now + timedelta(seconds=time - monotonic)

        state = self.model_copy()
        state.expire(monotonic)
        error = state._error
        stable_since = state._stable_since

        return FailureStateSnapshot(
            **state.model_dump(include=set(FailureState.model_fields)),
            failure=None if error is None else f"{type(error).__name__}: {error}",
            retry_at=project(state.waiting(monotonic)),
            stable_at=project(None if stable_since is None else stable_since + self.stable_period),
        )

    def begin_procedure(self):
        """Clear the current failure and retry delay without clearing accumulated counts."""
        self._error = None
        self._retry_at = None

    def expire(self, now: float):
        """Reset failure and recovery counts once the stable period passes without either."""
        if self._stable_since is not None and now - self._stable_since >= self.stable_period:
            self.failures = 0
            self.attempts = 0

    def record_initial_error(self, now: float):
        """Count a controller error found at startup once, without delaying the first command."""
        self.expire(now)
        self._stable_since = now

        if self.failures == 0:
            self.failures = 1

    def record_failure(self, now: float, error: BaseException):
        """Record and count the first controller failure for the current procedure."""
        if self._error is not None:
            return

        self.expire(now)
        self._error = error
        self._stable_since = now
        self.failures += 1

        # Only the first failure schedules a retry delay.
        if not self.recovery_due:
            self._retry_at = now + self.retry_delay

    def delay_retry(self, now: float):
        """Delay the next command without counting a controller failure."""
        self._retry_at = now + self.retry_delay

    def record_recovery(self, now: float):
        """Record a successful recovery.

        Another failure before the stable period ends triggers recovery again and counts toward
        `recovery_attempts`.
        """
        self.failures = 1
        self.attempts += 1
        self._stable_since = now
        self._retry_at = now + self.retry_delay

    def record_failed_attempt(self, now: float):
        """Count a failed recovery attempt."""
        self.attempts += 1
        self._stable_since = now

        # Exhausted attempts fall back to emergency shutdown without waiting.
        if not self.emergency_due:
            self._retry_at = now + self.attempt_delay

    def record_failed_emergency(self, now: float):
        """Resume recovery attempts after a delay."""
        self.attempts = 0
        self._stable_since = now
        self._retry_at = now + self.cycle_delay

    def clear(self):
        """Reset escalation counters and timing after emergency shutdown."""
        self.failures = 0
        self.attempts = 0
        self._retry_at = None
        self._stable_since = None


class FailureStateSnapshot(FailureState):
    """Detached failure state with UTC times and failure text."""

    failure: str | None = None
    """First controller failure counted for the current procedure, if any."""
    retry_at: datetime | None = None
    """Earliest retry or recovery time, or None if no delay remains."""
    stable_at: datetime | None = None
    """When counts reset or will reset without another failure, or None if none were counted."""
