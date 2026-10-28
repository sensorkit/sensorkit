# SPDX-License-Identifier: Apache-2.0
"""Track controller failures and retry and recovery timing."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class FailureState:
    """Track controller failures and schedule retries, recovery, and emergency shutdown.

    Two failures trigger recovery unless the controller stayed settled for the stable period
    between them. Exhausting recovery attempts triggers an emergency shutdown. Each procedure
    counts at most one failure, so one incident seen by several observers counts once.
    """

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
    failures: int = field(default=0, init=False)
    """Failures counted toward recovery. Successful recovery resets this to one."""
    attempts: int = field(default=0, init=False)
    """Failed recovery attempts in the current recovery cycle."""
    retry_at: float | None = field(default=None, init=False)
    """Earliest event loop time for the next retry or recovery attempt."""
    failure: BaseException | None = field(default=None, init=False)
    """First controller failure counted for the current procedure, if any."""
    settled_at: float | None = field(default=None, init=False)
    """Event loop time when the current settled interval began."""

    @property
    def recovery_due(self) -> bool:
        """Whether the next command is a recovery attempt."""
        return self.failures >= 2

    @property
    def emergency_due(self) -> bool:
        """Whether the next recovery attempt is an emergency shutdown."""
        return self.attempts >= self.recovery_attempts

    def waiting(self, now: float) -> float | None:
        """Return the pending retry deadline in event loop time, or None if no delay remains."""
        if self.retry_at is not None and now < self.retry_at:
            return self.retry_at

        return None

    def begin_procedure(self):
        """Clear the current procedure's failure without clearing accumulated counts."""
        self.failure = None

    def record_initial_error(self):
        """Count a controller error found at startup once, without delaying the first command."""
        if self.failures == 0:
            self.failures = 1
            self.settled_at = None

    def record_failure(self, now: float, error: BaseException):
        """Record and count the first controller failure for the current procedure."""
        if self.failure is not None:
            return

        self.failure = error
        self.set_settled(False, now)
        self.failures += 1

        # Only the first failure schedules a retry delay.
        if not self.recovery_due:
            self.retry_at = now + self.retry_delay

    def set_settled(self, settled: bool, now: float):
        """Track continuous settled time and reset failure counts when earned."""
        if settled:
            if self.settled_at is None:
                self.settled_at = now
            return

        if self.settled_at is not None:
            if now - self.settled_at >= self.stable_period:
                self.failures = 0
            self.settled_at = None

    def delay_retry(self, now: float):
        """Delay the next command without counting a controller failure."""
        self.retry_at = now + self.retry_delay

    def record_recovery(self, now: float):
        """Record a successful recovery.

        Another failure before the stable period ends triggers recovery again.
        """
        self.failures = 1
        self.attempts = 0
        self.retry_at = now + self.retry_delay

    def record_failed_attempt(self, now: float):
        """Count a failed recovery attempt."""
        self.attempts += 1

        # Exhausted attempts fall back to emergency shutdown without waiting.
        if not self.emergency_due:
            self.retry_at = now + self.attempt_delay

    def record_failed_emergency(self, now: float):
        """Resume recovery attempts after a delay."""
        self.attempts = 0
        self.retry_at = now + self.cycle_delay

    def clear(self):
        """Reset escalation counters and timing after emergency shutdown."""
        self.failures = 0
        self.attempts = 0
        self.retry_at = None
        self.settled_at = None
