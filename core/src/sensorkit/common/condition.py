"""Condition types for threshold and change-detection logic over value streams."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal, override

from pydantic import BaseModel


def resolve_field(obj: object, field_path: str) -> object:
    """Resolve a dot-separated field path on an object or dict."""
    value = obj
    for part in field_path.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = getattr(value, part, None)
        if value is None:
            return None
    return value


def _coerce_to_threshold_type(value: object, threshold: float | str | bool | None) -> object:
    """Coerce a value to the threshold's type for comparison."""
    if threshold is None:
        return value
    if isinstance(threshold, bool):
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        return bool(value)
    if isinstance(threshold, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return value


class Condition(BaseModel, ABC):
    """Abstract condition evaluated against a value stream. Discriminated on `kind`."""

    kind: str

    @abstractmethod
    def evaluate(
        self,
        current: object,
        previous: object,
        was_active: bool,
    ) -> tuple[bool, bool]:
        """Evaluate the condition.

        Returns:
            A `(should_notify, is_active)` tuple.

            *should_notify* -- `True` if a notification should be sent this
            tick.

            *is_active* -- `True` if the condition is currently in an "active"
            zone (used for deadband/hysteresis tracking).  For conditions
            without deadband the two values are identical.
        """
        ...


class ChangesCondition(Condition):
    """Fires on any value change."""

    kind: Literal["changes"] = "changes"

    @override
    def evaluate(self, current, previous, was_active):
        """Return `(True, True)` when `current != previous`, otherwise `(False, False)`."""
        fired = current != previous
        return (fired, fired)


class AboveCondition(Condition):
    """Fires every update while value > threshold.

    With *deadband*: remains active until value < threshold - deadband.
    """

    kind: Literal["above"] = "above"
    threshold: float
    deadband: float = 0.0

    @override
    def evaluate(self, current, previous, was_active):
        """Fire while the numeric value exceeds the threshold, applying deadband hysteresis."""
        try:
            v = float(current)
        except (TypeError, ValueError):
            return (False, False)
        if was_active:
            still = v >= self.threshold - self.deadband
            return (still, still)
        newly = v > self.threshold
        return (newly, newly)


class BelowCondition(Condition):
    """Fires every update while value < threshold.

    With *deadband*: remains active until value > threshold + deadband.
    """

    kind: Literal["below"] = "below"
    threshold: float
    deadband: float = 0.0

    @override
    def evaluate(self, current, previous, was_active):
        """Fire while the numeric value is below the threshold, applying deadband hysteresis."""
        try:
            v = float(current)
        except (TypeError, ValueError):
            return (False, False)
        if was_active:
            still = v <= self.threshold + self.deadband
            return (still, still)
        newly = v < self.threshold
        return (newly, newly)


class EqualsCondition(Condition):
    """Fires every update while value equals threshold."""

    kind: Literal["equals"] = "equals"
    threshold: float | str | bool

    @override
    def evaluate(self, current, previous, was_active):
        """Fire on every update where current (coerced to the threshold type) equals the threshold."""
        matches = _coerce_to_threshold_type(current, self.threshold) == self.threshold
        return (matches, matches)


class BecomesCondition(Condition):
    """Fires once when value transitions to equal threshold."""

    kind: Literal["becomes"] = "becomes"
    threshold: float | str | bool | None

    @override
    def evaluate(self, current, previous, was_active):
        """Fire exactly once when the value transitions from not-equal to equal to the threshold."""
        coerced_cur = _coerce_to_threshold_type(current, self.threshold)
        coerced_prev = _coerce_to_threshold_type(previous, self.threshold)
        fired = coerced_cur == self.threshold and coerced_prev != self.threshold
        return (fired, fired)


class CrossesAboveCondition(Condition):
    """Fires once on upward crossing.

    With *deadband*: won't re-fire until value drops below
    threshold - deadband and crosses back above.
    """

    kind: Literal["crosses_above"] = "crosses_above"
    threshold: float
    deadband: float = 0.0

    @override
    def evaluate(self, current, previous, was_active):
        """Fire once when value crosses upward through the threshold; deadband prevents re-firing."""
        try:
            v = float(current)
            p = float(previous)
        except (TypeError, ValueError):
            return (False, False)
        if was_active:
            still = v >= self.threshold - self.deadband
            return (False, still)
        crossed = v > self.threshold and p <= self.threshold
        return (crossed, crossed)


class CrossesBelowCondition(Condition):
    """Fires once on downward crossing.

    With *deadband*: won't re-fire until value rises above
    threshold + deadband and crosses back below.
    """

    kind: Literal["crosses_below"] = "crosses_below"
    threshold: float
    deadband: float = 0.0

    @override
    def evaluate(self, current, previous, was_active):
        """Fire once when value crosses downward through the threshold; deadband prevents re-firing."""
        try:
            v = float(current)
            p = float(previous)
        except (TypeError, ValueError):
            return (False, False)
        if was_active:
            still = v <= self.threshold + self.deadband
            return (False, still)
        crossed = v < self.threshold and p >= self.threshold
        return (crossed, crossed)


type AnyCondition = (
    ChangesCondition
    | AboveCondition
    | BelowCondition
    | EqualsCondition
    | BecomesCondition
    | CrossesAboveCondition
    | CrossesBelowCondition
)
