# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, override

from intervaltree import IntervalTree
from loguru import logger
from pydantic import AfterValidator, BaseModel, Field, model_validator

from sensorkit.common.interval import intersect_interval_trees
from sensorkit.common.time import TimeRangeError, parse_time_delta, parse_time_range

DAILY_PERIOD = timedelta(days=1)


class Mode(BaseModel):
    """A named operating mode with scheduling criteria and an associated sensor state."""
    name: str
    description: str = ""
    state: Literal["operate", "standby"] = "operate"
    criteria: list[AnyCriterion] = Field(default_factory=list)
    context: dict = Field(default_factory=dict)

    def evaluate(self, context: Mapping[str, Any]):
        """Evaluate all criteria and return an IntervalTree of common intervals."""
        trees: list[IntervalTree] = []

        for criterion in self.criteria:
            # Evaluate the criterion, returning a tree of datetime intervals.
            tree = IntervalTree.from_tuples(criterion.evaluate(context))

            if tree.is_empty():
                # We can short-circuit if any evaluation returns no intervals.
                return tree

            # Merge overlaps to make the resulting tree disjoint.
            tree.merge_overlaps(strict=False)
            trees.append(tree)

        # Return the intersection of all trees, applying AND semantics.
        return intersect_interval_trees(*trees)

    def __lt__(self, other):
        # Operate states take priority over standby states.
        return self.state != other.state and self.state == "operate"


def _ensure_unique_mode_names(modes: list[Mode]):
    if len(modes) != len(set(mode.name for mode in modes)):
        raise ValueError("Mode names must be unique")

    return modes


ModeList = Annotated[
    list[Mode],
    AfterValidator(_ensure_unique_mode_names)
]

type IntervalList = Iterable[tuple[datetime, datetime]]
type AnyCriterion = TimeRangeCriterion | TaskingAvailableCriterion | AfterActivityCriterion


class Criterion(BaseModel, ABC):
    """Abstract scheduling criterion that produces a list of valid time intervals."""
    when: str

    @abstractmethod
    def evaluate(self, context: Mapping[str, Any]) -> IntervalList:
        """Evaluate this criterion and return valid `(start, end)` datetime interval tuples."""

    @model_validator(mode="before")
    @classmethod
    def _compat(cls, data: Any):
        if isinstance(data, dict) and "kind" in data:
            logger.warning(f"update {data["kind"]} criterion to specify 'when' instead of 'kind'")
            assert "when" not in data
            data["when"] = data.pop("kind")

        return data


class TimeRangeCriterion(Criterion):
    """Criterion satisfied during a configured daily time range (e.g. sunset to sunrise)."""
    when: Literal["time_range"] = "time_range"
    start: str
    end: str

    @override
    def evaluate(self, context: Mapping[str, Any]):
        time_ref = context.get("time_ref") or datetime.now(UTC)
        symbol_handlers = context.get("time_range_parsers")
        horizon = time_ref + DAILY_PERIOD
        start, end = parse_time_range(
            self.start,
            self.end,
            time_ref=time_ref,
            symbol_handlers=symbol_handlers,
        )

        yield start, end

        if end >= horizon:
            return

        # A day-periodic spec's next occurrence starts one period on from this one, which
        # is always enough to reach the horizon.
        try:
            next_start, next_end = parse_time_range(
                self.start,
                self.end,
                time_ref=start + DAILY_PERIOD,
                symbol_handlers=symbol_handlers,
            )
        except TimeRangeError:
            return

        if next_end == end:
            # Correct the edge case where the input endpoints evaluate to the same time.
            next_start += DAILY_PERIOD
            next_end += DAILY_PERIOD

        yield next_start, next_end


class TaskingAvailableCriterion(Criterion):
    """Criterion satisfied when a program has active offer intervals in the current schedule."""
    when: Literal["tasking_available"] = "tasking_available"
    from_program: str | None = None

    @override
    def evaluate(self, context: Mapping[str, Any]):
        offers: IntervalTree

        if offers := context.get("scheduler_previous_combined"):
            if self.from_program is not None:
                return (iv for iv in offers if self.from_program in iv.data)
            else:
                return offers
        else:
            return []


class AfterActivityCriterion(Criterion):
    """Criterion satisfied for a configured duration after each completed program offer window."""
    when: Literal["after_activity"] = "after_activity"
    duration: str

    @override
    def evaluate(self, context: Mapping[str, Any]):
        tree = IntervalTree()
        duration = parse_time_delta(self.duration)

        offers: IntervalTree

        if offers := context.get("scheduler_offer_history"):
            for iv in offers:
                tree.addi(iv.end, iv.end + duration)
        elif offers := context.get("scheduler_previous_combined"):
            for iv in offers:
                tree.addi(iv.end, iv.end + duration)

        return tree
