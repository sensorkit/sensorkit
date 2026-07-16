# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from intervaltree import Interval, IntervalTree
from pydantic import BaseModel, Field

from sensorkit.auto.mode import Mode
from sensorkit.common.interval import (
    combine_interval_trees,
    print_interval_tree,
    stack_interval_trees,
    stamp_interval_trees,
)
from sensorkit.core.task import TaskContextOverlay


class ProgramConfig(BaseModel):
    """Scheduling configuration for a single Program within a Controller."""
    program: str
    priority: int = 5
    interrupt: bool = False
    contexts: TaskContextOverlay = Field(default_factory=TaskContextOverlay)


class ScheduleInterval(Interval):
    """A typed interval carrying a ScheduleIntent as its data payload."""
    begin: datetime
    end: datetime
    data: ScheduleIntent | None


@dataclass
class ScheduleIntent:
    """The intended operating mode and ordered program candidates for a schedule interval."""
    mode: str
    programs: tuple[str, ...] = tuple()


type Schedule = Sequence[ScheduleInterval]


class Scheduler:
    """Combines user configuration and Program tasking availability into an operating schedule."""

    def __init__(
        self,
        modes: Iterable[Mode],
        program_configs: list[ProgramConfig],
    ):
        # Sort the input modes in priority order.
        self.modes = sorted(modes, reverse=True)
        self.program_configs = {config.program: config for config in program_configs}
        self.combined_offers = IntervalTree()
        self.intervals = IntervalTree()
        self._offer_history = IntervalTree()

    def get_schedule(self) -> Schedule:
        """Returns a serializable copy of the current schedule."""
        return tuple(self.intervals)

    def get_intent(self, dt: datetime):
        """Retrieves the scheduled intent at the given time, if any."""
        ivs = self.intervals.at(dt)

        if not ivs:
            return None

        assert len(ivs) == 1
        return cast(ScheduleIntent, next(iter(ivs)).data)

    def update(
        self,
        *,
        offers_dict: dict[str, IntervalTree],
        enabled_programs: set[str],
        mode_context: dict[str, Any] = None
    ):
        """Updates the schedule."""
        mode_context = mode_context.copy() if mode_context else {}

        now = datetime.now(UTC)

        # Snapshot the previous combined offers before rebuilding so we can
        # detect offers that were removed mid-window (e.g. task finished early).
        prev_combined = self.combined_offers

        mode_context["scheduler_previous_combined"] = prev_combined
        mode_context["scheduler_offer_history"] = self._offer_history

        # Evaluate observing mode criteria. This gets us an IntervalTree for each configured
        # observing mode.
        evaluated_modes = [mode.evaluate(mode_context) for mode in self.modes]

        # Get all the program offers for this Controller and sort them by configured priority.
        offers = sorted(
            (it for it in offers_dict.items() if it[0] in enabled_programs),
            key=lambda item: self.program_configs[item[0]].priority
        )

        # Combine into our updated schedule.
        self.combined_offers, self.intervals = create_schedule(
            self.modes,
            evaluated_modes,
            (it[0] for it in offers),
            (it[1] for it in offers),
        )

        # FIXME: Below only exists to support the `after_activity` mode criterion, which should
        #        instead use closed-loop feedback about task execution (including manual).
        # Detect offers that disappeared while still in-progress, and inform schedule
        for iv in prev_combined:
            if iv.begin < now < iv.end and not self.combined_offers.overlaps(iv.begin, iv.end):
                truncated_end = now
                if not self._offer_history.containsi(iv.begin, truncated_end, iv.data):
                    self._offer_history.addi(iv.begin, truncated_end, iv.data)

        # Merge current offers into the persistent history
        for iv in self.combined_offers:
            if not self._offer_history.containsi(iv.begin, iv.end, iv.data):
                self._offer_history.addi(iv.begin, iv.end, iv.data)

        # Prune historical entries
        cutoff = now - timedelta(hours=1)
        stale = [iv for iv in self._offer_history if iv.end <= cutoff]
        for iv in stale:
            self._offer_history.discard(iv)


def debug_print_schedule(arg: Schedule | Scheduler, *, lookahead_mins=30, print_func=print):
    """Print the schedule interval tree for debugging, showing the next *lookahead_mins* minutes."""
    schedule = arg.intervals if isinstance(arg, Scheduler) else arg
    now = datetime.now(UTC)
    print_interval_tree(
        schedule,
        start_from=now,
        end_at=now + timedelta(minutes=lookahead_mins),
        key_func=lambda iv: iv.data.programs[0] if iv.data.programs else None,
        print_func=print_func,
        legend_show_count=True,
        legend_show_keys=True,
    )


def create_schedule(
    modes: Iterable[Mode],
    evaluated_modes: Iterable[IntervalTree],
    programs: Iterable[str],
    offers: Iterable[IntervalTree],
):
    """Build a combined schedule from evaluated mode intervals and program offer trees.

    Returns `(combined_offers, schedule)` where *schedule* is an IntervalTree whose
    intervals carry `ScheduleIntent` objects with the active mode and candidate programs.
    """
    # Combine each set of intervals by stacking them in order, removing overlap regions.
    # The result is that operate modes take precedence over standby modes. We also set the
    # associated data with each interval to a new `ScheduleIntent` object.
    schedule = stack_interval_trees(
        *evaluated_modes,
        data=(ScheduleIntent(mode.name) for mode in modes),
    )

    # Combine the tasking schedules for all associated Programs such that each interval contains
    # a priority-ordered list of Program candidates.
    combined_offers = combine_interval_trees(
        *offers,
        data=programs,
    )

    # Merge program offer intervals into the mode intervals. This is done by splitting
    # the mode intervals on offer interval boundaries. The data fields (ScheduleIntent) are
    # updated to reflect which program offer, if any, overlaps each interval.
    def merge_func(target_data: ScheduleIntent, stamp_data: tuple[str, ...]):
        return ScheduleIntent(target_data.mode, stamp_data)

    stamp_interval_trees(schedule, stamp=combined_offers, merge_func=merge_func)

    return combined_offers, schedule
