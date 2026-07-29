# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta, timezone

import pytest
from intervaltree import IntervalTree
from pydantic import TypeAdapter

from sensorkit.astro.observer import EarthObserver
from sensorkit.auto.mode import (
    AfterActivityCriterion,
    Mode,
    ModeList,
    TaskingAvailableCriterion,
    TimeRangeCriterion,
)
from sensorkit.common.time import parse_time_range


def test_mode_evaluate():
    mode = Mode(
        name="night_ops",
        state="operate",
        criteria=[
            TimeRangeCriterion(start="6pm", end="5am"),
            TimeRangeCriterion(start="8pm", end="8am"),
            TimeRangeCriterion(start="3am", end="4am"),
        ],
    )

    context: dict = {}
    tree: IntervalTree = mode.evaluate(context)

    assert isinstance(tree, IntervalTree)
    assert not tree.is_empty()

    # The intersection should be exactly the 3am-4am window for the current ref day.
    expected_start, expected_end = parse_time_range("3am", "4am")

    # Grab the single interval present.
    ivs = tree.at(expected_start)
    assert len(ivs) == 1
    iv = next(iter(ivs))

    assert iv.begin == expected_start
    assert iv.end == expected_end

    # Include a criterion that yields no intervals; evaluate should short-circuit to empty tree.
    mode = Mode(
        name="no_tasking",
        state="operate",
        criteria=[
            TimeRangeCriterion(start="12am", end="1am"),
            TimeRangeCriterion(start="3am", end="4am"),
        ],
    )

    tree = mode.evaluate({})
    assert isinstance(tree, IntervalTree)
    assert tree.is_empty()


def test_mode_ordering():
    operate = Mode(name="op", state="operate")
    standby = Mode(name="sb", state="standby")

    # Operate modes should compare as less-than standby (higher priority)
    assert (operate < standby) is True
    assert (standby < operate) is False
    assert (operate < operate) is False
    assert (standby < standby) is False


def test_modelist_unique_names_validator():
    # Valid unique names
    data_ok = [
        {"name": "a", "state": "operate"},
        {"name": "b", "state": "standby"},
    ]
    modes = TypeAdapter(ModeList).validate_python(data_ok)
    assert len(modes) == 2
    assert {m.name for m in modes} == {"a", "b"}

    # Duplicate names should raise ValueError from the AfterValidator
    data_dup = [
        {"name": "a", "state": "operate"},
        {"name": "a", "state": "standby"},
    ]
    with pytest.raises(ValueError):
        TypeAdapter(ModeList).validate_python(data_dup)


def test_time_range():
    # From the example: concrete times with explicit time zones
    criterion = TimeRangeCriterion(start="4pm utc-10", end="1am hst")

    intervals = list(criterion.evaluate(context={}))
    assert len(intervals) == 1
    start, end = intervals[0]

    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.tzinfo is not None
    assert end.tzinfo is not None
    # We cannot assert specific absolute times due to current date dependency,
    # but we can require a reasonable ordering and duration
    assert start < end
    assert (end - start) < timedelta(hours=24)


@pytest.mark.asyncio
async def test_time_range_with_sunrise_sunset():
    samples = [
        ("sunrise", "sunset"),
        ("sunset + 5m", "sunrise - 5m"),
    ]
    criteria = [TimeRangeCriterion(start=a, end=b) for a, b in samples]

    observer = await EarthObserver.get(lat_deg=51.5, lon_deg=-0.1)
    context = {
        "time_range_parsers": {
            "sunrise": observer.get_sunrise_time,
            "sunset": observer.get_sunset_time,
        }
    }

    for criterion in criteria:
        intervals = list(criterion.evaluate(context))
        assert len(intervals) == 1
        start, end = intervals[0]

        # Basic shape and ordering checks
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert start.tzinfo is not None
        assert end.tzinfo is not None
        assert start < end

        # Sanity-check: interval should be within a plausible daily window (< 36h)
        assert end - start < timedelta(hours=36)


def test_time_range_24h_format():
    criterion = TimeRangeCriterion(start="22:00 utc", end="06:00 utc")
    intervals = list(criterion.evaluate(context={}))
    assert len(intervals) == 1
    start, end = intervals[0]
    assert start < end
    duration = end - start
    assert timedelta(hours=7) < duration < timedelta(hours=9)


def test_time_range_compat_kind_to_when():
    """The model_validator rewrites legacy 'kind' key to 'when'."""
    c = TimeRangeCriterion.model_validate({"kind": "time_range", "start": "8pm utc", "end": "9pm utc"})
    assert c.when == "time_range"


def _make_tree(*intervals):
    """Build an IntervalTree from (begin, end, data) tuples."""
    tree = IntervalTree()
    for iv in intervals:
        tree.addi(*iv)
    return tree


def test_tasking_available_no_context():
    """Empty context returns no intervals."""
    c = TaskingAvailableCriterion()
    result = list(c.evaluate(context={}))
    assert result == []


def test_tasking_available_no_filter():
    """Without from_program, returns all intervals."""
    now = datetime.now(timezone.utc)
    tree = _make_tree(
        (now, now + timedelta(hours=1), ("prog_a",)),
        (now + timedelta(hours=2), now + timedelta(hours=3), ("prog_b",)),
    )

    c = TaskingAvailableCriterion()
    result = list(c.evaluate(context={"scheduler_previous_combined": tree}))
    assert len(result) == 2


def test_tasking_available_with_filter():
    """from_program filters intervals to those containing the program."""
    now = datetime.now(timezone.utc)
    tree = _make_tree(
        (now, now + timedelta(hours=1), ("prog_a",)),
        (now + timedelta(hours=1), now + timedelta(hours=2), ("prog_a", "prog_b")),
        (now + timedelta(hours=2), now + timedelta(hours=3), ("prog_b",)),
    )

    c = TaskingAvailableCriterion(from_program="prog_a")
    result = list(c.evaluate(context={"scheduler_previous_combined": tree}))

    # Only first two intervals contain prog_a
    assert len(result) == 2
    for iv in result:
        assert "prog_a" in iv.data


def test_tasking_available_filter_no_match():
    """Filtering by a program not present returns empty."""
    now = datetime.now(timezone.utc)
    tree = _make_tree(
        (now, now + timedelta(hours=1), ("prog_a",)),
    )

    c = TaskingAvailableCriterion(from_program="prog_x")
    result = list(c.evaluate(context={"scheduler_previous_combined": tree}))
    assert len(result) == 0


def test_tasking_available_empty_tree():
    """An empty IntervalTree is falsy, returns empty list."""
    c = TaskingAvailableCriterion()
    result = list(c.evaluate(context={"scheduler_previous_combined": IntervalTree()}))
    assert result == []


def test_tasking_available_compat_kind():
    c = TaskingAvailableCriterion.model_validate({"kind": "tasking_available"})
    assert c.when == "tasking_available"


def test_after_activity_from_offer_history():
    """Creates intervals starting at each offer's end, extending by duration."""
    now = datetime.now(timezone.utc)
    offers = _make_tree(
        (now, now + timedelta(hours=1)),
        (now + timedelta(hours=2), now + timedelta(hours=3)),
    )

    c = AfterActivityCriterion(duration="0:30")
    result = c.evaluate(context={"scheduler_offer_history": offers})

    intervals = sorted(result, key=lambda iv: iv.begin)
    assert len(intervals) == 2

    # First interval: starts at end of first offer, lasts 30 min
    assert intervals[0].begin == now + timedelta(hours=1)
    assert intervals[0].end == now + timedelta(hours=1, minutes=30)

    # Second interval: starts at end of second offer, lasts 30 min
    assert intervals[1].begin == now + timedelta(hours=3)
    assert intervals[1].end == now + timedelta(hours=3, minutes=30)


def test_after_activity_falls_back_to_combined():
    """When offer_history is missing, uses scheduler_previous_combined."""
    now = datetime.now(timezone.utc)
    combined = _make_tree(
        (now, now + timedelta(hours=2)),
    )

    c = AfterActivityCriterion(duration="1:00")
    result = c.evaluate(context={"scheduler_previous_combined": combined})

    intervals = list(result)
    assert len(intervals) == 1
    assert intervals[0].begin == now + timedelta(hours=2)
    assert intervals[0].end == now + timedelta(hours=3)


def test_after_activity_prefers_history_over_combined():
    """offer_history takes priority over combined when both present."""
    now = datetime.now(timezone.utc)
    history = _make_tree(
        (now, now + timedelta(hours=1)),
    )
    combined = _make_tree(
        (now, now + timedelta(hours=5)),
    )

    c = AfterActivityCriterion(duration="0:15")
    result = c.evaluate(context={
        "scheduler_offer_history": history,
        "scheduler_previous_combined": combined,
    })

    intervals = list(result)
    assert len(intervals) == 1
    # Should use history (ends at +1h), not combined (ends at +5h)
    assert intervals[0].begin == now + timedelta(hours=1)
    assert intervals[0].end == now + timedelta(hours=1, minutes=15)


def test_after_activity_empty_context():
    c = AfterActivityCriterion(duration="0:30")
    result = c.evaluate(context={})
    assert len(list(result)) == 0


def test_after_activity_empty_tree():
    c = AfterActivityCriterion(duration="0:30")
    result = c.evaluate(context={"scheduler_offer_history": IntervalTree()})
    assert len(list(result)) == 0


def test_after_activity_compat_kind():
    c = AfterActivityCriterion.model_validate({"kind": "after_activity", "duration": "0:30"})
    assert c.when == "after_activity"
