from datetime import UTC, datetime, timedelta

from intervaltree import IntervalTree

from sensorkit.auto.mode import Mode
from sensorkit.auto.scheduler import ScheduleIntent, create_schedule


def test_create_schedule_mode_priority():
    # Two modes that fully overlap: standby and operate. Operate should take precedence.
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    t1 = datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)

    operate = Mode(name="operate", state="operate")
    standby = Mode(name="standby", state="standby")

    # evaluated_modes align with the modes iterator order passed to create_schedule
    modes = [standby, operate]  # lower precedence first, higher precedence last
    evaluated_modes = [
        IntervalTree.from_tuples([(t0, t1)]),  # standby
        IntervalTree.from_tuples([(t0, t1)]),  # operate
    ]

    _, schedule = create_schedule(modes, evaluated_modes, programs=[], offers=[])

    # Expect a single interval that carries the operate mode and no program selection.
    intervals = sorted(schedule)
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.begin == t0 and iv.end == t1
    assert isinstance(iv.data, ScheduleIntent)
    assert iv.data.mode == "operate"
    assert len(iv.data.programs) == 0


def test_create_schedule_offer_priority():
    # Mode interval spans an hour. Two programs have overlapping offers.
    base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    t0 = base
    t3 = base + timedelta(minutes=30)
    t5 = base + timedelta(minutes=50)
    t8 = base + timedelta(minutes=80)
    t10 = base + timedelta(minutes=100)

    operate = Mode(name="operate", state="operate")
    modes = [operate]
    evaluated_modes = [IntervalTree.from_tuples([(t0, t10)])]

    # Program A offers [t0, t5), Program B offers [t3, t8).
    prog_names = ["A", "B"]
    offer_trees = [
        IntervalTree.from_tuples([(t0, t5)]),
        IntervalTree.from_tuples([(t3, t8)]),
    ]

    _, schedule = create_schedule(modes, evaluated_modes, programs=prog_names, offers=offer_trees)

    intervals = sorted(schedule)
    # Expect three pieces after stamping at boundaries t3 and t8.
    assert [(iv.begin, iv.end, iv.data.mode, iv.data.programs) for iv in intervals] == [
        (t0, t3, "operate", ("A",)),
        (t3, t5, "operate", ("A", "B")),
        (t5, t8, "operate", ("B",)),
        (t8, t10, "operate", ()),
    ]


def test_create_schedule_no_offer():
    base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    t0 = base
    t1 = base + timedelta(minutes=15)

    operate = Mode(name="operate", state="operate")

    _, schedule = create_schedule(
        [operate],
        [IntervalTree.from_tuples([(t0, t1)])],
        programs=[],
        offers=[],
    )

    intervals = sorted(schedule)
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.begin == t0 and iv.end == t1
    assert iv.data.mode == "operate"
    assert len(iv.data.programs) == 0


def test_create_schedule_exact_offer():
    # Offer exactly matches the mode interval; expect a single stamped interval.
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    t1 = datetime(2025, 1, 1, 0, 30, 0, tzinfo=UTC)

    mode = Mode(name="operate", state="operate")
    modes = [mode]
    evaluated_modes = [IntervalTree.from_tuples([(t0, t1)])]

    programs = ["C"]
    offers = [IntervalTree.from_tuples([(t0, t1)])]

    _, schedule = create_schedule(modes, evaluated_modes, programs, offers)

    intervals = sorted(schedule)
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.begin == t0 and iv.end == t1
    assert iv.data.mode == "operate"
    assert iv.data.programs == ("C",)
