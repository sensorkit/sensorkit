# SPDX-License-Identifier: Apache-2.0
import random
from datetime import datetime, timedelta

from intervaltree import IntervalTree

from sensorkit.common.interval import (
    combine_interval_trees,
    intersect_interval_trees,
    stack_interval_trees,
    stamp_interval_trees,
)


def _to_tuples(tree: IntervalTree) -> list[tuple[int, int]]:
    return sorted((iv.begin, iv.end) for iv in tree)


def _dump(tree: IntervalTree) -> list[tuple[int, int, object]]:
    return sorted((iv.begin, iv.end, iv.data) for iv in tree)


def _labels_at(tree: IntervalTree, x: int) -> set:
    return {iv.data for iv in tree.at(x)}


def test_intersect_interval_trees_basic():
    # Two simple, disjoint-sorted inputs of half-open intervals
    a = [(0, 5), (10, 20), (25, 30)]
    b = [(3, 12), (18, 22), (27, 40)]

    out = intersect_interval_trees(a, b)

    # Expected intersections: [3,5), [10,12), [18,20), [27,30)
    assert _to_tuples(out) == [(3, 5), (10, 12), (18, 20), (27, 30)]


def test_intersect_interval_trees_multiple_and_endpoint_touching():
    # Three inputs; include touching endpoints which should not produce zero-length intervals
    a = [(0, 5), (7, 10)]
    b = [(5, 7), (8, 12)]  # touches a at 5 and 7
    c = [(1, 9)]

    out = intersect_interval_trees(a, b, c)

    # Intersections across all three:
    # - [0,5)∩[5,7) = empty at endpoint (no interval)
    # - [7,10)∩[8,12)∩[1,9) = [8,9)
    assert _to_tuples(out) == [(8, 9)]


def test_intersect_interval_trees_with_empty_input():
    a = [(0, 10)]
    b: list[tuple[int, int]] = []

    out = intersect_interval_trees(a, b)
    assert len(out) == 0


def test_interval_trees_from_unsorted_inputs():
    a = [(i * 10, i * 10 + 7) for i in range(12)]
    b = [(i * 10 + 3, i * 10 + 12) for i in range(12)]

    tree_a = IntervalTree.from_tuples(a)
    tree_b = IntervalTree.from_tuples(b)

    assert _to_tuples(intersect_interval_trees(tree_a, tree_b)) == _to_tuples(
        intersect_interval_trees(a, b)
    )
    assert _dump(combine_interval_trees(tree_a, tree_b, data=("A", "B"))) == _dump(
        combine_interval_trees(a, b, data=("A", "B"))
    )


def test_stack_interval_trees_splitting_and_labels():
    # Build stack: add A, then B, then C; later intervals override earlier ones
    segments_a = [(0, 10)]
    segments_b = [(5, 15)]
    segments_c = [(2, 7)]

    out = stack_interval_trees(segments_a, segments_b, segments_c, data=["A", "B", "C"])

    # After stacking with override semantics, there are no overlaps:
    # A: [0,2), C: [2,7), B: [7,15)
    assert _labels_at(out, 1) == {"A"}  # [0,2)
    assert _labels_at(out, 3) == {"C"}  # [2,7)
    assert _labels_at(out, 6) == {"C"}  # [2,7)
    assert _labels_at(out, 8) == {"B"}  # [7,15)
    assert _labels_at(out, 12) == {"B"}  # [7,15)

    # Half-open endpoints
    assert _labels_at(out, 0) == {"A"}
    assert _labels_at(out, 2) == {"C"}
    assert _labels_at(out, 5) == {"C"}
    assert _labels_at(out, 7) == {"B"}
    assert _labels_at(out, 10) == {"B"}
    assert _labels_at(out, 15) == set()

    # Endpoints reflect merged segments without redundant splits at 5 and 10
    endpoints = sorted({p for iv in out for p in (iv.begin, iv.end)})
    assert endpoints == [0, 2, 7, 15]


def test_stack_interval_trees_with_existing_target_and_chop():
    # Pre-existing target interval X that spans the whole region; stacking should chop it
    target = IntervalTree()
    target.addi(0, 20, "X")

    # Now stack two new layers; later intervals override earlier ones in the overlapped region
    a = [(3, 8)]
    b = [(12, 18)]
    out = stack_interval_trees(a, b, data=["A", "B"], target=target)

    # Existing data X remains only where not covered by A or B
    assert _labels_at(out, 1) == {"X"}
    assert _labels_at(out, 4) == {"A"}
    assert _labels_at(out, 9) == {"X"}
    assert _labels_at(out, 13) == {"B"}
    assert _labels_at(out, 19) == {"X"}

    # Boundaries should include original [0,20) plus new splits at 3,8,12,18
    endpoints = sorted({p for iv in out for p in (iv.begin, iv.end)})
    assert endpoints == [0, 3, 8, 12, 18, 20]


def test_stamp_interval_trees():
    # target: [0,10)='A', [10,20)='B'
    target = IntervalTree()
    target.addi(0, 10, "A")
    target.addi(10, 20, "B")

    # stamp: [5,15)='S'
    stamp = IntervalTree()
    stamp.addi(5, 15, "S")

    def merge_func(t, s):
        return f"{t}+{s}"

    stamp_interval_trees(target, stamp=stamp, merge_func=merge_func)

    # Expect: [0,5)='A', [5,10)='A+S', [10,15)='B+S', [15,20)='B'
    expected = [
        (0, 5, "A"),
        (5, 10, "A+S"),
        (10, 15, "B+S"),
        (15, 20, "B"),
    ]
    assert _dump(target) == expected


def test_stamp_interval_trees_no_overlaps():
    # target: [0,10)='A'
    target = IntervalTree()
    target.addi(0, 10, "A")

    # stamp touches at endpoint 10 and starts at 10 -> no overlap
    stamp = IntervalTree()
    stamp.addi(10, 15, "S")

    stamp_interval_trees(target, stamp=stamp, merge_func=lambda t, s: (t, s))

    # unchanged
    assert _dump(target) == [(0, 10, "A")]


def test_stamp_interval_trees_complex():
    # target: [0,8)='X', [8,12)='Y', [12,20)='Z'
    target = IntervalTree()
    target.addi(0, 8, "X")
    target.addi(8, 12, "Y")
    target.addi(12, 20, "Z")

    # stamps: [3,10)='A', [15,18)='B'
    stamp = IntervalTree()
    stamp.addi(3, 10, "A")
    stamp.addi(15, 18, "B")

    def merge(t, s):
        return t, s

    stamp_interval_trees(target, stamp=stamp, merge_func=merge)

    # Expect splits at 3,8,10,15,18 with merging only inside stamps
    expected = [
        (0, 3, "X"),
        (3, 8, ("X", "A")),
        (8, 10, ("Y", "A")),
        (10, 12, "Y"),
        (12, 15, "Z"),
        (15, 18, ("Z", "B")),
        (18, 20, "Z"),
    ]
    assert _dump(target) == expected


def test_combine_simple_non_overlap():
    # Two non‑overlapping intervals from separate inputs
    a = [(0, 5)]
    b = [(6, 10)]
    out = combine_interval_trees(a, b, data=("A", "B"))
    assert _dump(out) == [
        (0, 5, ("A",)),
        (6, 10, ("B",)),
    ]


def test_combine_overlaps():
    a = [(0, 10)]
    b = [(5, 15)]
    out = combine_interval_trees(a, b, data=("A", "B"))
    assert _dump(out) == [
        (0, 5, ("A",)),
        (5, 10, ("A", "B")),
        (10, 15, ("B",)),
    ]


def test_combine_order_preservation():
    a = [(0, 20)]
    b = [(5, 15)]
    c = [(7, 12)]
    out = combine_interval_trees(c, b, a, data=("C", "B", "A"))
    assert _dump(out) == [
        (0, 5, ("A",)),
        (5, 7, ("B", "A")),
        (7, 12, ("C", "B", "A")),
        (12, 15, ("B", "A")),
        (15, 20, ("A",)),
    ]


def test_combine_same_start():
    a = [(0, 10)]
    b = [(0, 5)]
    c = [(0, 3)]
    out = combine_interval_trees(a, b, c, data=("A", "B", "C"))
    assert _dump(out) == [
        (0, 3, ("A", "B", "C")),
        (3, 5, ("A", "B")),
        (5, 10, ("A",)),
    ]
    out = combine_interval_trees(c, b, a, data=("C", "B", "A"))
    assert _dump(out) == [
        (0, 3, ("C", "B", "A")),
        (3, 5, ("B", "A")),
        (5, 10, ("A",)),
    ]


def test_combine_zero_length_intervals():
    a = [(0, 0)]
    b = [(1, 3)]
    out = combine_interval_trees(a, b, data=("Z", "A"))
    # Zero‑length interval should be ignored
    assert _dump(out) == [
        (1, 3, ("A",)),
    ]


def test_combine_output_param():
    existing = IntervalTree()
    existing.addi(5, 15, "X")
    a = [(0, 4)]
    out = combine_interval_trees(a, data=("A",), target=existing)
    assert out is existing
    assert _dump(out) == [
        (0, 4, ("A",)),
        (5, 15, "X"),
    ]


def test_combine_performance():
    num_trees = 20
    num_intervals = 1000
    interval_length = timedelta(seconds=50)
    random_sec = lambda: random.randint(0, 6)
    now = datetime.now()
    trees = []

    for _ in range(num_trees):
        intervals = []
        start = now

        for _ in range(num_intervals):
            start += timedelta(seconds=random_sec())
            end = start + interval_length
            intervals.append((start, end))

        intervals.sort()
        trees.append(intervals)

    result = combine_interval_trees(*trees, data=range(len(trees)))
    assert isinstance(result, IntervalTree)
    assert not result.is_empty()
