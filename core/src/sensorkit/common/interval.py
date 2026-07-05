# SPDX-License-Identifier: Apache-2.0
"""Utilities for constructing, combining, and visualizing interval trees."""

import collections
import heapq
from collections.abc import Hashable, Sequence
from typing import Any, Callable, Iterable

from intervaltree import Interval, IntervalTree

type IntervalLike[T] = tuple[T, T] | tuple[T, T, Any]


def combine_interval_trees(
    *inputs: Iterable[IntervalLike],
    data: Iterable[Any] | None = None,
    target: IntervalTree | None = None,
):
    """Construct an IntervalTree combining the data fields of all input interval collections.

    Each collection of intervals must be disjoint and sorted in ascending order. Assuming the
    inputs are IntervalTrees, this can be achieved using the `merge_overlaps()` method.
    """
    target = IntervalTree() if target is None else target

    match data:
        case None:
            data = tuple(None for _ in range(len(inputs)))
        case Sequence():
            pass
        case _:
            # Make sure the data iterable is materialized for random access.
            data = tuple(data)

    # Line sweep, making sure to preserve the input ordering.
    def tagged_tree(i, tree):
        return ((iv[0], i, iv[1]) for iv in tree)

    tagged_inputs = (tagged_tree(i, tree) for i, tree in enumerate(inputs))
    events: list[tuple[Any, bool, int]] = []
    endpoints = []

    for left, i, right in heapq.merge(*tagged_inputs):
        while endpoints and endpoints[0][0] <= left:
            pt, j = heapq.heappop(endpoints)
            events.append((pt, False, j))

        events.append((left, True, i))
        heapq.heappush(endpoints, (right, i))

    while endpoints:
        pt, j = heapq.heappop(endpoints)
        events.append((pt, False, j))

    if not events:
        return target

    left = events[0][0]
    active = collections.defaultdict(int)

    for event in events:
        right, rising, i = event

        if active and left < right:
            target.addi(left, right, tuple(d for d in data if d in active))

        datum = data[i]

        if rising:
            active[datum] += 1
        elif active[datum] > 1:
            active[datum] -= 1
        else:
            del active[datum]

        left = right

    return target


def intersect_interval_trees(
    *inputs: Iterable[IntervalLike],
    target: IntervalTree | None = None,
):
    """Construct an IntervalTree containing the intersection of all input interval collections.

    Each collection of intervals must be disjoint and sorted in ascending order. Assuming the
    inputs are IntervalTrees, this can be achieved using the `merge_overlaps()` method.
    """
    target = IntervalTree() if target is None else target

    # Line sweep.
    events: list[tuple[Any, bool]] = []
    endpoints = []

    for iv in heapq.merge(*inputs):
        while endpoints and endpoints[0] <= iv[0]:
            end = heapq.heappop(endpoints)
            events.append((end, False))

        events.append((iv[0], True))
        heapq.heappush(endpoints, iv[1])

    while endpoints:
        events.append((heapq.heappop(endpoints), False))

    next_start = None
    depth = 0

    for event in events:
        depth += 1 if event[1] else -1

        if depth == len(inputs):
            next_start = event[0] if next_start is None else next_start
        elif next_start is not None and next_start < event[0]:
            target.addi(next_start, event[0])
            next_start = None
        elif next_start is not None:
            next_start = None

    return target


def stack_interval_trees(
    *inputs: Iterable[IntervalLike],
    data: Iterable[Any] | None = None,
    target: IntervalTree | None = None,
):
    """Add each input interval collection to an IntervalTree, splitting existing intervals."""
    if target is None:
        target = IntervalTree()

    if data is None:
        data = (None for _ in range(len(inputs)))

    for iv_data, tree in zip(data, inputs, strict=True):
        for iv in tree:
            target.chop(iv[0], iv[1])
            target.addi(iv[0], iv[1], iv_data)

    return target


def stamp_interval_trees(
    target: IntervalTree,
    *,
    stamp: Iterable[IntervalLike],
    merge_func: Callable[[Any, Any], Any],
):
    """Slice the target IntervalTree on "stamp" interval boundaries and merge overlapping data."""
    for iv in stamp:
        # Split overlaps at both endpoints so inner pieces are fully enveloped.
        if target.overlaps_point(iv[0]):
            target.slice(iv[0])

        if target.overlaps_point(iv[1]):
            target.slice(iv[1])

        # Merge data for all intervals fully inside the stamp range.
        for enveloped in target.envelop(iv[0], iv[1]):
            target.remove(enveloped)
            target.addi(enveloped.begin, enveloped.end, merge_func(enveloped.data, iv[2]))


def print_interval_tree(  # noqa: C901
    tree: IntervalTree,
    *,
    width: int = 80,
    start_from: Any = None,
    end_at: Any = None,
    key_func: Callable[[Interval], Hashable] = lambda iv: iv.data,
    print_func: Callable[..., Any] = print,
    no_interval_char: str = "_",
    none_key_char: str = "^",
    legend: bool = True,
    legend_show_count: bool = False,
    legend_show_keys: bool = False,
):
    """Render an IntervalTree as a fixed-width ASCII timeline with an optional legend."""
    line = [no_interval_char] * width
    char_counter = ord("A")
    chars = {}
    start_from = tree.begin() if start_from is None else start_from
    end_at = tree.end() if end_at is None else end_at
    span = end_at - start_from
    incr = span / width

    if legend:
        left = f"\u250f\u2501\u2501 {start_from}"
        right = f"span: {span}"
        print_func(f"{left} {right:>{width - len(left) - 1}}")

    for i in range(width):
        ivs = tree.at(start_from + i * incr)

        if not ivs:
            continue

        if len(ivs) == 1:
            consensus = key_func(next(iter(ivs)))
        else:
            counts = collections.Counter(key_func(iv) for iv in ivs)
            consensus = counts.most_common(1)[0][0]

        if consensus is not None:
            if consensus not in chars:
                chars[consensus] = chr(char_counter)
                char_counter += 1

            char = chars[consensus]
        else:
            char = none_key_char

        line[i] = char

    print_func("".join(line))

    if legend:
        if legend_show_count:
            ivs = tree.envelop(start_from, end_at)
            count = len(ivs)
            count += len(tree.at(start_from) - ivs)
            count += len(tree.at(end_at) - ivs)

            left = f"intervals: {count}"
        else:
            left = ""

        right = f"{end_at} \u2501\u2501\u251b"
        print_func(f"{left} {right:>{width - len(left) - 1}}")

        if legend_show_keys and chars:
            import textwrap

            for line in textwrap.wrap(" ".join(f"{v}={k}" for k, v in chars.items()), width=width):
                print_func(line)