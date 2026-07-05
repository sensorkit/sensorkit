# SPDX-License-Identifier: Apache-2.0
"""Graph utilities for dependency-aware state election and topological evaluation."""

from __future__ import annotations

import collections
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass

type AdjacencyMap[E: Hashable] = Mapping[E, Sequence[E]]


class StateElection[E: Hashable = str]:
    """Dependency-aware up/down state election."""

    @dataclass
    class VoteTally:
        """Accumulated up and down vote counts for a single election subject."""

        upvotes: int = 0
        downvotes: int = 0

    def __init__(self, dependency_graph: AdjacencyMap[E]):
        self._graph = dependency_graph
        self._votes: dict[str | None, dict[E, bool | None]] = collections.defaultdict(dict)
        self._tally: dict[E, StateElection.VoteTally] = collections.defaultdict(self.VoteTally)

    def vote(self, source: str = None, *, subject: E, vote: bool | None):
        """Set or clear an up/down state vote for the specified subject.

        The `source` parameter specifies a "voter" for this vote. When multiple votes for the same
        subject exist, pessimistic semantics apply: a single downvote overrules any number of
        upvotes.
        """
        votes = self._votes[source]
        prev_vote = votes.get(subject, None)

        if vote is prev_vote:
            return

        tally = self._tally[subject]

        match prev_vote:
            case True:
                tally.upvotes -= 1
            case False:
                tally.downvotes -= 1

        votes[subject] = vote

        match vote:
            case True:
                tally.upvotes += 1
            case False:
                tally.downvotes += 1

    def _evaluate_subject(self, subject: E, output: dict[E, bool], *, dependency: bool = False):
        if subject in output:
            return output[subject]

        tally = self._tally[subject]

        if tally.downvotes > 0:
            # There was at least one direct downvote for this subject, which takes precedence.
            output[subject] = False
        elif tally.upvotes + dependency > 0:
            # There is at least one upvote and/or this subject is a dependency of another. Now
            # check our own dependencies.
            deps_good = True

            if subject in self._graph:
                for other in self._graph[subject]:
                    # Recurse to evaluating the dependency subject.
                    deps_good &= self._evaluate_subject(other, output, dependency=True)

            output[subject] = deps_good
        else:
            return None

        return output[subject]

    def evaluate(self):
        """Return the state election results.

        Returns a dictionary mapping subjects to their evaluated boolean states. The keys of the
        dictionary are in the topological order of the subject dependency graph. Subjects with no
        votes will be absent from the dictionary.
        """
        output: dict[E, bool] = {}

        # Topological sort to evaluate each element's consensus vote with dependencies.
        for subject in self._graph:
            self._evaluate_subject(subject, output)

        return output
