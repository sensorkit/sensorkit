# SPDX-License-Identifier: Apache-2.0
"""External abort, delivered as task cancellation.

Its own module because `AbortSignal._absorb` is the designated integration seam:
the whole of the cancel-counting protocol lives in that one method, and a host
framework with its own cancel-counting task wrapper replaces it there and nowhere
else. Nothing here knows about graphs, devices or phases.
"""

from __future__ import annotations

import asyncio


class AbortSignal:
    """A domain abort (weather, e-stop), delivered as task cancellation.

    `fire()` cancels the task executing the run, and is single-use: once fired, an
    `AbortSignal` aborts any run it is given, including one that has not started.
    The run absorbs that cancellation — returning a report with `aborted=True` —
    and lets every other cancellation propagate, so an abort is a reportable
    outcome while a shutdown stays a shutdown. Op code needs no cooperation to be
    abortable; if it wants to know why it is being cancelled it consults this
    object from its `CancelledError` handler (`ctx.run.abort`) rather than polling
    it.

    Telling one's own cancellation from another's is the cancel-count protocol of
    `asyncio.timeout`: note `Task.cancelling()` before adding our own cancel, and
    on the way out `uncancel()`. If the count comes back to what it was, ours was
    the only one outstanding and the run may report; if it does not, someone else's
    cancellation is still owed and must be re-raised. Counting at fire time rather
    than at scope entry keeps a cancel/uncancel cycle that happened in between (an
    inner timeout, say) from being mistaken for ours.
    """

    def __init__(self) -> None:
        self.reason: str | None = None
        self._task: asyncio.Task | None = None
        self._count = 0            # the run task's cancel count before ours

    @property
    def fired(self) -> bool:
        return self.reason is not None

    def fire(self, reason: str = "abort") -> None:
        """Abort the run this signal is bound to.

        Idempotent, and safe before the run starts — it is delivered on entry
        instead.
        """
        if self.fired:
            return

        self.reason = reason

        if self._task is not None:
            self._cancel()

    # The scope below is entered by DagRunner.execute.

    def _cancel(self) -> None:
        self._count = self._task.cancelling()
        self._task.cancel()

    def _enter(self) -> None:
        if self._task is not None:
            raise RuntimeError("AbortSignal is already bound to a running graph")

        self._task = asyncio.current_task()

        if self.fired:
            self._cancel()         # fired before the run got going

    def _exit(self) -> None:
        self._task = None

    def _absorb(self) -> bool:
        """Whether this abort is the only cancellation outstanding, and the run may
        therefore report instead of propagating. Called while handling a
        `CancelledError`."""
        return self.fired and self._task.uncancel() <= self._count
