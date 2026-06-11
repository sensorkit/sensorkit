from __future__ import annotations

import asyncio
import contextlib
import heapq
import os
import random
from datetime import datetime, timedelta
from typing import Callable

from loguru import logger
from pydantic import BaseModel

from sensorkit.backend.base import BackendError, KeyValueContext, RevisionError
from sensorkit.common.aio import scoped_waiter


class LeaseModel[M: BaseModel | None](BaseModel):
    """Serialisable metadata stored in the KV entry for an active lease."""

    acquired_at: datetime
    refreshed_at: datetime
    record: M


class LeaseUnavailableError(BackendError):
    """Raised when a lease cannot be acquired because the key is already held."""


class LeaseAbandonedError(BackendError):
    """Raised when a lease is lost unexpectedly (TTL expiry or external deletion)."""


class Lease[M: BaseModel | None]:
    """A distributed, TTL-backed lock stored in the KV backend.

    A Lease is obtained via Lease.acquire() and must be periodically refreshed (or managed
    via a LeaseGroup) to prevent expiry. It can be used as an async context manager: the lease
    is expired automatically on exit.
    """

    @classmethod
    async def acquire(
        cls,
        kv: KeyValueContext,
        key: str,
        ttl: float,
        record: M,
    ):
        """Attempt to create a new lease at the given KV key with the specified TTL.

        Raises LeaseUnavailableError if the key is already held.
        """
        now = datetime.now()
        model = LeaseModel.model_construct(
            acquired_at=now,
            refreshed_at=now,
            record=record,
        )

        if os.environ.get("SENSORKIT_NO_ENFORCE_LEASES"):
            await kv.delete(key)

        try:
            logger.debug(f"acquiring lease {kv.entity.subject(key)} {record=}")
            entry = await kv.create(
                key=key,
                value=model.model_dump_json().encode(),
                ttl=ttl,
            )
            return cls(kv, key, ttl, entry.revision, model)
        except RevisionError as e:
            raise LeaseUnavailableError() from e

    def __init__(
        self,
        kv: KeyValueContext,
        key: str,
        ttl: float,
        revision: int,
        model: LeaseModel[M],
    ):
        self._kv = kv
        self._key = key
        self.ttl = ttl
        self._revision = revision
        self.model = model
        self.expire_called = False
        self._expired = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task = asyncio.create_task(self._expiry_monitor())

    async def _temp_monitor_lease(self, key: str, *, poll_wait=2.0):
        while True:
            yield await self._kv.get(key)
            await asyncio.sleep(poll_wait)

    async def _expiry_monitor(self):
        if os.environ.get("SENSORKIT_NO_ENFORCE_LEASES"):
            return

        # FIXME: The NATS TTL saga continues: while nats-py v2.13 has added basic support for KV
        #        TTLs, this does not include the ability for watchers to detect KV expiry. So, here
        #        we employ yet another stopgap that polls the lease key instead.
        monitor = self._temp_monitor_lease(self._key)
        # monitor = await self._kv.monitor(self._key)

        try:
            async with asyncio.timeout(self.ttl) as timeout:
                while not self.expired:
                    # Wait for a change to our KV entry.
                    entry = await anext(monitor)

                    if entry.deleted():
                        # We have lost the lease. Fallthrough to ensure we are marked as expired.
                        break

                    # Extend local timeout.
                    timeout.reschedule(asyncio.get_running_loop().time() + self.ttl)
        except TimeoutError:
            logger.warning(f"Abandoning lease for {self._kv.entity.subject(self._key)}!")
        finally:
            self._expired.set()

    async def refresh(self, record: M | None = None):
        """Extend the lease, optionally updating the associated record."""
        if record:
            self.model.record = record

        self.model.refreshed_at = datetime.now()
        entry = await self._kv.update(
            self._key,
            self.model.model_dump_json().encode(),
            ttl=self.ttl,
            revision=self._revision,
        )
        self._revision = entry.revision

    async def expire(self):
        """Expire the lease if it isn't already expired."""
        async with self._lock:
            if not self._expired.is_set():
                self.expire_called = True
                logger.debug(f"expiring lease for {self._kv.entity.subject(self._key)}")

                async with asyncio.timeout(1.0):
                    await self._kv.delete(
                        self._key,
                        revision=self._revision,
                    )

                self._expired.set()
                self._task.cancel()

                with contextlib.suppress(asyncio.CancelledError):
                    await self._task

    @property
    def expired(self):
        """Return True if the lease is expired."""
        return self._expired.is_set()

    def wait_expired(self):
        """Wait until lease expiry."""
        return self._expired.wait()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.expire()


class LeaseGroup:
    """Manage a group of Leases with shared expiration semantics."""

    def __init__(self):
        self._lease_waiters: dict[Lease, asyncio.Task] = {}
        self._refresh_queue: list[tuple[float, Lease]] = []
        self._lease_added = asyncio.Event()

    async def acquire[M: BaseModel | None](
        self,
        kv: KeyValueContext,
        key: str,
        ttl: float,
        record: M,
    ) -> Lease[M]:
        """Acquire a Lease and add it to the maintenance schedule."""
        lease = await Lease.acquire(
            kv,
            key,
            ttl,
            record,
        )
        self._lease_waiters[lease] = asyncio.create_task(lease.wait_expired())
        self._schedule_add(lease)
        self._lease_added.set()
        return lease

    async def refresh_loop(self, record_callback: Callable[[Lease], None] | None = None):
        """Yield Leases as their scheduled refresh time becomes current.

        NOTE: This implementation requires that lease refreshes are performed serially. There are
              potential performance pitfalls with this if using a high-latency backend (which, it
              should be noted, is not a supported use case) or if servicing a large number of
              entities such that a burst of coincident refreshes causes a delay beyond a lease TTL.
              Lease refreshes are scheduled with a random element, which reduces the potential for
              this happening, and TTLs can always be tuned longer. However, a callback-based
              implementation where refreshes can be performed concurrently could be considered.
        """
        try:
            while True:
                lease = self._schedule_pop()

                if lease and not lease.expired:
                    # Generate the user data if a callback was provided.
                    record = record_callback(lease) if record_callback else None

                    # Refresh and reschedule the lease.
                    await lease.refresh(record)
                    self._schedule_add(lease)

                # Wait for a lease to expire, a new lease to be added, or until our next scheduled
                # refresh. If the return value is False, an expiration occurred.
                if not await self._wait_for_event():
                    abandoned = [
                        lease
                        for lease in self._lease_waiters
                        if lease.expired and not lease.expire_called
                    ]
                    break
        finally:
            # Expire all leases in the group on expiration of any lease or on error.
            await self.expire()

        if abandoned:
            keys = ", ".join(lease._key for lease in abandoned)
            raise LeaseAbandonedError(f"Lease(s) lost unexpectedly: {keys}")

    async def expire(self):
        """Expire all leases in the group."""
        leases = tuple(lease for lease in self._lease_waiters.keys() if not lease.expired)
        results = await asyncio.gather(
            *(lease.expire() for lease in leases),
            return_exceptions=True,
        )

        for task in self._lease_waiters.values():
            task.cancel()

        try:
            await asyncio.gather(*self._lease_waiters.values(), return_exceptions=True)
        finally:
            errors = tuple(
                result if isinstance(result, Exception) else RuntimeError("Lease failed to expire")
                for lease, result in zip(leases, results, strict=True)
                if not lease.expired
            )

            if errors:
                raise ExceptionGroup("One or more leases failed to expire", errors)

    async def _wait_for_event(self):
        # Make a copy of the current set of tasks waiting for each lease expiry.
        waiters = list(self._lease_waiters.values())

        # Wait until the next scheduled refresh, or until a lease is acquired or lost.
        max_wait = self._time_until_next_refresh()

        async with scoped_waiter(self._lease_added.wait()) as wait_added:
            expired, _ = await asyncio.wait(
                [wait_added, *waiters], timeout=max_wait, return_when=asyncio.FIRST_COMPLETED
            )
            expired.discard(wait_added)

        self._lease_added.clear()
        return not expired

    def _schedule_pop(self):
        if self._refresh_queue and self._refresh_queue[0][0] <= datetime.now().timestamp():
            _, lease = heapq.heappop(self._refresh_queue)
            return lease

        return None

    def _time_until_next_refresh(self):
        return (
            max(0.0, self._refresh_queue[0][0] - datetime.now().timestamp())
            if self._refresh_queue
            else None
        )

    def _schedule_add(self, lease: Lease):
        delay = lease.ttl * 0.3 + random.random() * lease.ttl * 0.25
        dt = lease.model.refreshed_at + timedelta(seconds=delay)
        heapq.heappush(self._refresh_queue, (dt.timestamp(), lease))
