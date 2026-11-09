# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta

import pytest
from pydantic import BaseModel

from sensorkit.backend.base import Entity, KeyNotFound
from sensorkit.backend.lease import (
    Lease,
    LeaseAbandonedError,
    LeaseGroup,
    LeaseModel,
    LeaseUnavailableError,
)


@pytest.mark.asyncio
async def test_leases(_backend):
    group_name = "test_leases"
    lease_name = "TestLease"
    kv = _backend.key_value(Entity.at(group_name))

    class MyLease(BaseModel):
        some_data: int = 42

    def acquire_lease():
        return Lease.acquire(
            kv,
            lease_name,
            ttl=10.0,
            record=MyLease(),
        )

    async with asyncio.timeout(5.0):
        lease = await acquire_lease()

        async for entry in await kv.monitor(lease_name):
            model = LeaseModel[MyLease].model_validate_json(entry.value)
            assert model.record.some_data == 42
            break

        with pytest.raises(LeaseUnavailableError):
            await acquire_lease()

        await lease.refresh()
        await lease.expire()
        await lease.wait_expired()

        lease = await acquire_lease()

        await kv.delete(lease_name, revision=lease._revision)
        await lease.wait_expired()


@pytest.mark.asyncio
async def test_lease_expire_call(_backend):
    """_intentional is False by default and True only after an explicit expire()."""
    kv = _backend.key_value(Entity.at("test_intentional"))
    lease = await Lease.acquire(kv, "TestLease", ttl=10.0, record=None)

    assert not lease.expire_called

    await lease.expire()

    assert lease.expire_called


@pytest.mark.asyncio
async def test_lease_group_raises_on_external_deletion(_backend):
    """refresh_loop raises LeaseAbandonedError when a lease entry is deleted externally."""
    kv = _backend.key_value(Entity.at("test_abandoned_deletion"))
    group = LeaseGroup()
    lease = await group.acquire(kv, "TestLease", ttl=10.0, record=None)

    async with asyncio.timeout(5.0):
        loop_task = asyncio.create_task(group.refresh_loop())
        await asyncio.sleep(0)

        await kv.delete("TestLease", revision=lease._revision)

        with pytest.raises(LeaseAbandonedError):
            await loop_task


@pytest.mark.asyncio
async def test_lease_group_raises_on_ttl_expiry(_backend):
    """refresh_loop raises LeaseAbandonedError when the local TTL monitor times out."""
    kv = _backend.key_value(Entity.at("test_ttl_expiry"))
    group = LeaseGroup()
    await group.acquire(kv, "TestLease", ttl=0.3, record=None)

    async with asyncio.timeout(5.0):
        with pytest.raises(LeaseAbandonedError):
            await group.refresh_loop()


@pytest.mark.asyncio
async def test_lease_group_no_raise_on_explicit_expire(_backend):
    """refresh_loop returns normally when the group is shut down via expire()."""
    kv = _backend.key_value(Entity.at("test_explicit_expire"))
    group = LeaseGroup()
    await group.acquire(kv, "TestLease", ttl=10.0, record=None)

    async with asyncio.timeout(5.0):
        loop_task = asyncio.create_task(group.refresh_loop())
        await asyncio.sleep(0)

        await group.expire()

        await loop_task  # must complete without raising


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline_offsets", [(0, 0, 0, 0), (0, 1, 1)], ids=["push", "pop"])
async def test_lease_group_equal_refresh_deadlines(_backend, deadline_offsets):
    kv = _backend.key_value(Entity.at("test_equal_refresh_deadlines"))
    group = LeaseGroup()
    leases = {}

    try:
        for index in range(len(deadline_offsets)):
            key = f"lease{index}"
            leases[key] = await group.acquire(kv, key, ttl=4.0, record=None)

        group._refresh_queue.clear()
        deadline = datetime.now() - timedelta(seconds=2)

        for lease, offset in zip(leases.values(), deadline_offsets, strict=True):
            lease.model.refreshed_at = deadline + timedelta(seconds=offset)
            ttl = lease.ttl
            # A zero scheduling TTL removes jitter without changing the backend TTL.
            lease.ttl = 0.0
            try:
                group._schedule_add(lease)
            finally:
                lease.ttl = ttl

        async def wait_for_renewals(key):
            updates = await kv.monitor(key)
            async with contextlib.aclosing(updates):
                renewals = 0
                async for entry in updates:
                    if entry.deleted():
                        continue

                    model = LeaseModel.model_validate_json(entry.value)
                    if model.refreshed_at > model.acquired_at:
                        renewals += 1
                        if renewals == 2:
                            return

        async with asyncio.timeout(10.0):
            async with asyncio.TaskGroup() as tasks:
                renewal_tasks = [tasks.create_task(wait_for_renewals(key)) for key in leases]
                loop_task = tasks.create_task(group.refresh_loop())
                await asyncio.gather(*renewal_tasks)

                assert not loop_task.done()
                assert all(not lease.expired for lease in leases.values())

                await group.expire()
                await loop_task

        for key, lease in leases.items():
            assert lease.expired
            assert lease.expire_called
            with pytest.raises(KeyNotFound):
                await kv.get(key)
    finally:
        await group.expire()
