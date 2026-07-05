# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from sensorkit.backend.base import Entity
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
