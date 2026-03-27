from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from sensorkit.backend.base import Entity
from sensorkit.backend.lease import Lease, LeaseModel, LeaseUnavailableError


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
