# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel, ValidationError

from sensorkit.backend.base import Entity, RevisionError, SpecialProperty, Subject
from sensorkit.backend.fake import FakeBackendImpl

ENTITY = Entity.at("mydevice")
SUBJECT = ENTITY.subject("mykey")


class Gauge(BaseModel):
    """Model monitored from the KV store."""

    level: int


@pytest.fixture
def fake(backend) -> FakeBackendImpl:
    """Skip registry assertions when the configured backend is not the fake."""
    if not isinstance(backend.impl, FakeBackendImpl):
        pytest.skip("asserts the subscriber registry of the in-memory FakeBackendImpl broker")

    return backend.impl


def subscriptions(fake: FakeBackendImpl, subject: Subject) -> int:
    """Count registered queues for a subject in the fake broker."""
    match subject.prop:
        case SpecialProperty.ALL_PROPERTIES:
            return len(fake._kv.element(subject.path).shallow_wildcard)
        case SpecialProperty.ALL_DESCENDANTS:
            return len(fake._kv.element(subject.path).deep_wildcard)
        case _:
            return len(fake._kv.element(subject.full_path()).direct)


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_first", [False, True])
async def test_unconditional_kv_write_advances_revision(backend, delete_first):
    kv = backend.key_value(ENTITY)
    original = await kv.create("mykey", b"original")

    if delete_first:
        await kv.delete("mykey")

    replacement = await kv.update("mykey", b"replacement")
    assert replacement.revision == original.revision + (2 if delete_first else 1)

    with pytest.raises(RevisionError):
        await kv.update("mykey", b"stale", revision=original.revision)

    with pytest.raises(RevisionError):
        await kv.delete("mykey", revision=original.revision)

    assert await kv.get("mykey") == replacement


# KV monitor release


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("prop", "existing"),
    [
        ("mykey", True),
        ("mykey", False),
        (SpecialProperty.ALL_PROPERTIES, True),
        (SpecialProperty.ALL_DESCENDANTS, True),
    ],
)
async def test_kv_monitor_releases_after_initial_batch(backend, fake, prop, existing):
    subject = ENTITY.subject(prop)

    if existing:
        await backend.key_value(ENTITY).create(key="mykey", value=b"value")

    batches = await fake.kv_monitor(subject)

    async with asyncio.timeout(1.0):
        initial = await anext(batches)

    assert len(initial) == int(existing)
    assert subscriptions(fake, subject) == 1

    await batches.aclose()

    assert subscriptions(fake, subject) == 0


@pytest.mark.asyncio
async def test_key_monitor_releases_after_initial_entry(backend, fake):
    key_value = backend.key_value(ENTITY)
    await key_value.create(key="mykey", value=b"value")
    monitor = await key_value.monitor("mykey")

    async with asyncio.timeout(1.0):
        assert (await anext(monitor)).value == b"value"

    await monitor.aclose()

    assert subscriptions(fake, SUBJECT) == 0


@pytest.mark.asyncio
async def test_key_monitor_releases_after_live_entry(backend, fake):
    key_value = backend.key_value(ENTITY)
    entry = await key_value.create(key="mykey", value=b"value")
    monitor = await key_value.monitor("mykey")

    async with asyncio.timeout(1.0):
        assert (await anext(monitor)).value == b"value"
        await key_value.update(key="mykey", value=b"update", revision=entry.revision)
        assert (await anext(monitor)).value == b"update"

    await monitor.aclose()

    assert subscriptions(fake, SUBJECT) == 0


# Typed client release


@pytest.mark.asyncio
async def test_kv_monitor_model_releases_after_initial_yield(kit, fake):
    client = kit.entity("gauge")
    subject = client.entity.subject("Gauge")
    await client.kv_put_model(Gauge(level=1))
    gauges = client.kv_monitor_model(Gauge)

    async with asyncio.timeout(1.0):
        assert await anext(gauges) == Gauge(level=1)

    assert subscriptions(fake, subject) == 1

    await gauges.aclose()

    assert subscriptions(fake, subject) == 0


@pytest.mark.asyncio
async def test_kv_monitor_model_releases_after_strict_failure(kit, fake):
    client = kit.entity("gauge")
    subject = client.entity.subject("Gauge")
    await kit.backend.key_value(client.entity).create(key="Gauge", value=b"{}")
    gauges = client.kv_monitor_model(Gauge, strict=True)

    async with asyncio.timeout(1.0):
        with pytest.raises(ValidationError):
            await anext(gauges)

    assert subscriptions(fake, subject) == 0
