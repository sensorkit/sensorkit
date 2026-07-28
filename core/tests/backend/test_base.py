# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest

from sensorkit.backend.base import (
    Entity,
    KeyNotFound,
    RemoteRequestError,
    UnregisteredResponder,
)


@pytest.mark.asyncio
async def test_request(_backend):
    request = _backend.request(Entity.at("mydevice"))
    done = asyncio.Event()

    async def cmd_handler(payload: bytes):
        assert payload == b"hello world"
        done.set()
        return b""

    async with asyncio.timeout(1.0):
        await request.handle_request("mycmd", cmd_handler)
        await request.invoke("mycmd", b"hello world")
        await done.wait()


@pytest.mark.asyncio
async def test_request_error(_backend):
    request = _backend.request(Entity.at("mydevice"))

    async def cmd_handler(_: bytes):
        raise RuntimeError("foobar")

    async with asyncio.timeout(1.0):
        with pytest.raises(UnregisteredResponder):
            await request.invoke("mycmd", b"")

        await request.handle_request("mycmd", cmd_handler)

        with pytest.raises(RemoteRequestError):
            await request.invoke("mycmd", b"")


@pytest.mark.asyncio
async def test_request_shutdown(_backend):
    event = asyncio.Event()

    async def handler(_: bytes):
        event.set()
        return b""

    async with asyncio.timeout(1.0):
        req = _backend.request(Entity.at("myentity"))
        await req.handle_request("test", handler)

        await req.invoke("test", b"")
        await event.wait()

        await _backend.shutdown_request_listeners()

        with pytest.raises(UnregisteredResponder):
            await req.invoke("test", b"")


@pytest.mark.asyncio
async def test_stream(_backend):
    stream = _backend.stream(Entity.at("mydevice"))

    async with asyncio.timeout(1.0):
        await stream.publish("mykey", b"event payload")
        consumer = await stream.consume("mykey")
        await stream.publish("mykey", b"event payload2")
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey"
        assert msg.data == b"event payload2"


@pytest.mark.asyncio
async def test_stream_with_latest(_backend):
    stream = _backend.stream(Entity.at("mydevice"))

    async with asyncio.timeout(1.0):
        await stream.publish("mykey", b"event payload")
        await stream.publish("mykey", b"event payload2")
        consumer = await stream.consume("mykey", include_latest=True)
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey"
        assert msg.data == b"event payload2"


@pytest.mark.asyncio
async def test_stream_with_recall(_backend):
    stream = _backend.stream(Entity.at("mydevice"))

    async with asyncio.timeout(1.0):
        await stream.publish("mykey", b"foobar")
        await stream.publish("mykey", b"event payload")
        await stream.publish("mykey", b"event payload2")
        consumer = await stream.consume("mykey", from_sequence=2)
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey"
        assert msg.data == b"event payload"
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey"
        assert msg.data == b"event payload2"


@pytest.mark.asyncio
async def test_stream_with_wildcard(_backend):
    stream = _backend.stream(Entity.at("mydevice"))

    async with asyncio.timeout(1.0):
        consumer = await stream.consume()
        await stream.publish("mykey1", b"event payload")
        await stream.publish("mykey2", b"event payload2")

    async with asyncio.timeout(1.0):
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey1"
        assert msg.data == b"event payload"

    async with asyncio.timeout(1.0):
        msg = await anext(consumer)
        assert msg.subject.prop == "mykey2"
        assert msg.data == b"event payload2"


@pytest.mark.asyncio
async def test_kv(_backend):
    key_value = _backend.key_value(Entity.at("mydevice"))

    async with asyncio.timeout(1.0):
        entry = await key_value.create(
            key="testkey",
            value=b"initial value",
        )
        assert entry.key.prop == "testkey"
        assert entry.value == b"initial value"
        assert entry.revision == 1

        retrieved = await key_value.get("testkey")
        assert retrieved.key.prop == "testkey"
        assert retrieved.value == b"initial value"
        assert retrieved.revision == 1

        updated = await key_value.update(
            key="testkey",
            value=b"updated value",
            revision=entry.revision,
        )
        assert updated.key.prop == "testkey"
        assert updated.value == b"updated value"
        assert updated.revision == 2

        retrieved_updated = await key_value.get("testkey")
        assert retrieved_updated.value == b"updated value"
        assert retrieved_updated.revision == 2

        await key_value.delete("testkey")

        with pytest.raises(KeyNotFound):
            await key_value.get("testkey")


@pytest.mark.asyncio
async def test_kv_monitor(_backend):
    key_value = _backend.key_value(Entity.at("mydevice"))
    ready = asyncio.Event()

    async def monitor_task():
        count = 0
        monitor = await key_value.monitor("mykey")
        ready.set()

        async for ent in monitor:
            match count:
                case 0:
                    assert ent.key.prop == "mykey"
                    assert ent.value == b"kv payload"
                case 1:
                    assert ent.key.prop == "mykey"
                    assert ent.value == b"kv payload2"
                    return

            count += 1

    task = asyncio.create_task(monitor_task())

    async with asyncio.timeout(1.0):
        await ready.wait()
        entry = await key_value.create(
            key="mykey",
            value=b"kv payload",
        )
        await key_value.update(
            key="mykey",
            value=b"kv payload2",
            revision=entry.revision,
        )
        await task


@pytest.mark.asyncio
async def test_kv_monitor_all_existing(_backend):
    await _backend.key_value(Entity.at("mydevice", "test")).create(key="nomatch", value=b"")

    key_value = _backend.key_value(Entity.at("mydevice"))
    await key_value.create(key="key_a", value=b"value_a")
    await key_value.create(key="key_b", value=b"value_b")

    async with asyncio.timeout(2.0):
        monitor = await key_value.monitor_all()

        entry = await anext(monitor)
        assert entry.key.prop == "key_a"
        assert entry.value == b"value_a"

        entry = await anext(monitor)
        assert entry.key.prop == "key_b"
        assert entry.value == b"value_b"

        await key_value.update(key="key_b", value=b"value_c", revision=entry.revision)

        entry = await anext(monitor)
        assert entry.key.prop == "key_b"
        assert entry.value == b"value_c"


@pytest.mark.asyncio
async def test_kv_monitor_all_deep(_backend):
    """monitor_all(deep=True) returns keys across nested entities."""
    parent = _backend.key_value(Entity.at("parent"))
    child = _backend.key_value(Entity.at("parent", "child"))

    await parent.create(key="pk", value=b"parent_val")
    await child.create(key="ck", value=b"child_val")

    async with asyncio.timeout(2.0):
        monitor = await parent.monitor_all(deep=True)

        seen = {}
        async for entry in monitor:
            seen[entry.value] = True
            if len(seen) == 2:
                break

    assert b"parent_val" in seen
    assert b"child_val" in seen


@pytest.fixture
def ttl_expiry_delivery(_backend, request):
    """Expect failure on backends that never deliver the TTL expiry to a watcher.

    FIXME: Drop once nats-py publishes KV deletes on TTL expiry. The mark is
    strict, so the test starts failing as soon as that lands.
    """
    from sensorkit.backend.nats import NATSBackendImpl

    if isinstance(_backend.impl, NATSBackendImpl):
        request.applymarker(
            pytest.mark.xfail(
                reason="nats-py does not deliver KV TTL expiry to watchers",
                strict=True,
            )
        )


@pytest.mark.asyncio
async def test_kv_ttl(_backend, ttl_expiry_delivery):
    key_value = _backend.key_value(Entity.at("mydevice"))

    async def monitor(ready: asyncio.Event):
        stream = await key_value.monitor("ttl_key")
        ready.set()

        async for entry in stream:
            if entry.deleted():
                break

    async with asyncio.timeout(2.0):
        ready = asyncio.Event()
        monitor_task = asyncio.create_task(monitor(ready))
        await ready.wait()

        await key_value.create(
            key="ttl_key",
            value=b"expiring value",
            ttl=0.5,
        )

        # Verify key exists immediately after creation
        entry = await key_value.get("ttl_key")
        assert entry is not None
        assert entry.value == b"expiring value"

        # Wait for TTL to expire
        await monitor_task

        # Verify key no longer exists
        with pytest.raises(KeyNotFound):
            await key_value.get("ttl_key")
