# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import socket

import httpx
import pytest
import pytest_asyncio
import uuid_utils.compat as uuid
from pydantic import TypeAdapter

from sensorkit.core.device import Abort
from sensorkit.core.task import CollectTask, InitTask
from sensorkit.webapi.fastapi import WebAPI, WebAPIConfig
from sensorkit.webapi.forwarder import SKRecord


@pytest_asyncio.fixture
async def webapi_setup(kit, service_context):
    """Stand up a minimal sensorkit system and return (kit, webapi, http_client)."""
    dev = await service_context.register_device("mydevice")
    controller = await service_context.register_controller("mycontroller")
    program = await service_context.register_program("myprogram")

    config = WebAPIConfig()  # default agent="agent"; entity not registered → backend errors
    webapi = WebAPI(kit, config)

    async with asyncio.TaskGroup() as tg:
        await webapi.kv_forwarder.start(task_group=tg)
        await webapi.stream_forwarder.start(task_group=tg)

        transport = httpx.ASGITransport(app=webapi.app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")

        yield kit, service_context, dev, controller, program, webapi, client

        await client.aclose()
        await webapi.shutdown()


@pytest_asyncio.fixture
async def http(webapi_setup):
    """Shortcut: return just the httpx client."""
    _, _, _, _, _, _, client = webapi_setup
    return client


# ---------------------------------------------------------------------------
# Global endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_empty(http):
    resp = await http.get("/data/snapshot")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_snapshot_after_publish(webapi_setup):
    _, sc, dev, _, _, webapi, client = webapi_setup

    await dev.publish_entity_info()

    # Wait until the forwarder has cached the EntityInfo record specifically.
    for _ in range(20):
        if "EntityInfo" in webapi.kv_forwarder.cache.get("mydevice", {}):
            break
        await asyncio.sleep(0.05)

    data = (await client.get("/data/snapshot")).json()
    assert len(data) > 0

    ta = TypeAdapter(SKRecord)
    assert any(ta.validate_python(r).subject.prop == "EntityInfo" for r in data)


@pytest.mark.asyncio
async def test_entity_snapshot_by_id(webapi_setup):
    _, sc, dev, _, _, webapi, client = webapi_setup

    await dev.publish_entity_info()

    for _ in range(20):
        if "mydevice" in webapi.kv_forwarder.cache:
            break
        await asyncio.sleep(0.05)

    resp = await client.get("/data/snapshot/mydevice")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_entity_snapshot_unknown_returns_empty(http):
    resp = await http.get("/data/snapshot/nonexistent")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_get_entities(webapi_setup):
    _, sc, dev, _, _, webapi, client = webapi_setup

    await dev.publish_entity_info()

    for _ in range(20):
        if "EntityInfo" in webapi.kv_forwarder.cache.get("mydevice", {}):
            break
        await asyncio.sleep(0.05)

    resp = await client.get("/entities")
    assert resp.status_code == 200
    names = [e["name"] for e in resp.json()]
    assert "mydevice" in names


# ---------------------------------------------------------------------------
# Device endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_device_state(webapi_setup):
    _, _, dev, _, _, _, client = webapi_setup

    resp = await client.get("/device/mydevice/state")
    assert resp.status_code == 200
    body = resp.json()
    assert "enable_state" in body


@pytest.mark.asyncio
async def test_run_device_command(webapi_setup):
    _, _, dev, _, _, _, client = webapi_setup

    done = asyncio.Event()

    @dev.command_handler(Abort)
    async def handle_abort(cmd: Abort):
        done.set()

    resp = await client.post(
        "/device/mydevice/command",
        json={"command_id": "Abort"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    async with asyncio.timeout(3.0):
        await done.wait()


@pytest.mark.asyncio
async def test_run_device_command_unknown_device(http):
    resp = await http.post(
        "/device/unknown/command",
        json={"command_id": "Abort"},
    )
    assert resp.status_code in (404, 503)


# ---------------------------------------------------------------------------
# Controller endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_controller_state(webapi_setup):
    _, _, _, controller, _, _, client = webapi_setup

    resp = await client.get("/controller/mycontroller/state")
    assert resp.status_code == 200
    body = resp.json()
    assert "enable_state" in body
    assert "execution_state" in body


@pytest.mark.asyncio
async def test_execute_controller_task(webapi_setup):
    _, _, _, controller, _, _, client = webapi_setup

    done = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle(task: InitTask):
        done.set()

    resp = await client.post(
        "/controller/mycontroller/execute",
        json={"task_type": "init"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    # The controller mints the task_id; the response reports the minted value.
    assert uuid.UUID(body["task_id"])

    async with asyncio.timeout(3.0):
        await done.wait()


@pytest.mark.asyncio
async def test_execute_controller_task_with_submission_envelope(webapi_setup):
    """A TaskSubmission envelope threads context and expiry_time onto the minted execution."""
    _, _, _, controller, _, _, client = webapi_setup

    seen: dict[str, object] = {}
    done = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle(task: InitTask):
        # The controller associates the execution before invoking the handler, so the
        # client-supplied context and expiry are reachable here.
        seen["context"] = dict(task.execution.context)
        seen["expiry_time"] = task.execution.expiry_time
        done.set()

    resp = await client.post(
        "/controller/mycontroller/execute",
        json={
            "task": {"task_type": "init"},
            "context": {"program_name": "skyview", "FileNameTemplate": "{target}_{time}"},
            "expiry_time": "2026-01-01T00:00:00Z",
        },
    )
    assert resp.status_code == 200
    assert uuid.UUID(resp.json()["task_id"])

    async with asyncio.timeout(3.0):
        await done.wait()

    # The controller injects a TaskInfo keyword alongside the client-supplied context.
    assert seen["context"]["program_name"] == "skyview"
    assert seen["context"]["FileNameTemplate"] == "{target}_{time}"
    assert seen["expiry_time"] is not None


@pytest.mark.asyncio
async def test_abort_controller_task(webapi_setup):
    _, _, _, controller, _, _, client = webapi_setup

    ready = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def long_task(_: InitTask):
        ready.set()
        await can_finish.wait()

    # Fire execute in the background — it blocks until the task completes.
    exec_bg = asyncio.create_task(
        client.post(
            "/controller/mycontroller/execute",
            json={"task_type": "init", "controller_id": "mycontroller", "task_id": str(uuid.uuid7())},
        )
    )

    async with asyncio.timeout(3.0):
        await ready.wait()

    resp = await client.post("/controller/mycontroller/abort")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    # Unblock the handler. The execute call raises CallError when the task is aborted;
    # with ASGI transport that exception propagates through httpx rather than becoming a 500.
    can_finish.set()
    with contextlib.suppress(Exception):
        await exec_bg


@pytest.mark.asyncio
async def test_wait_for_current_task(webapi_setup):
    _, _, _, controller, _, _, client = webapi_setup

    ready = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle(_: InitTask):
        ready.set()

        async with asyncio.timeout(3.0):
            await can_finish.wait()

    exec_bg = asyncio.create_task(
        client.post(
            "/controller/mycontroller/execute",
            json={"task_type": "init", "controller_id": "mycontroller", "task_id": str(uuid.uuid7())},
        )
    )

    async with asyncio.timeout(3.0):
        await ready.wait()

        can_finish.set()

        resp = await client.post("/controller/mycontroller/wait")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        with contextlib.suppress(Exception):
            await exec_bg


@pytest.mark.asyncio
async def test_wait_for_task_by_id(webapi_setup):
    _, _, _, controller, _, _, client = webapi_setup

    ready = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle(_: InitTask):
        ready.set()

        async with asyncio.timeout(3.0):
            await can_finish.wait()

    task_id = str(uuid.uuid7())
    exec_bg = asyncio.create_task(
        client.post(
            "/controller/mycontroller/execute",
            json={"task_type": "init", "controller_id": "mycontroller", "task_id": task_id},
        )
    )

    async with asyncio.timeout(3.0):
        await ready.wait()

        can_finish.set()

        resp = await client.post(f"/controller/mycontroller/wait/{task_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        with contextlib.suppress(Exception):
            await exec_bg


# ---------------------------------------------------------------------------
# Program endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_program_state(webapi_setup):
    _, _, _, _, program, _, client = webapi_setup

    resp = await client.get("/program/myprogram/state")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_enable_disable_program(webapi_setup):
    _, _, _, controller, program, _, client = webapi_setup

    enabled = asyncio.Event()
    disabled = asyncio.Event()

    @program.on_enable
    async def on_enable():
        enabled.set()

    @program.on_disable
    async def on_disable():
        disabled.set()

    resp = await client.post("/program/myprogram/enable?controller_id=mycontroller")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    async with asyncio.timeout(3.0):
        await enabled.wait()

    resp = await client.post("/program/myprogram/disable")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    async with asyncio.timeout(3.0):
        await disabled.wait()


@pytest.mark.asyncio
async def test_enable_program_requires_controller_id(http):
    resp = await http.post("/program/myprogram/enable")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_activate_deactivate_program(webapi_setup):
    kit, _, _, controller, program, _, client = webapi_setup

    task_done = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def handle(_: CollectTask):
        task_done.set()

    @program.task_factory
    async def factory():
        yield CollectTask(
            task_id=uuid.uuid7(),
            controller_id="mycontroller",
        )

    await client.post("/program/myprogram/enable?controller_id=mycontroller")
    await kit.controller("mycontroller").enable()

    resp = await client.post("/program/myprogram/activate")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    async with asyncio.timeout(3.0):
        await task_done.wait()

    resp = await client.post("/program/myprogram/deactivate")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("path", [
    "/device/unknown/state",
    "/controller/unknown/state",
    "/program/unknown/state",
])
async def test_get_state_unknown_entity_returns_404(http, path):
    resp = await http.get(path)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Agent endpoints — no agent registered → 503
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_state_no_agent(http):
    # "agent" entity is not registered → KeyNotFound → 404
    resp = await http.get("/agent/state")
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("path,kwargs", [
    ("/agent/enable", {}),
    ("/agent/disable", {}),
    ("/agent/override/mycontroller", {"json": {"state": True}}),
    ("/agent/scheduler/enable", {}),
    ("/agent/scheduler/disable", {}),
    ("/agent/enable/mycontroller", {}),
    ("/agent/disable/mycontroller", {}),
    ("/agent/scheduler/include/myprogram", {}),
    ("/agent/scheduler/exclude/myprogram", {}),
])
async def test_agent_action_no_agent_returns_503(http, path, kwargs):
    resp = await http.post(path, **kwargs)
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def free_port() -> int:
    """Return a port that is free right now, for the server to bind."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def get_when_serving(client: httpx.AsyncClient, path: str) -> httpx.Response:
    """Poll a path until the server accepts connections, then return the response."""
    while True:
        try:
            return await client.get(path)
        except httpx.ConnectError:
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_shutdown_unwinds_the_running_server(kit):
    """Shutting down has to let `serve` return on its own.

    The task group exit is the assertion; a server left running makes it hang, so the
    timeout is what turns that into a failure.
    """
    port = free_port()
    webapi = WebAPI(kit, WebAPIConfig(port=port))

    async with asyncio.timeout(30):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(webapi.serve(task_group=tg))

            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
                response = await get_when_serving(client, "/data/snapshot")
                assert response.status_code == 200

            await webapi.shutdown()


@pytest.mark.asyncio
async def test_shutdown_before_serve_keeps_the_server_down(kit):
    """A shutdown that beats the serve task to the loop must stop it from starting."""
    port = free_port()
    webapi = WebAPI(kit, WebAPIConfig(port=port))

    async with asyncio.timeout(30):
        async with asyncio.TaskGroup() as tg:
            await webapi.shutdown()

            # Stands in for a serve task that is only scheduled after the shutdown; an
            # unguarded one binds the port and runs until something cancels it.
            await webapi.serve(task_group=tg)

    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("/data/snapshot")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_config_survives_a_key_value_round_trip(kit):
    config = WebAPIConfig()
    entity = kit.entity("webapi")

    await entity.kv_put_model(config)

    assert await entity.kv_get_model(WebAPIConfig) == config
