import asyncio
import contextlib
import itertools
from typing import Any

import uuid_utils.compat as uuid
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.sse import EventSourceResponse, ServerSentEvent
from loguru import logger
from pydantic import BaseModel, Field, model_validator

import sensorkit.api as sk
from sensorkit.auto.agent import AgentConfigureRequest, AgentState, agent_configure_request
from sensorkit.backend.base import (
    BackendError,
    KeyNotFound,
    RemoteRequestError,
    UnregisteredResponder,
)
from sensorkit.core.controller import ControllerState
from sensorkit.core.device import DeviceState
from sensorkit.core.entity import DeviceDetails, EntityInfo
from sensorkit.core.program import ProgramState
from sensorkit.webapi.forwarder import (
    KeyValueForwarder,
    ProductForwarder,
    RecordQueueSet,
    SKRecord,
    StreamForwarder,
)
from sensorkit.webapi.preview import PreviewJPEG
from sensorkit.webapi.schema import add_sensorkit_schema
from sensorkit.webapi.serve import ProductInfo, ServeDataConfig, ServeHandler

# Seconds a product-listing request waits for a handler's initial scan to finish
# before giving up with a 503. See list_controller_products.
PRODUCT_LISTING_TIMEOUT = 10.0


class WebAPIConfig(BaseModel):
    """Configuration for the WebAPI service."""

    port: int = 8000
    host: str = "0.0.0.0"
    agent: sk.EntityRef = sk.EntityRef("agent")
    serve_data_products: list[ServeDataConfig] = []


class AgentOverrideRequest(BaseModel):
    """Request body for controller demand override."""

    state: bool | None


class EntityListing(EntityInfo):
    """Entity listing response."""

    name: str
    online: bool


class DeviceListing(EntityInfo):
    """Device listing response."""

    name: str
    online: bool
    archetype: str | None = None
    traits: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _populate_from_details(self):
        if isinstance(self.details, DeviceDetails):
            self.archetype = self.details.archetype.name if self.details.archetype else None
            self.traits = [trait.name for trait in self.details.traits]

        return self


def _status_ok(**kwargs) -> dict[str, Any]:
    return {"status": "ok", **kwargs}


@contextlib.contextmanager
def _error_handler():
    try:
        yield
    except KeyNotFound as err:
        raise HTTPException(status_code=404, detail="Requested entity not found") from err
    except RemoteRequestError as err:
        raise HTTPException(status_code=500, detail="Remote exception occurred") from err
    except UnregisteredResponder as err:
        raise HTTPException(status_code=503, detail="Endpoint unavailable") from err
    except BackendError as err:
        raise HTTPException(status_code=503, detail="Backend not available") from err
    except sk.CallError as err:
        raise HTTPException(status_code=409, detail=str(err)) from err


class WebAPI:
    """
    FastAPI-based web service for SensorKit.

    Provides REST API endpoints for interacting with devices, controllers, programs,
    and agents, as well as Server-Sent Events (SSE) for real-time state updates.
    """

    def __init__(self, kit: sk.SensorKit, config: WebAPIConfig):
        self.kit = kit
        self.config = config
        self.client_queues: RecordQueueSet = set()
        self.kv_forwarder = KeyValueForwarder(kit, targets=self.client_queues)
        self.stream_forwarder = StreamForwarder(kit, targets=self.client_queues)
        self._serve_handlers: list[ServeHandler] = []
        self._product_forwarders: list[ProductForwarder] = []
        self.app = self._create_fastapi_app()
        self.server = uvicorn.Server(
            uvicorn.Config(
                self.app,
                host=self.config.host,
                port=self.config.port,
                log_level="info",
            )
        )

        # Resolve the configured agent entity reference.
        self.config.agent.resolve(kit)

    def _create_fastapi_app(self):  # noqa: C901
        app = FastAPI(title="SensorKit Web API")

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Global

        @app.get("/data/subscribe", tags=["Global"], response_class=EventSourceResponse)
        async def firehose_subscription(request: Request):
            """SSE endpoint for real-time updates."""
            request.state.is_sse = True
            queue = asyncio.Queue()
            initial_updates = itertools.chain(
                self.kv_forwarder.snapshot(),
                self.stream_forwarder.snapshot(),
            )
            self.client_queues.add(queue)

            try:
                # Send initial snapshot
                for upd in initial_updates:
                    yield ServerSentEvent(raw_data=upd.serialize())

                while True:
                    upd = await queue.get()

                    if upd is None:
                        break

                    yield ServerSentEvent(raw_data=upd.serialize())
                    queue.task_done()
            finally:
                self.client_queues.remove(queue)

        @app.get("/data/snapshot", tags=["Global"])
        async def get_full_system_snapshot() -> list[SKRecord]:
            """Return the all cached data for all entities."""
            return list(
                itertools.chain(
                    self.kv_forwarder.snapshot(),
                    self.stream_forwarder.snapshot(),
                )
            )

        @app.get("/data/snapshot/{entity_id}", tags=["Global"])
        async def get_entity_snapshot(entity_id: str):
            """Return all cached data for a specific entity."""
            return list(
                itertools.chain(
                    self.kv_forwarder.cache.get(entity_id, {}).values(),
                    self.stream_forwarder.cache.get(entity_id, {}).values(),
                )
            )

        @app.get("/entities", tags=["Global"])
        async def get_entity_list() -> list[EntityListing | DeviceListing]:
            """Return a list of all entity IDs."""
            output = []
            cache = self.kv_forwarder.cache
            entities = tuple(cache.keys())

            for entity in entities:
                if record := cache[entity].get("EntityInfo"):
                    if record.payload is None:
                        continue

                    info = record.payload.copy()
                    info["name"] = entity
                    info["online"] = "EntityLease" in cache[entity]

                    if info["entity_type"] == "device":
                        output.append(DeviceListing.model_validate(info))
                    else:
                        output.append(EntityListing.model_validate(info))

            return output

        # Device

        @app.get("/device/{device_id}/state", tags=["Device"])
        async def get_device_state(device_id: str) -> DeviceState:
            """Return the current state of a device."""
            with _error_handler():
                return await self.kit.device(device_id).kv_get_model(DeviceState)

        @app.post("/device/{device_id}/command", tags=["Device"])
        async def run_device_command(device_id: str, command: sk.DeviceCommand):
            """Execute a command on a device."""
            with _error_handler():
                await self.kit.device(device_id).command(command)

            return _status_ok()

        # Controller

        @app.get("/controller/{controller_id}/state", tags=["Controller"])
        async def get_controller_state(controller_id: str) -> ControllerState:
            """Return the current state of a controller."""
            with _error_handler():
                return await self.kit.controller(controller_id).kv_get_model(ControllerState)

        @app.post("/controller/{controller_id}/execute", tags=["Controller"])
        async def run_controller_task(controller_id: str, task: sk.ControllerTask):
            """Execute a task on a controller."""
            # Ensure task has ID and controller_id if not provided
            if not task.task_id:
                task.task_id = uuid.uuid7()

            if not task.controller_id:
                task.controller_id = controller_id

            with _error_handler():
                await self.kit.controller(controller_id).execute_task(task)

            return _status_ok(task_id=task.task_id)

        @app.post("/controller/{controller_id}/wait", tags=["Controller"])
        async def wait_for_current_task(controller_id: str):
            """Wait for the currently executing task to complete on a controller."""
            with _error_handler():
                await self.kit.controller(controller_id).wait_for_task()

            return _status_ok()

        @app.post("/controller/{controller_id}/wait/{task_id}", tags=["Controller"])
        async def wait_for_task(controller_id: str, task_id: uuid.UUID):
            """Wait for a task to complete on a controller."""
            with _error_handler():
                await self.kit.controller(controller_id).wait_for_task(task_id)

            return _status_ok()

        @app.post("/controller/{controller_id}/abort", tags=["Controller"])
        async def abort_controller_task(controller_id: str, task_id: uuid.UUID | None = None):
            """Abort a specific task or the current task on a controller."""
            with _error_handler():
                await self.kit.controller(controller_id).abort_task(task_id)

            return _status_ok()

        @app.get("/controller/{controller_id}/products", tags=["Controller"])
        async def list_controller_products(controller_id: str) -> list[ProductInfo]:
            """List data products associated with a controller.

            A handler's listing may not be ready immediately. Rather than report an empty
            listing — which would falsely imply the controller has no products — we wait
            up to PRODUCT_LISTING_TIMEOUT and return 503 if it is still not available.
            """
            try:
                async with asyncio.timeout(PRODUCT_LISTING_TIMEOUT):
                    listings = [await handler.get_listing() for handler in self._serve_handlers]
            except TimeoutError as err:
                raise HTTPException(
                    status_code=503,
                    detail="Data product listing not yet available; try again shortly",
                ) from err

            return [
                info
                for listing in listings
                for info in listing
                if info.controller_id == controller_id
            ]

        @app.get("/controller/{controller_id}/products/subscribe", tags=["Controller"], response_class=EventSourceResponse)
        async def subscribe_controller_products(request: Request, controller_id: str):
            """SSE stream of product arrivals for a controller, including initial listing."""
            request.state.is_sse = True
            queue: asyncio.Queue[SKRecord | None] = asyncio.Queue()
            initial_records = [
                record
                for forwarder in self._product_forwarders
                for record in forwarder.snapshot()
                if str(record.subject.entity()) == controller_id
            ]
            self.client_queues.add(queue)

            try:
                for record in initial_records:
                    yield ServerSentEvent(raw_data=record.serialize())

                while True:
                    record = await queue.get()

                    if record is None:
                        break

                    try:
                        if record.kind == "product" and str(record.subject.entity()) == controller_id:
                            yield ServerSentEvent(raw_data=record.serialize())
                    finally:
                        queue.task_done()
            except asyncio.CancelledError:
                host = request.client.host if request.client else "unknown"
                logger.debug(f"SSE client disconnected: {host}")
            finally:
                self.client_queues.remove(queue)

        @app.get("/controller/{controller_id}/product/{product_id}/data", tags=["Controller"])
        async def get_controller_product_data(controller_id: str, product_id: str):
            """Return the raw FITS data for a product."""
            for handler in self._serve_handlers:
                if handler.has_product(controller_id, product_id):
                    raw_bytes = await handler.get_data(controller_id, product_id)
                    return Response(content=raw_bytes, media_type="application/fits")

            raise HTTPException(status_code=404, detail="Product not found")

        @app.get("/controller/{controller_id}/product/{product_id}/preview", tags=["Controller"])
        async def get_controller_product_preview(controller_id: str, product_id: str):
            """Return a JPEG preview image generated from the FITS product data."""
            for handler in self._serve_handlers:
                if handler.has_product(controller_id, product_id):
                    data = await handler.get_data(controller_id, product_id)

                    try:
                        preview = await PreviewJPEG.from_fits(data)
                    except Exception as err:
                        raise HTTPException(status_code=422, detail=f"Could not generate preview: {err}") from err

                    return Response(content=preview.jpeg_bytes, media_type="image/jpeg")

            raise HTTPException(status_code=404, detail="Product not found")

        @app.get("/controller/{controller_id}/product/{product_id}/metadata", tags=["Controller"])
        async def get_controller_product_metadata(controller_id: str, product_id: str) -> dict:
            """Return the cached metadata for a product as a JSON object."""
            for handler in self._serve_handlers:
                if handler.has_product(controller_id, product_id):
                    metadata = handler.get_metadata(controller_id, product_id)

                    if not metadata.get(ProductInfo):
                        logger.warning(f"Product {product_id} has no ProductInfo keyword set")

                    return metadata

            raise HTTPException(status_code=404, detail="Product not found")

        # Program

        @app.get("/program/{program_id}/state", tags=["Program"])
        async def get_program_state(program_id: str) -> ProgramState:
            """Return the current state of a program."""
            with _error_handler():
                return await self.kit.program(program_id).kv_get_model(ProgramState)

        @app.post("/program/{program_id}/enable", tags=["Program"])
        async def enable_program(program_id: str, controller_id: str | None = None):
            """Enable a program."""
            if controller_id is None:
                # TODO: When the program enable() method supports a None controller, i.e. leave
                #       the associated controller unchanged, this part can be removed.
                with _error_handler():
                    state = await self.kit.program(program_id).kv_get_model(ProgramState)
                    controller_id = state.enable_state.controller

                if controller_id is None:
                    raise HTTPException(status_code=422, detail="Controller ID is required")

            with _error_handler():
                await self.kit.program(program_id).enable(controller_id)

            return _status_ok()

        @app.post("/program/{program_id}/disable", tags=["Program"])
        async def disable_program(program_id: str):
            """Disable a program."""
            with _error_handler():
                await self.kit.program(program_id).disable()

            return _status_ok()

        @app.post("/program/{program_id}/activate", tags=["Program"])
        async def activate_program(program_id: str):
            """Start tasking for a program."""
            with _error_handler():
                await self.kit.program(program_id).start_tasking()

            return _status_ok()

        @app.post("/program/{program_id}/deactivate", tags=["Program"])
        async def deactivate_program(program_id: str):
            """Stop tasking for a program."""
            with _error_handler():
                await self.kit.program(program_id).stop_tasking()

            return _status_ok()

        @app.post("/program/{program_id}/abort", tags=["Program"])
        async def abort_program(program_id: str):
            """Abort program tasking immediately."""
            with _error_handler():
                await self.kit.program(program_id).abort_tasking()

            return _status_ok()

        # Agent

        @app.get("/agent/state", tags=["Agent"])
        async def get_agent_state() -> AgentState:
            """Get the current status of the agent."""
            with _error_handler():
                return await self.config.agent.require().kv_get_model(AgentState)

        @app.post("/agent/enable", tags=["Agent"])
        async def enable_agent():
            """Enable all agent control."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request, AgentConfigureRequest(global_control_enabled=True)
                )

            return _status_ok()

        @app.post("/agent/enable/{controller_id}", tags=["Agent"])
        async def enable_controller(controller_id: str):
            """Enable agent control for a specific controller."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(controller_control_enabled={controller_id: True}),
                )

            return _status_ok()

        @app.post("/agent/disable", tags=["Agent"])
        async def disable_agent():
            """Disable all agent control."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request, AgentConfigureRequest(global_control_enabled=False)
                )

            return _status_ok()

        @app.post("/agent/disable/{controller_id}", tags=["Agent"])
        async def disable_controller(controller_id: str):
            """Disable agent control for a specific controller."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(controller_control_enabled={controller_id: False}),
                )

            return _status_ok()

        @app.post("/agent/override/{controller_id}", tags=["Agent"])
        async def override_controller(controller_id: str, req: AgentOverrideRequest):
            """Override controller demand."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(controller_demand_override={controller_id: req.state}),
                )

            return _status_ok()

        @app.post("/agent/scheduler/enable", tags=["Agent"])
        async def enable_scheduler():
            """Enable agent scheduling."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(scheduling_enabled=True),
                )

            return _status_ok()

        @app.post("/agent/scheduler/disable", tags=["Agent"])
        async def disable_scheduler():
            """Disable agent scheduling."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(scheduling_enabled=False),
                )

            return _status_ok()

        @app.post("/agent/scheduler/include/{program_id}", tags=["Agent"])
        async def include_program(program_id: str):
            """Include a program for agent consideration."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(remove_program_exclusions={program_id}),
                )

            return _status_ok()

        @app.post("/agent/scheduler/exclude/{program_id}", tags=["Agent"])
        async def exclude_program(program_id: str):
            """Exclude a program from agent consideration."""
            with _error_handler():
                await self.config.agent.require().call(
                    agent_configure_request,
                    AgentConfigureRequest(add_program_exclusions={program_id}),
                )

            return _status_ok()

        app.openapi()
        add_sensorkit_schema(app.openapi_schema["components"]["schemas"])

        return app

    async def _start_forwarders(self, *, task_group: asyncio.TaskGroup):
        """Start the KV/stream forwarders and any configured data-product handlers.

        Split out from `serve` only so tests can drive it without running the uvicorn
        server. Not for use elsewhere: calling this and `serve` both would start two sets
        of forwarder tasks.
        """
        await self.kv_forwarder.start(task_group=task_group)
        await self.stream_forwarder.start(task_group=task_group)

        self._serve_handlers = []
        self._product_forwarders = []

        for serve_config in self.config.serve_data_products:
            handler = serve_config.create_handler()
            forwarder = ProductForwarder(handler, targets=self.client_queues)

            await forwarder.start(task_group=task_group)
            handler.start_monitor(task_group=task_group)

            self._serve_handlers.append(handler)
            self._product_forwarders.append(forwarder)

    async def serve(self, *, task_group: asyncio.TaskGroup):
        """Run the FastAPI server."""
        await self._start_forwarders(task_group=task_group)
        await self.server.serve()

    async def shutdown(self):
        # Unblock any parked firehose generators so uvicorn's graceful shutdown
        # isn't stuck waiting on a never-ending SSE response. Snapshot the set
        # because each generator removes its own queue on exit.
        for queue in tuple(self.client_queues):
            queue.put_nowait(None)

        if self.server.started:
            await self.server.shutdown()

        for handler in self._serve_handlers:
            await handler.stop_monitor()

        for forwarder in self._product_forwarders:
            await forwarder.stop()

        await self.kv_forwarder.stop()
        await self.stream_forwarder.stop()
