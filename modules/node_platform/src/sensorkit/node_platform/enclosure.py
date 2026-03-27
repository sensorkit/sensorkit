from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


# Re-export SDK enums for convenience in config / external use
EnclosureShutterState = osapi.EnclosureShutterState


@sk.declare_keyword
class OperationMode(BaseModel):
    mode: str


@sk.declare_device
class NodePlatformEnclosure(NodePlatformDevice):
    """Node Platform Enclosure implementation."""
    config: NodePlatformEnclosureConfig
    device_name = "Enclosure"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state and start status publishing."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformEnclosureState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformEnclosureState()

        # Start enclosure status publishing
        logger.debug("starting node_platform enclosure status loop")
        self._status_task = asyncio.create_task(self.status_publish())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def enclosure_init(self, cmd: sk.Init):
        """Home as needed."""
        self.require_connected()
        if self.config.needs_homed:
            if not self.state.has_been_homed:
                await self.enclosure_home(sk.Home())

        # Sync to the mount
        # FIXME: this likely replaces homing for Node Platform enclosures
        await self.api.call("v1_sync_enclosure_rotator_with_mount")
        await self.api.call("v1_sync_enclosure_window_with_mount")

    @sk.command_handler
    async def enclosure_deinit(self, cmd: sk.Deinit):
        """Stop all motion."""
        await self.enclosure_stop(sk.Stop())

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state and stop status publishing."""
        logger.debug("stopping node_platform enclosure status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def enclosure_home(self, cmd: sk.Home):
        self.require_connected()
        logger.debug("homing node_platform enclosure")

        await self.api.call("v1_home_enclosure_shutters")
        await asyncio.sleep(0.1)

        # Wait for homing to complete (state transitions away from HOMING)
        async with asyncio.timeout(self.config.timeout):
            while self.state.shutter_state in (
                EnclosureShutterState.HOMING,
                EnclosureShutterState.UNKNOWN,
                None,
            ):
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed node_platform enclosure")

    @sk.command_handler
    async def enclosure_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform enclosure")

        await asyncio.gather(
            self.api.call("v1_halt_enclosure_shutters"),
            self.api.call("v1_halt_enclosure_window"),
        )
        logger.debug("stopped node_platform enclosure")

    @sk.command_handler
    async def enclosure_open(self, cmd: sk.Open):
        # Wait for entity_init to complete
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

        self.require_connected()

        if self.state.shutter_state in (
            EnclosureShutterState.OPENED,
            EnclosureShutterState.MOVING_OPEN,
        ):
            return

        # Ensure in MANUAL mode
        status: osapi.V1SystemOperationStatus = await self.api.call(
            "v1_get_system_operation_status"
        )
        mode = status.system_operation_mode.value == "MANUAL"
        if mode:
            logger.debug("opening node_platform enclosure")
            req = osapi.V1OpenEnclosureShuttersRequest(ignore_safety=False)
            await self.api.call("v1_open_enclosure_shutters", req)
            await asyncio.sleep(0.1)

            async with asyncio.timeout(self.config.timeout):
                while self.state.shutter_state is not EnclosureShutterState.OPENED:
                    await asyncio.sleep(self.config.status_frequency)

            logger.debug("opened node_platform enclosure")

    @sk.command_handler
    async def enclosure_close(self, cmd: sk.Close):
        # Wait for entity_init to complete
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

        self.require_connected()

        if self.state.shutter_state in (
            EnclosureShutterState.CLOSED,
            EnclosureShutterState.MOVING_CLOSE,
        ):
            return

        # Ensure in MANUAL mode
        status: osapi.V1SystemOperationStatus = await self.api.call(
            "v1_get_system_operation_status"
        )
        mode = status.system_operation_mode.value == "MANUAL"
        if mode:
            logger.debug("closing node_platform enclosure")
            await self.api.call("v1_close_enclosure_shutters")
            await asyncio.sleep(0.1)

            async with asyncio.timeout(self.config.timeout):
                while self.state.shutter_state is not EnclosureShutterState.CLOSED:
                    await asyncio.sleep(self.config.status_frequency)

            logger.debug("closed node_platform enclosure")

    async def status_publish(self):
        while True:
            try:
                enclosure_status: osapi.V2EnclosureStatus = await self.api.call("v2_get_enclosure_status")
                operation_status: osapi.V1SystemOperationStatus = await self.api.call(
                    "v1_get_system_operation_status"
                )
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                # Extract primary shutter status (first in list)
                shutter = None
                if enclosure_status.shutters and enclosure_status.shutters.statuses:
                    shutter = enclosure_status.shutters.statuses[0]

                connected = shutter.connected if shutter else False
                shutter_state = shutter.state if shutter else EnclosureShutterState.UNKNOWN
                position = shutter.position_percent if shutter else None

                self.device_connected = connected
                self.state.shutter_state = shutter_state

                device = sk.device()
                await device.kv_put_model(self.state)

                is_open = shutter_state in (
                    EnclosureShutterState.OPENED,
                    EnclosureShutterState.MOVING_OPEN,
                )

                # logger.debug(
                #     f"NodePlatform enclosure status: connected={connected}, "
                #     f"state={shutter_state.value}, position={position}, "
                #     f"operation_mode={self.operation_mode}"
                # )

                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=is_open))
                await device.publish(OperationMode(mode=operation_status.system_operation_mode.value))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform enclosure status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformEnclosureConfig(NodePlatformDeviceConfig[NodePlatformEnclosure]):
    """Node Platform Enclosure configuration."""
    device_type: Literal["dome"] = "dome"
    needs_homed: bool = False
    timeout: float = 120.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformEnclosure(self)


class NodePlatformEnclosureState(NodePlatformDeviceState):
    """Node Platform Enclosure state."""
    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
    shutter_state: EnclosureShutterState | None = EnclosureShutterState.UNKNOWN