from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure

# Re-export SDK enums for convenience in config / external use
EnclosureShutterState = osapi.EnclosureShutterState


@sk.declare_keyword
class EnclosureStatus(BaseModel):
    # Shutter
    shutter_state: str = "unknown"
    shutter_position_percent: float | None = None

    # Rotator
    rotator_connected: bool = False
    rotator_azimuth_degrees: float | None = None
    rotator_synced_with_mount: bool = False

    # Window
    window_connected: bool = False
    window_position_degrees: float | None = None
    window_synced_with_mount: bool = False

    # Fans
    fans: list[dict] | None = None

    # HVAC
    hvac_connected: bool = False
    hvac_on: bool = False
    hvac_mode: str | None = None
    hvac_fan_speed: str | None = None
    hvac_target_temperature_celsius: int | None = None
    hvac_temperature_celsius: int | None = None
    hvac_error: str | None = None


@sk.declare_device
class NodePlatformEnclosure(NodePlatformDevice):
    """Node Platform Enclosure implementation."""

    config: NodePlatformEnclosureConfig
    device_name = "Enclosure"
    shutter_state: EnclosureShutterState | None = EnclosureShutterState.UNKNOWN

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(NodePlatformEnclosureState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformEnclosureState()

        self.start_status_loop(self.status_publish())
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)
        await self.enclosure_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.enclosure_deinit(sk.Deinit())
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def enclosure_init(self, cmd: sk.Init):
        # FIXME: this likely replaces homing for Node Platform enclosures
        await self.api.call("v1_sync_enclosure_rotator_with_mount")
        await self.api.call("v1_sync_enclosure_window_with_mount")

        if not self.state.has_been_homed:
            await self.enclosure_home(sk.Home())

        # Turn on all enclosure fans
        if self.config.fans:
            all_fan_roles = list(osapi.V1EnclosureFanRole)
            await self.api.call(
                "v1_turn_on_enclosure_fans",
                osapi.V1TurnOnEnclosureFansRequest(roles=all_fan_roles),
            )
            logger.debug(f"turned on all enclosure fans: {[r.value for r in all_fan_roles]}")

    @sk.command_handler
    async def enclosure_deinit(self, cmd: sk.Deinit):
        await self.enclosure_stop(sk.Stop())
        await self.enclosure_close(CloseEnclosure())

        # Turn off all enclosure fans
        if self.config.fans:
            all_fan_roles = list(osapi.V1EnclosureFanRole)
            await self.api.call(
                "v1_turn_off_enclosure_fans",
                osapi.V1TurnOffEnclosureFansRequest(roles=all_fan_roles),
            )
            logger.debug(f"turned off all enclosure fans: {[r.value for r in all_fan_roles]}")

    @sk.command_handler
    async def enclosure_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping enclosure")

        await asyncio.gather(
            self.api.call("v1_halt_enclosure_shutters"),
            self.api.call("v1_halt_enclosure_window"),
        )

        logger.debug("stopped node_platform enclosure")

    @sk.command_handler
    async def enclosure_home(self, cmd: sk.Home):
        await self.require_connected()
        logger.debug("homing enclosure")

        await self.api.call("v1_home_enclosure_shutters")
        await asyncio.sleep(0.1)

        # Wait for homing to complete (state transitions away from HOMING)
        async with asyncio.timeout(self.config.timeout):
            while self.shutter_state in (
                EnclosureShutterState.HOMING,
                EnclosureShutterState.UNKNOWN,
                None,
            ):
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed enclosure")

    @sk.command_handler
    async def enclosure_open(self, cmd: OpenEnclosure):
        await self.require_connected()

        if self.config.operation_mode == "assisted":
            logger.warning(
                "Rejecting enclosure open (Node Platform controls shutter in ASSISTED mode)"
            )
            return

        # Ensure we're not moving
        async with asyncio.timeout(self.config.timeout):
            while self.shutter_state in (
                EnclosureShutterState.MOVING_OPEN,
                EnclosureShutterState.MOVING_CLOSE,
                EnclosureShutterState.HOMING,
                EnclosureShutterState.ERROR,
                EnclosureShutterState.UNKNOWN,
                None,
            ):
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opening enclosure")
        req = osapi.V1OpenEnclosureShuttersRequest(ignore_safety=False)
        await self.api.call("v1_open_enclosure_shutters", req)
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.shutter_state is not EnclosureShutterState.OPENED:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened enclosure")

    @sk.command_handler
    async def enclosure_close(self, cmd: CloseEnclosure):
        await self.require_connected()

        if self.config.operation_mode == "assisted":
            logger.warning(
                "Rejecting enclosure close (Node Platform controls shutter in ASSISTED mode)"
            )
            return

        # Ensure we're not moving
        async with asyncio.timeout(self.config.timeout):
            while self.shutter_state in (
                EnclosureShutterState.MOVING_OPEN,
                EnclosureShutterState.MOVING_CLOSE,
                EnclosureShutterState.HOMING,
                EnclosureShutterState.ERROR,
                EnclosureShutterState.UNKNOWN,
                None,
            ):
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closing enclosure")
        await self.api.call("v1_close_enclosure_shutters")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.shutter_state is not EnclosureShutterState.CLOSED:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed enclosure")

    async def status_publish(self):
        while True:
            try:
                enclosure_status: osapi.V2EnclosureStatus = await self.api.call(
                    "v2_get_enclosure_status"
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
                self.shutter_state = shutter_state

                device = sk.device()
                await device.kv_put_model(self.state)

                is_open = shutter_state in (
                    EnclosureShutterState.OPENED,
                    EnclosureShutterState.MOVING_OPEN,
                )

                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=is_open))

                # Build enclosure subsystem status
                properties: dict = {
                    "shutter_state": shutter_state.value
                    if hasattr(shutter_state, "value")
                    else str(shutter_state),
                }
                if position is not None:
                    properties["shutter_position_percent"] = position

                rotator = enclosure_status.rotator
                if rotator is not None:
                    properties["rotator_connected"] = rotator.connected
                    properties["rotator_azimuth_degrees"] = rotator.azimuth_degrees
                    properties["rotator_synced_with_mount"] = rotator.is_synced_with_mount

                window = enclosure_status.window
                if window is not None:
                    properties["window_connected"] = window.connected
                    properties["window_position_degrees"] = window.position_degrees
                    properties["window_synced_with_mount"] = window.is_synced_with_mount or False

                fans = enclosure_status.fans
                if fans is not None and fans.statuses:
                    properties["fans"] = [
                        {
                            "role": fan.role.value
                            if hasattr(fan.role, "value")
                            else str(fan.role),
                            "is_on": fan.is_on,
                            "connected": fan.connected,
                        }
                        for fan in fans.statuses
                    ]

                hvac = enclosure_status.hvac
                if hvac is not None:
                    properties["hvac_connected"] = hvac.connected
                    if hvac.details is not None:
                        d = hvac.details
                        properties["hvac_on"] = d.is_on
                        properties["hvac_mode"] = (
                            d.mode.value if hasattr(d.mode, "value") else str(d.mode)
                        )
                        properties["hvac_fan_speed"] = (
                            d.fan_speed.value
                            if hasattr(d.fan_speed, "value")
                            else str(d.fan_speed)
                        )
                        properties["hvac_target_temperature_celsius"] = (
                            d.target_temperature_celsius
                        )
                        properties["hvac_temperature_celsius"] = d.temperature_celsius
                        if d.error is not None:
                            properties["hvac_error"] = (
                                d.error.value if hasattr(d.error, "value") else str(d.error)
                            )

                await device.publish(EnclosureStatus(**properties))

                # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                # logger.debug(
                #     f"NodePlatform enclosure status: connected={connected}, {properties_str}"
                # )

            except Exception as e:
                logger.warning(f"Failed to update Node Platform enclosure status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformEnclosureConfig(NodePlatformDeviceConfig[NodePlatformEnclosure]):
    """Node Platform Enclosure configuration."""

    device_type: Literal["dome"] = "dome"
    fans: bool = False
    status_frequency: float = 1.0
    timeout: float = 120.0

    @override
    def create_device(self):
        return NodePlatformEnclosure(self)


class NodePlatformEnclosureState(NodePlatformDeviceState):
    """Node Platform Enclosure state."""

    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
