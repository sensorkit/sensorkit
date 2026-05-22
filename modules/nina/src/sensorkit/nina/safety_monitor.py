from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.models.safety import BasicSafety
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState


@sk.declare_device
class NinaSafetyMonitor(NinaDevice):
    """NINA SafetyMonitor implementation."""

    config: NinaSafetyMonitorConfig
    device_name = "SafetyMonitor"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaSafetyMonitorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaSafetyMonitorState()

        # Initialize the safety monitor
        await self.safety_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.safety_disconnect(sk.Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def safety_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.safety_connect(sk.Connect())
        await self.safety_connect(sk.Connect())

    @sk.command_handler
    async def safety_deinit(self, cmd: sk.Deinit):
        pass

    @sk.command_handler
    async def safety_connect(self, cmd: sk.Connect):
        await self.connect("safetymonitor")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def safety_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("safetymonitor")
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        while True:
            try:
                safety_monitor = await self.info("safetymonitor")
                connected = safety_monitor.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    is_safe = safety_monitor.get("IsSafe", False)

                    # logger.debug(
                    #     f"NINA safety monitor status: connected={connected}, is_safe={is_safe}"
                    # )

                    await device.publish(BasicSafety(is_safe=is_safe))
            except Exception as e:
                logger.exception(f"Error in safety monitor status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaSafetyMonitorConfig(NinaDeviceConfig[NinaSafetyMonitor]):
    device_type: Literal["safety_monitor"] = "safety_monitor"
    status_frequency: float = 10.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return NinaSafetyMonitor(self)


class NinaSafetyMonitorState(NinaDeviceState):
    device_type: Literal["safety_monitor"] = "safety_monitor"
