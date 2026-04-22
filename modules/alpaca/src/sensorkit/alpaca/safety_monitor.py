from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.safetymonitor import SafetyMonitor
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected


@sk.declare_keyword
class AlpacaSafety(BaseModel):
    is_safe: bool


@sk.declare_device
class AlpacaSafetyMonitor(AlpacaDevice):
    """Alpaca SafetyMonitor implementation."""

    config: AlpacaSafetyMonitorConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaSafetyMonitorState)
        except Exception:
            self.state = AlpacaSafetyMonitorState()

        self._start_init(self.safety_init(sk.Init()))

    @sk.on_detach
    async def entity_deinit(self):
        await self._cancel_init()
        await self.safety_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def safety_init(self, cmd: sk.Init):
        self.device_name = "SafetyMonitor"
        self.monitor = SafetyMonitor(
            self.address, self.config.device_number, self.config.protocol
        )

        await self.safety_connect(sk.Connect())
        self.start_status_loop(self.status_publish())

    @sk.command_handler
    async def safety_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.safety_disconnect(sk.Disconnect())

    @sk.command_handler
    async def safety_connect(self, cmd: sk.Connect):
        await self.connect(self.monitor, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def safety_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.monitor)
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        while True:
            try:
                m = self.monitor
                connected = await self.get(m, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    is_safe = await self.get(m, "IsSafe", False)

                    logger.debug(
                        f"Alpaca safety monitor status: connected={connected}, is_safe={is_safe}"
                    )

                    await device.publish(AlpacaSafety(is_safe=is_safe))
            except Exception as e:
                logger.exception(f"Error in safety monitor status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AlpacaSafetyMonitorConfig(AlpacaDeviceConfig[AlpacaSafetyMonitor]):
    device_type: Literal["safety_monitor"] = "safety_monitor"
    timeout: float = 30.0
    status_frequency: float = 10.0

    @override
    def create_device(self):
        return AlpacaSafetyMonitor(self)


class AlpacaSafetyMonitorState(AlpacaDeviceState):
    device_type: Literal["safety_monitor"] = "safety_monitor"
