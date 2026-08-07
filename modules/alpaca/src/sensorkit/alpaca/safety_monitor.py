# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.safetymonitor import SafetyMonitor
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, Connected, Disconnect


@sk.declare_keyword
class AlpacaSafety(BaseModel):
    is_safe: bool


class AlpacaSafetyMonitorState(AlpacaDeviceState):
    device_type: Literal["safety_monitor"] = "safety_monitor"


@sk.declare_device
class AlpacaSafetyMonitor(AlpacaDevice):
    """Alpaca SafetyMonitor implementation."""

    config: AlpacaSafetyMonitorConfig
    device_name = "SafetyMonitor"
    state_model = AlpacaSafetyMonitorState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        # Initialize the safety monitor
        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.safety_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.safety_connect(Connect())
        self.monitor = SafetyMonitor(self.address, self.config.device_number, self.config.protocol)
        await self.safety_connect(Connect())

    @sk.command_handler
    async def safety_connect(self, cmd: Connect):
        await self.connect(self.monitor, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def safety_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.monitor)
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        m = self.monitor
        connected = await self.get(m, "Connected", False)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if connected:
            is_safe = await self.get(m, "IsSafe", False)

            # logger.debug(
            #     f"Alpaca safety monitor status: connected={connected}, is_safe={is_safe}"
            # )

            await device.publish(AlpacaSafety(is_safe=is_safe))


class AlpacaSafetyMonitorConfig(AlpacaDeviceConfig[AlpacaSafetyMonitor]):
    device_type: Literal["safety_monitor"] = "safety_monitor"
    status_frequency: float = 10.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return AlpacaSafetyMonitor(self)
