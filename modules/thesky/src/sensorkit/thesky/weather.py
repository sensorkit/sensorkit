# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.std.safety import BasicSafety, StandardSafety
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device(type=StandardSafety)
class TheSkyWeather(TheSkyDevice):
    """TheSky Weather implementation."""

    config: TheSkyDeviceConfig
    device_name = "Weather"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyWeatherState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyWeatherState()

        # Initialize the weather
        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.weather_connect(Connect())
        await self.weather_connect(Connect())

    @sk.command_handler
    async def weather_connect(self, cmd: Connect):
        logger.debug("connecting to weather")

        await self.execute(
            """
            WeatherUtil.connectWeatherStation();
            WeatherUtil.autoStartup = 1;
            WeatherUtil.autoShutdown = 1;
            WeatherUtil.scriptedObjectsNoGoAware = 1;
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""WeatherUtil.isWeatherStationConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to weather")

    @sk.command_handler
    async def weather_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from weather")

        await self.execute(
            """
            WeatherUtil.disconnectWeatherStation();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""WeatherUtil.isWeatherStationConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from weather")

    async def status_publish(self):
        resp = await self.execute(
            """
            var Out;
            Out = [
                WeatherUtil.isWeatherStationConnected,
                WeatherUtil.goodToGo
            ];
            """
        )

        connected, is_safe = [float(x) for x in resp.split(",")]

        connected = bool(connected)
        self.device_connected = connected
        is_safe = bool(is_safe)

        # logger.debug(
        #     f"TheSky weather status: connected={connected}, is_safe={is_safe}"
        # )

        device = sk.device()
        await device.publish(Connected(is_connected=connected))
        await device.publish(BasicSafety(is_safe=is_safe))


class TheSkyWeatherConfig(TheSkyDeviceConfig[TheSkyWeather]):
    """TheSky Weather configuration."""

    device_type: Literal["weather"] = "weather"
    status_frequency: float = 5.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return TheSkyWeather(self)


class TheSkyWeatherState(TheSkyDeviceState):
    """TheSky Weather state."""

    device_type: Literal["weather"] = "weather"
