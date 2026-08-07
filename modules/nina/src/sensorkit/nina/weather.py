# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std import BasicWeather, Connect, Connected, Disconnect, StandardWeather


@sk.declare_device(type=StandardWeather)
class NinaWeather(NinaDevice):
    """NINA ObservingConditions implementation."""

    config: NinaWeatherConfig
    device_name = "Weather"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaWeatherState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaWeatherState()

        # Initialize the weather
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()
        await self._initialize()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.weather_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.weather_connect(Connect())
        await self.weather_connect(Connect())

    @sk.command_handler
    async def weather_connect(self, cmd: Connect):
        await self.connect("weather")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def weather_disconnect(self, cmd: Disconnect):
        await self.disconnect("weather")
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        info = await self.info("weather")
        connected = info.get("Connected", False)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if connected:
            weather = BasicWeather(
                temperature=info.get("Temperature"),
                humidity=info.get("Humidity"),
                pressure=info.get("Pressure"),
                dew_point=info.get("DewPoint"),
                wind_speed=info.get("WindSpeed"),
                wind_direction=info.get("WindDirection"),
                cloud_cover=info.get("CloudCover"),
                rain_rate=info.get("RainRate"),
            )

            await device.publish(weather)

            # weather_str = ", ".join(
            #     f"{k}={v}" for k, v in weather.model_dump(exclude_none=True).items()
            # )
            # logger.debug(f"NINA weather status: connected={connected}, {weather_str}")


class NinaWeatherConfig(NinaDeviceConfig[NinaWeather]):
    device_type: Literal["weather"] = "weather"
    status_frequency: float = 30.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return NinaWeather(self)


class NinaWeatherState(NinaDeviceState):
    device_type: Literal["weather"] = "weather"
