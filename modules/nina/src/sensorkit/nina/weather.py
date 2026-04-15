from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std.weather import BasicWeather


@sk.declare_device
class NinaWeather(NinaDevice):
    """NINA ObservingConditions implementation."""

    config: NinaWeatherConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaWeatherState)
        except Exception:
            self.state = NinaWeatherState()

        await self.weather_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.weather_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def weather_init(self, cmd: sk.Init):
        self.device_name = "Weather"
        await self.connect("weather")
        await sk.device().publish(Connected(is_connected=True))
        self.start_status_loop(self.status_publish())

    @sk.command_handler
    async def weather_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.disconnect("weather")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def weather_connect(self, cmd: sk.Connect):
        await self.connect("weather")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def weather_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("weather")
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        while True:
            try:
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

                    weather_str = ", ".join(
                        f"{k}={v}" for k, v in weather.model_dump(exclude_none=True).items()
                    )
                    # logger.debug(
                    #     f"NINA observing conditions status: connected={connected}, {weather_str}"
                    # )

                    await device.publish(weather)
            except Exception as e:
                logger.exception(f"Error in weather status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaWeatherConfig(NinaDeviceConfig[NinaWeather]):
    device_type: Literal["weather"] = "weather"
    timeout: float = 30.0
    status_frequency: float = 30.0

    @override
    def create_device(self):
        return NinaWeather(self)


class NinaWeatherState(NinaDeviceState):
    device_type: Literal["weather"] = "weather"
