from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_keyword
class GoodToGo(BaseModel):
    is_good_to_go: bool


@sk.declare_device
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
        await self.weather_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        # Deinitialize the weather
        await self.weather_deinit(sk.Deinit())

        # Clean up, disconnect
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.weather_disconnect(sk.Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def weather_init(self, cmd: sk.Init):
        # Connect to the hardware
        self._reconnect = lambda: self.weather_connect(sk.Connect())
        await self.weather_connect(sk.Connect())

    @sk.command_handler
    async def weather_deinit(self, cmd: sk.Deinit):
        pass

    @sk.command_handler
    async def weather_connect(self, cmd: sk.Connect):
        logger.debug("connecting to Weather")

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

        logger.debug("connected to Weather")

    @sk.command_handler
    async def weather_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from Weather")

        await self.execute(
            """
            WeatherUtil.disconnectWeatherStation();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""WeatherUtil.isWeatherStationConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from Weather")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        WeatherUtil.isWeatherStationConnected,
                        WeatherUtil.goodToGo
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                connected, good_to_go = [float(x) for x in resp.split(",")]

                connected = bool(connected)
                self.device_connected = connected
                good_to_go = bool(good_to_go)

                # logger.debug(
                #     f"TheSky weather status: connected={connected}, good_to_go={good_to_go}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(GoodToGo(is_good_to_go=good_to_go))

            except Exception as e:
                logger.warning(f"Failed to update TheSky weather status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


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
