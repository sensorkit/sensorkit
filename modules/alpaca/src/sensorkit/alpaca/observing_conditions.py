from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.observingconditions import ObservingConditions
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected
from sensorkit.std.weather import BasicWeather


@sk.declare_keyword
class AlpacaObservingConditionsStatus(BaseModel):
    """IObservingConditionsV2 properties."""

    # Extended sensors (beyond BasicWeather)
    sky_brightness: float | None = None
    sky_quality: float | None = None
    sky_temperature: float | None = None
    star_fwhm: float | None = None
    wind_gust: float | None = None

    # Time since last update
    time_since_last_update: dict[str, float] | None = None


@sk.declare_device
class AlpacaObservingConditions(AlpacaDevice):
    """Alpaca ObservingConditions implementation."""

    config: AlpacaObservingConditionsConfig
    device_name = "ObservingConditions"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaObservingConditionsState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AlpacaObservingConditionsState()

        await self.observing_conditions_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.observing_conditions_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def observing_conditions_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.observing_conditions_connect(sk.Connect())
        self.oc = ObservingConditions(
            self.address, self.config.device_number, self.config.protocol
        )
        await self.observing_conditions_connect(sk.Connect())

        if self.config.average_period is not None:
            try:
                await self.put(self.oc, "AveragePeriod", self.config.average_period)
            except Exception:
                logger.warning("Unable to set AveragePeriod")

    @sk.command_handler
    async def observing_conditions_deinit(self, cmd: sk.Deinit):
        await self.observing_conditions_disconnect(sk.Disconnect())

    @sk.command_handler
    async def observing_conditions_connect(self, cmd: sk.Connect):
        await self.connect(self.oc, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def observing_conditions_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.oc)
        await sk.device().publish(Connected(is_connected=False))

    async def _read_sensor(self, attr: str) -> float | None:
        try:
            val = await self.get(self.oc, attr, None)
            return float(val) if val is not None else None
        except Exception:
            return None

    async def _time_since_last_update(self, sensor_name: str) -> float | None:
        try:
            return await asyncio.to_thread(self.oc.TimeSinceLastUpdate, sensor_name)
        except Exception:
            return None

    async def status_publish(self):
        while True:
            try:
                connected = await self.get(self.oc, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    # Force a hardware refresh if supported
                    try:
                        await asyncio.to_thread(self.oc.Refresh)
                    except Exception:
                        pass

                    # BasicWeather sensors
                    weather = BasicWeather(
                        temperature=await self._read_sensor("Temperature"),
                        humidity=await self._read_sensor("Humidity"),
                        pressure=await self._read_sensor("Pressure"),
                        cloud_cover=await self._read_sensor("CloudCover"),
                        dew_point=await self._read_sensor("DewPoint"),
                        rain_rate=await self._read_sensor("RainRate"),
                        wind_direction=await self._read_sensor("WindDirection"),
                        wind_speed=await self._read_sensor("WindSpeed"),
                    )
                    await device.publish(weather)

                    # Extended sensors — only include available ones
                    properties: dict = {}
                    for attr, key in (
                        ("SkyBrightness", "sky_brightness"),
                        ("SkyQuality", "sky_quality"),
                        ("SkyTemperature", "sky_temperature"),
                        ("StarFWHM", "star_fwhm"),
                        ("WindGust", "wind_gust"),
                    ):
                        val = await self._read_sensor(attr)
                        if val is not None:
                            properties[key] = val

                    # Time since last update
                    all_sensors = [
                        "Temperature",
                        "Humidity",
                        "Pressure",
                        "CloudCover",
                        "DewPoint",
                        "RainRate",
                        "WindSpeed",
                        "WindDirection",
                        "WindGust",
                        "SkyBrightness",
                        "SkyQuality",
                        "SkyTemperature",
                        "StarFWHM",
                    ]
                    time_since_last_update = {}
                    for sensor in all_sensors:
                        t = await self._time_since_last_update(sensor)
                        if t is not None:
                            time_since_last_update[sensor] = t
                    if time_since_last_update:
                        properties["time_since_last_update"] = time_since_last_update

                    weather_str = ", ".join(
                        f"{k}={v}" for k, v in weather.model_dump(exclude_none=True).items()
                    )
                    properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                    # logger.debug(
                    #     f"Alpaca observing conditions status: connected={connected}, "
                    #     f"{weather_str}, {properties_str}"
                    # )

                    if properties:
                        await device.publish(AlpacaObservingConditionsStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in observing conditions status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class AlpacaObservingConditionsConfig(AlpacaDeviceConfig[AlpacaObservingConditions]):
    device_type: Literal["observing_conditions"] = "observing_conditions"
    average_period: float | None = None
    status_frequency: float = 30.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return AlpacaObservingConditions(self)


class AlpacaObservingConditionsState(AlpacaDeviceState):
    device_type: Literal["observing_conditions"] = "observing_conditions"
