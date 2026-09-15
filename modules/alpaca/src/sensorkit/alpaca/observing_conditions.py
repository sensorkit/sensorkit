# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.exceptions import NotImplementedException
from alpaca.observingconditions import ObservingConditions
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.std.weather import BasicWeather, StandardWeather


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


class AlpacaObservingConditionsState(AlpacaDeviceState):
    device_type: Literal["observing_conditions"] = "observing_conditions"


@sk.declare_device(type=StandardWeather)
class AlpacaObservingConditions(AlpacaDevice):
    """Alpaca ObservingConditions implementation."""

    config: AlpacaObservingConditionsConfig
    device_name = "ObservingConditions"
    state_model = AlpacaObservingConditionsState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        # Initialize the observing conditions
        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.observing_conditions_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.observing_conditions_connect(Connect())
        self.oc = ObservingConditions(
            self.address, self.config.device_number, self.config.protocol
        )
        await self.observing_conditions_connect(Connect())

        # Set the average period
        if self.config.average_period is not None:
            try:
                await self.put(self.oc, "AveragePeriod", self.config.average_period)
            except Exception:
                logger.warning("Unable to set AveragePeriod")

    @sk.command_handler
    async def observing_conditions_connect(self, cmd: Connect):
        await self.connect(self.oc, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def observing_conditions_disconnect(self, cmd: Disconnect):
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

    async def _refresh(self):
        """Force a hardware refresh, tolerating devices that do not offer one."""

        try:
            await asyncio.to_thread(self.oc.Refresh)
        except NotImplementedException:
            pass
        except Exception as e:
            logger.opt(exception=e).warning(
                "observing conditions refresh failed, readings may be stale"
            )

    async def status_publish(self):
        connected = await self.get(self.oc, "Connected", False)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if connected:
            await self._refresh()

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

            # weather_str = ", ".join(
            #     f"{k}={v}" for k, v in weather.model_dump(exclude_none=True).items()
            # )
            # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
            # logger.debug(
            #     f"Alpaca observing conditions status: connected={connected}, "
            #     f"{weather_str}, {properties_str}"
            # )

            if properties:
                await device.publish(AlpacaObservingConditionsStatus(**properties))


class AlpacaObservingConditionsConfig(AlpacaDeviceConfig[AlpacaObservingConditions]):
    device_type: Literal["observing_conditions"] = "observing_conditions"
    average_period: float | None = None
    status_frequency: float = 30.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return AlpacaObservingConditions(self)
