# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.std import Connected
from sensorkit.std.safety import BasicSafety, StandardSafety
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)
from sensorkit.std.weather import BasicWeather, StandardWeather

# Map Node Platform system metric names to BasicWeather() field names.
_METRIC_FIELD_MAP: dict[str, str] = {
    "node_controller.weather_monitor.air_temperature": "temperature",
    "node_controller.weather_monitor.air_humidity": "humidity",
    "node_controller.weather_monitor.wind_speed_average": "wind_speed",
    "node_controller.weather_monitor.wind_direction_average": "wind_direction",
    "node_controller.weather_monitor.air_pressure": "pressure",
    "node_controller.weather_monitor.rainfall_intensity": "rain_rate",
}


@sk.declare_keyword
class OperationMode(BaseModel):
    mode: str


@sk.declare_keyword
class Safety(BaseModel):
    """Node Platform extended safety status. The summary `is_safe` flag is published
    separately via `BasicSafety`; this keyword carries the breakdown."""
    is_weather_safe: bool
    is_all_sky_safe: bool
    is_night: bool


@sk.declare_device(traits=[StandardWeather, StandardSafety])
class NodePlatformWeather(NodePlatformDevice):
    """Node Platform Weather Station implementation.

    Publishes weather data via SensorKit's BasicWeather keyword system.
    Combines live sensor metrics with the Node Platform safety status to
    provide a unified weather picture for observatory automation.
    """

    config: NodePlatformWeatherConfig
    device_name = "Weather"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NodePlatformWeatherState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformWeatherState()

        # Initialize the weather
        await self._initialize()
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Discover which weather metric names are available on this node
        self._weather_metric_names: list[str] = []
        try:
            names_resp = await self.api.call("v1_get_system_metric_names")
            all_names = names_resp.metric_names if names_resp.metric_names else []
            self._weather_metric_names = [n for n in all_names if n in _METRIC_FIELD_MAP]
            logger.debug(f"discovered weather metrics: {self._weather_metric_names}")
        except Exception as e:
            logger.warning(f"Could not discover metric names: {e}")

        # Set the configured operation mode on the Node Platform.
        #
        # ASSISTED mode: the Node Platform closes the shutter on unsafe conditions,
        # but does NOT auto-open on safe — the enclosure module temporarily
        # switches to MANUAL to perform opens/closes, then restores ASSISTED (if configured)
        # so unsafe-close still works. SensorKit publishes safety/mode keywords for
        # agent constraints.
        #
        # MANUAL mode: SensorKit is fully responsible for opening/closing the
        # shutter via its own constraint monitoring.
        if self.config.operation_mode == "assisted":
            await self.api.call("v1_enable_assisted_operation")
            logger.debug("set Node Platform to ASSISTED mode")
        else:
            await self.api.call("v1_enable_manual_operation")
            logger.debug("set Node Platform to MANUAL mode")

    async def status_publish(self):
        while True:
            try:
                weather_kw, basic_safety_kw, safety_kw = await self._build_weather_keywords()
                if weather_kw is not None:
                    device = sk.device()
                    await device.publish(Connected(is_connected=True))

                    if basic_safety_kw is not None:
                        # Standard go/no-go signal — what the StandardSafety archetype
                        # asserts and what WeatherConstraint / SafetyConstraint consume.
                        await device.publish(basic_safety_kw)

                    if safety_kw is not None:
                        # Node Platform's breakdown of the safety signal (weather vs.
                        # all-sky vs. day/night). Supplementary to BasicSafety.
                        await device.publish(safety_kw)

                    # Operation mode (allows agent to constrain on mode changes)
                    try:
                        op_status: osapi.V1SystemOperationStatus = await self.api.call(
                            "v1_get_system_operation_status"
                        )
                        await device.publish(
                            OperationMode(mode=op_status.system_operation_mode.value)
                        )
                    except Exception as e:
                        logger.warning(f"Operation mode unavailable: {e}")

                    await device.publish(weather_kw)
                else:
                    await sk.device().publish(Connected(is_connected=False))
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)

    async def _build_weather_keywords(self):
        fields: dict[str, float | bool | None] = {}
        basic_safety_kw: BasicSafety | None = None
        safety_kw: Safety | None = None

        # 1. Weather station connected check
        try:
            ws_status: osapi.V1WeatherStationStatus = await self.api.call(
                "v1_get_weather_station_status"
            )
            self.device_connected = ws_status.connected
            if not ws_status.connected:
                return None, None, None
        except Exception as e:
            logger.debug(f"Weather station status unavailable: {e}")
            self.device_connected = False
            return None, None, None

        # 2. Live weather metrics via system metrics endpoint
        try:
            now = datetime.now(UTC)
            after = now - timedelta(seconds=self.config.metric_lookback_seconds)

            # Query each discovered weather metric name individually for
            # targeted results, or all at once if the list is empty
            raw_metrics: list[osapi.V1SystemMetric] = []
            if self._weather_metric_names:
                for metric_name in self._weather_metric_names:
                    resp: osapi.V1SystemMetrics = await self.api.call(
                        "v1_get_system_metrics",
                        metric_name=metric_name,
                        after=after,
                        before=now,
                    )
                    raw_metrics.extend(resp.metrics)
            else:
                resp = await self.api.call(
                    "v1_get_system_metrics",
                    after=after,
                    before=now,
                )
                raw_metrics = resp.metrics

            # Group by name and take the most recent reading
            latest_by_name: dict[str, osapi.V1SystemMetric] = {}
            for m in raw_metrics:
                prev = latest_by_name.get(m.name)
                if prev is None or m.measured_at > prev.measured_at:
                    latest_by_name[m.name] = m

            for name, metric in latest_by_name.items():
                field_name = _METRIC_FIELD_MAP.get(name)
                if field_name is not None:
                    fields[field_name] = metric.value
        except Exception as e:
            logger.debug(f"System metrics unavailable: {e}")

        # 3. Safety status (independent of BasicWeather). The summary go/no-go flag
        # goes into BasicSafety (the SK-standard keyword); the per-source breakdown
        # goes into our local Safety keyword.
        try:
            safety: osapi.V1SafetyStatus = await self.api.call("v1_get_safety_status")
            basic_safety_kw = BasicSafety(is_safe=safety.is_safe)
            safety_kw = Safety(
                is_weather_safe=safety.is_weather_safe,
                is_all_sky_safe=safety.is_all_sky_safe,
                is_night=safety.is_night,
            )
        except Exception as e:
            logger.warning(f"Safety status unavailable: {e}")

        status_parts = [f"connected={self.device_connected}"]
        status_parts.extend(f"{k}={v:.3f}" for k, v in fields.items())
        if basic_safety_kw is not None:
            status_parts.append(f"is_safe={basic_safety_kw.is_safe}")
        if safety_kw is not None:
            status_parts.extend(
                f"{k}={v}" for k, v in safety_kw.model_dump().items()
            )
        logger.debug(f"NodePlatform weather status: {', '.join(status_parts)}")

        try:
            return BasicWeather(**fields), basic_safety_kw, safety_kw
        except Exception as e:
            logger.warning(f"Failed to build BasicWeather model: {e}")
            return None, basic_safety_kw, safety_kw


class NodePlatformWeatherConfig(NodePlatformDeviceConfig[NodePlatformWeather]):
    """Node Platform Weather configuration."""

    device_type: Literal["weather"] = "weather"
    metric_lookback_seconds: float = 300.0
    status_frequency: float = 30.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return NodePlatformWeather(self)


class NodePlatformWeatherState(NodePlatformDeviceState):
    """Node Platform Weather state."""

    device_type: Literal["weather"] = "weather"
