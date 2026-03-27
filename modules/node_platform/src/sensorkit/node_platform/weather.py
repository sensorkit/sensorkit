from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


# Map Node Platform system metric names to sk.BasicWeather() field names.
_METRIC_FIELD_MAP: dict[str, str] = {
    "node_controller.weather_monitor.air_temperature":          "temperature",
    "node_controller.weather_monitor.air_humidity":             "humidity",
    "node_controller.weather_monitor.wind_speed_average":       "wind_speed",
    "node_controller.weather_monitor.wind_direction_average":   "wind_direction",
    "node_controller.weather_monitor.air_pressure":             "pressure",
    "node_controller.weather_monitor.rainfall_intensity":       "rain_rate",
}


@sk.declare_keyword
class Safety(BaseModel):
    is_safe: bool
    is_weather_safe: bool
    is_all_sky_safe: bool
    is_night: bool


@sk.declare_device
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
        """Restore state, discover available metrics, start publishing, put the mount into ASSISTED mode."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformWeatherState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformWeatherState()

        # Discover which weather metric names are available on this node
        self._weather_metric_names: list[str] = []
        try:
            names_resp = await self.api.call("v1_get_system_metric_names")
            all_names = names_resp.metric_names if names_resp.metric_names else []
            self._weather_metric_names = [
                n for n in all_names if n in _METRIC_FIELD_MAP
            ]
            logger.debug(f"discovered weather metrics: {self._weather_metric_names}")
        except Exception as e:
            logger.warning(f"Could not discover metric names: {e}")

        # Ensure we are in ASSISTED mode. Per the API:
        """
        Switches the telescope system to assisted operation mode. In this mode, the system provides automation
        support while keeping the operator in the control loop for key decisions. Use this endpoint when supervised
        remote operation is needed with operator oversight for safety-critical actions and observation approval.
        """
        # In practice, it means that the NodePlatform controls the opening/closing of shutters based on its internal
        # safety status, which is published here to SensorKit along with the operation mode, and where both are used as
        # agent constraints.
        await self.api.call("v1_enable_assisted_operation")

        # Start weather status publishing
        logger.debug("starting node_platform weather status loop")
        self._status_task = asyncio.create_task(self.status_publish())

        # Wait for initial connected status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def weather_init(self, cmd: sk.Init):
        """Nothing to do."""
        pass

    @sk.command_handler
    async def weather_deinit(self, cmd: sk.Deinit):
        """Nothing to do."""
        pass

    @sk.on_detach
    async def entity_deinit(self):
        """Stop status publishing, save state, put the mount back into MANUAL mode."""
        logger.debug("stopping node_platform weather status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        await sk.device().kv_put_model(self.state)

    async def status_publish(self):
        while True:
            try:
                weather_kw = await self._build_weather_keywords()
                if weather_kw is not None:
                    device = sk.device()
                    await device.publish(Connected(is_connected=True))

                    # Safety status (independent of BasicWeather)
                    try:
                        safety: osapi.V1SafetyStatus = await self.api.call(
                            "v1_get_safety_status"
                        )
                        await device.publish(
                            Safety(
                                is_safe=safety.is_safe,
                                is_weather_safe=safety.is_weather_safe,
                                is_all_sky_safe=safety.is_all_sky_safe,
                                is_night=safety.is_night
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Safety status unavailable: {e}")

                    await device.publish(weather_kw)
                else:
                    await sk.device().publish(Connected(is_connected=False))
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")

            await asyncio.sleep(self.config.status_frequency)

    async def _build_weather_keywords(self):
        """Collect weather data from the Node Platform and build a keyword model."""
        fields: dict[str, float | bool | None] = {}

        # 1. Weather station connected check
        try:
            ws_status: osapi.V1WeatherStationStatus = await self.api.call(
                "v1_get_weather_station_status"
            )
            self.device_connected = ws_status.connected
            if not ws_status.connected:
                return None
        except Exception as e:
            logger.debug(f"Weather station status unavailable: {e}")
            self.device_connected = False
            return None

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
                    value = metric.value
                    if field_name == "pressure":
                        value *= 100  # hPa -> Pa
                    fields[field_name] = value
        except Exception as e:
            logger.debug(f"System metrics unavailable: {e}")

        status_parts = [f"connected={self.device_connected}"]
        status_parts.extend(f"{k}={v:.3f}" for k, v in fields.items())
        logger.debug(f"NodePlatform weather status: {', '.join(status_parts)}")

        try:
            return sk.BasicWeather(**fields)
        except Exception as e:
            logger.warning(f"Failed to build BasicWeather model: {e}")
            return None


class NodePlatformWeatherConfig(NodePlatformDeviceConfig[NodePlatformWeather]):
    """Node Platform Weather configuration."""
    device_type: Literal["weather"] = "weather"
    metric_lookback_seconds: float = 300.0
    timeout: float = 30.0
    status_frequency: float = 30.0

    @override
    def create_device(self):
        return NodePlatformWeather(self)


class NodePlatformWeatherState(NodePlatformDeviceState):
    """Node Platform Weather state."""
    device_type: Literal["weather"] = "weather"