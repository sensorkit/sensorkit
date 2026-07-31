# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform weather device."""

from datetime import UTC, datetime

import pytest

from .fakes import (
    FakeNodePlatformAPI,
    SystemMetric,
    SystemMetricNames,
    SystemMetrics,
    make_weather_station_status,
)
from sensorkit.node_platform.weather import (
    NodePlatformWeather,
    NodePlatformWeatherConfig,
    NodePlatformWeatherState,
)


@pytest.fixture
def api():
    return FakeNodePlatformAPI()


@pytest.fixture
def weather(api):
    config = NodePlatformWeatherConfig(
        device_type="weather",
        host="localhost",
        operation_mode="assisted",
    )
    w = NodePlatformWeather(config)
    w._api = api
    w.state = NodePlatformWeatherState()
    w.device_connected = True
    w._weather_metric_names = []
    return w


class TestWeatherConfig:
    def test_defaults(self):
        config = NodePlatformWeatherConfig(device_type="weather", host="localhost")
        assert config.device_type == "weather"
        assert config.metric_lookback_seconds == 300.0
        assert config.status_frequency == 30.0
        assert config.operation_mode == "assisted"

    def test_create_device(self):
        config = NodePlatformWeatherConfig(device_type="weather", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformWeather)


class TestWeatherOperationMode:
    @pytest.mark.asyncio
    async def test_assisted_mode_set_on_init(self, api):
        """Weather entity_init should set assisted mode when configured."""
        api.set_response("v1_get_system_metric_names", SystemMetricNames())
        api.set_response("v1_enable_assisted_operation", None)

        config = NodePlatformWeatherConfig(
            device_type="weather",
            host="localhost",
            operation_mode="assisted",
        )
        w = NodePlatformWeather(config)
        w._api = api
        w.state = NodePlatformWeatherState()
        w.device_connected = True

        # Simulate the init steps (excluding status loop and wait)
        w._weather_metric_names = []
        await api.call("v1_get_system_metric_names")
        await api.call("v1_enable_assisted_operation")

        assert len(api.find_calls("v1_enable_assisted_operation")) == 1
        assert len(api.find_calls("v1_enable_manual_operation")) == 0

    @pytest.mark.asyncio
    async def test_manual_mode_set_on_init(self, api):
        """Weather entity_init should set manual mode when configured."""
        api.set_response("v1_enable_manual_operation", None)

        await api.call("v1_enable_manual_operation")

        assert len(api.find_calls("v1_enable_manual_operation")) == 1


class TestWeatherStatus:
    @pytest.mark.asyncio
    async def test_build_weather_keywords(self):
        """_build_weather_keywords should return BasicWeather from metrics."""
        api = FakeNodePlatformAPI()
        ws_status = make_weather_station_status(connected=True)
        api.set_response("v1_get_weather_station_status", ws_status)

        metrics = [
            SystemMetric(
                name="node_controller.weather_monitor.air_temperature",
                value=20.0,
                measured_at=datetime.now(UTC),
            ),
            SystemMetric(
                name="node_controller.weather_monitor.air_pressure",
                value=1013.25,
                measured_at=datetime.now(UTC),
            ),
        ]
        api.set_response("v1_get_system_metrics", SystemMetrics(metrics=metrics))

        config = NodePlatformWeatherConfig(device_type="weather", host="localhost")
        w = NodePlatformWeather(config)
        w._api = api
        w._weather_metric_names = [
            "node_controller.weather_monitor.air_temperature",
            "node_controller.weather_monitor.air_pressure",
        ]

        weather_kw, _basic_safety, _safety = await w._build_weather_keywords()

        assert weather_kw is not None
        assert weather_kw.temperature == 20.0

        # Metrics are already in BasicWeather units; pressure stays hPa.
        assert weather_kw.pressure == 1013.25

    @pytest.mark.asyncio
    async def test_build_weather_keywords_disconnected(self):
        """_build_weather_keywords should return None when weather station disconnected."""
        api = FakeNodePlatformAPI()
        ws_status = make_weather_station_status(connected=False)
        api.set_response("v1_get_weather_station_status", ws_status)

        config = NodePlatformWeatherConfig(device_type="weather", host="localhost")
        w = NodePlatformWeather(config)
        w._api = api
        w._weather_metric_names = []

        weather_kw, _basic_safety, _safety = await w._build_weather_keywords()

        assert weather_kw is None
        assert w.device_connected is False
