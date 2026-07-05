# SPDX-License-Identifier: Apache-2.0
"""test_indigo_real.py — Test the indigo service against a running indigo_service with real hardware.

Connects to the running SensorKit backend (NATS) and interacts with the
INDIGO weather device via SK keyword monitoring.

Prerequisites:
    1. NATS is running
    2. INDIGO server is running with the CloudWatcher driver loaded:
       indigo_server -v indigo_aux_cloudwatcher
    3. The CloudWatcher device is connected in INDIGO (via Control Panel or similar)
    4. Config is loaded:
       sensorkit kv load modules/indigo/tests/indigo_config.yaml
    5. indigo_service is running:
       sensorkit service run indigo_service sensorkit.indigo.service

Usage:
    pytest modules/indigo/tests/test_indigo_real.py -v
"""

import sys

import pytest
import pytest_asyncio

# Device entity name as registered in indigo_config.yaml
DEVICE_NAME = "weather"


@pytest_asyncio.fixture
async def kit():
    """Connect to the running SensorKit backend."""
    from sensorkit.api.bootstrap import connect

    sk = await connect()
    yield sk


@pytest_asyncio.fixture
async def device(kit):
    """Get a client for the INDIGO weather device."""
    return kit.device(DEVICE_NAME)


# ── Tests ────────────────────────────────────────────────────────────


class TestIndigoWeatherReal:
    @pytest.mark.asyncio
    async def test_device_is_connected(self, device):
        """Verify the weather device is online and reporting Connected."""
        from sensorkit.std import Connected

        monitor = await device.monitor(Connected)
        async for _, value in monitor:
            assert value.is_connected is True
            break

    @pytest.mark.asyncio
    async def test_basic_weather_published(self, device):
        """Verify BasicWeather keyword is being published."""
        from sensorkit.std.weather import BasicWeather

        monitor = await device.monitor(BasicWeather)
        async for _, weather in monitor:
            # At least one field should be populated
            has_data = any([
                weather.temperature is not None,
                weather.humidity is not None,
                weather.dew_point is not None,
                weather.pressure is not None,
                weather.wind_speed is not None,
            ])
            assert has_data, f"BasicWeather has no data: {weather}"
            break

    @pytest.mark.asyncio
    async def test_temperature_reasonable(self, device):
        """Verify temperature is within a physically plausible range."""
        from sensorkit.std.weather import BasicWeather

        monitor = await device.monitor(BasicWeather)
        async for _, weather in monitor:
            if weather.temperature is not None:
                assert -40.0 < weather.temperature < 60.0, (
                    f"Temperature {weather.temperature}°C outside plausible range"
                )
            break

    @pytest.mark.asyncio
    async def test_humidity_reasonable(self, device):
        """Verify humidity is within 0-100%."""
        from sensorkit.std.weather import BasicWeather

        monitor = await device.monitor(BasicWeather)
        async for _, weather in monitor:
            if weather.humidity is not None:
                assert 0.0 <= weather.humidity <= 100.0, (
                    f"Humidity {weather.humidity}% outside valid range"
                )
            break

    @pytest.mark.asyncio
    async def test_indigo_weather_status_published(self, device):
        """Verify IndigoWeatherStatus keyword is being published with raw items."""
        from sensorkit.indigo.weather import IndigoWeatherStatus

        monitor = await device.monitor(IndigoWeatherStatus)
        async for _, status in monitor:
            assert isinstance(status.items, dict)
            assert status.property_state in ("Idle", "Ok", "Busy", "Alert")
            break


# ── Run with plain python ────────────────────────────────────────────

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
