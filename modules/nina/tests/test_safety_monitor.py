# SPDX-License-Identifier: Apache-2.0
import pytest
from conftest import MockNinaClient

import sensorkit.api as sk
from sensorkit.nina.safety_monitor import NinaSafetyMonitorConfig, NinaSafetyMonitorState
from sensorkit.std import Connect, Disconnect


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "IsSafe": True,
    })


@pytest.fixture
def safety_monitor(client):
    config = NinaSafetyMonitorConfig(device_type="safety_monitor")
    s = config.create_device()
    s._client = client
    s.state = NinaSafetyMonitorState()
    s.device_connected = True
    return s


class TestSafetyMonitorConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, safety_monitor):
        safety_monitor.device_connected = None
        await safety_monitor.safety_connect(Connect())
        assert safety_monitor.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, safety_monitor):
        await safety_monitor.safety_disconnect(Disconnect())
        assert safety_monitor.device_connected is False
