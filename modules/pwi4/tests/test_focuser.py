# SPDX-License-Identifier: Apache-2.0
"""Test PWI4 focuser device."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import MockPWI4Client

from sensorkit.pwi4.device import DeviceConnectionError
from sensorkit.pwi4.focuser import PWI4Focuser, PWI4FocuserConfig


class TestPWI4FocuserConfig:
    def test_defaults(self):
        config = PWI4FocuserConfig()
        assert config.device_type == "focuser"
        assert config.status_frequency == 1.0
        assert config.timeout == 60.0

    def test_create_device(self):
        config = PWI4FocuserConfig()
        client = MockPWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Focuser)


class TestPWI4Focuser:
    @pytest.mark.asyncio
    async def test_init_connects_and_enables(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)

        await focuser._initialize()

        connect_reqs = client.find_requests("/focuser/connect")
        enable_reqs = client.find_requests("/focuser/enable")
        assert len(connect_reqs) == 1
        assert len(enable_reqs) == 1

    @pytest.mark.asyncio
    async def test_move(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        cmd = MagicMock()
        cmd.position = 20000
        await focuser.focuser_change(cmd)

        reqs = client.find_requests("/focuser/goto")
        assert len(reqs) == 1
        assert reqs[0][1] == {"target": 20000}

    @pytest.mark.asyncio
    async def test_move_requires_connected(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = False

        cmd = MagicMock()
        cmd.position = 20000

        with pytest.raises(DeviceConnectionError):
            await focuser.focuser_change(cmd)

    @pytest.mark.asyncio
    async def test_stop(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser.focuser_stop(MagicMock())

        reqs = client.find_requests("/focuser/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_stops_and_disables(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser._deinitialize()

        # focuser_deinit stops and disables; disconnect happens on detach (entity_deinit).
        stop_reqs = client.find_requests("/focuser/stop")
        disable_reqs = client.find_requests("/focuser/disable")
        assert len(stop_reqs) == 1
        assert len(disable_reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publish(self):
        client = MockPWI4Client()
        config = PWI4FocuserConfig(status_frequency=0.05)
        focuser = PWI4Focuser(config=config, client=client)

        with patch("sensorkit.pwi4.focuser.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device
            mock_sk.FocusPosition = MagicMock()

            task = asyncio.create_task(focuser.status_publish())
            await asyncio.sleep(0.15)
            task.cancel()

            assert focuser.device_connected is True
            assert mock_device.publish.call_count >= 3  # Connected + Enabled + FocusPosition
