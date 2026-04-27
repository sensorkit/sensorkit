"""Test PWI4 rotator device."""

import asyncio

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from conftest import MockPWI4Client
from sensorkit.pwi4.rotator import PWI4Rotator, PWI4RotatorConfig, PWI4RotatorState
from sensorkit.pwi4.device import DeviceConnectionError


class TestPWI4RotatorConfig:
    def test_defaults(self):
        config = PWI4RotatorConfig()
        assert config.device_type == "rotator"
        assert config.status_frequency == 1.0

    def test_create_device(self):
        config = PWI4RotatorConfig()
        client = MockPWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Rotator)


class TestPWI4Rotator:
    @pytest.mark.asyncio
    async def test_init_connects_and_enables(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)

        await rotator.rotator_init(MagicMock())

        connect_reqs = client.find_requests("/rotator/connect")
        enable_reqs = client.find_requests("/rotator/enable")
        assert len(connect_reqs) == 1
        assert len(enable_reqs) == 1

    @pytest.mark.asyncio
    async def test_move(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        cmd = MagicMock()
        cmd.position = 135.0
        await rotator.rotator_move(cmd)

        reqs = client.find_requests("/rotator/goto_mech")
        assert len(reqs) == 1
        assert reqs[0][1] == {"degs": 135.0}

    @pytest.mark.asyncio
    async def test_move_requires_connected(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = False

        cmd = MagicMock()
        cmd.position = 135.0

        with pytest.raises(DeviceConnectionError):
            await rotator.rotator_move(cmd)

    @pytest.mark.asyncio
    async def test_stop(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        await rotator.rotator_stop(MagicMock())

        reqs = client.find_requests("/rotator/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_disables_and_disconnects(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        await rotator.rotator_deinit(MagicMock())

        stop_reqs = client.find_requests("/rotator/stop")
        disable_reqs = client.find_requests("/rotator/disable")
        disconnect_reqs = client.find_requests("/rotator/disconnect")
        assert len(stop_reqs) == 1
        assert len(disable_reqs) == 1
        assert len(disconnect_reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publish(self):
        client = MockPWI4Client()
        config = PWI4RotatorConfig(status_frequency=0.05)
        rotator = PWI4Rotator(config=config, client=client)

        with patch("sensorkit.pwi4.rotator.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device
            mock_sk.RotatorPosition = MagicMock()

            task = asyncio.create_task(rotator.status_publish())
            await asyncio.sleep(0.15)
            task.cancel()

            assert rotator.device_connected is True
            assert mock_device.publish.call_count >= 3  # Connected + Enabled + RotatorPosition
