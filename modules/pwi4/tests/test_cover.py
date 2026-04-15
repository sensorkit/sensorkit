"""Test PWI4 mirror cover device."""

import asyncio

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from conftest import MockPWI4Client
from sensorkit.pwi4.cover import PWI4Cover, PWI4CoverConfig, PWI4CoverState
from sensorkit.pwi4.device import DeviceConnectionError


class TestPWI4CoverConfig:
    def test_defaults(self):
        config = PWI4CoverConfig()
        assert config.device_type == "cover"
        assert config.status_frequency == 1.0

    def test_create_device(self):
        config = PWI4CoverConfig()
        client = MockPWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Cover)


class TestPWI4Cover:
    @pytest.mark.asyncio
    async def test_init_connects(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)

        await cover.cover_init(MagicMock())

        reqs = client.find_requests("/mirrorcover/connect")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_open(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        from sensorkit.std.optics import OpenMirrorCover
        await cover.cover_open(OpenMirrorCover())

        reqs = client.find_requests("/mirrorcover/open")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_close(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        from sensorkit.std.optics import CloseMirrorCover
        await cover.cover_close(CloseMirrorCover())

        reqs = client.find_requests("/mirrorcover/close")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_open_requires_connected(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = False

        from sensorkit.std.optics import OpenMirrorCover
        with pytest.raises(DeviceConnectionError):
            await cover.cover_open(OpenMirrorCover())

    @pytest.mark.asyncio
    async def test_stop(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)

        await cover.cover_stop(MagicMock())

        reqs = client.find_requests("/mirrorcover/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_disconnects(self):
        client = MockPWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)

        await cover.cover_deinit(MagicMock())

        reqs = client.find_requests("/mirrorcover/disconnect")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publishes_open_state(self):
        client = MockPWI4Client()
        client.set_status(**{"mirrorcover.overall_state_name": "Open"})
        config = PWI4CoverConfig(status_frequency=0.05)
        cover = PWI4Cover(config=config, client=client)

        with patch("sensorkit.pwi4.cover.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device

            from sensorkit.models.devices import Connected, Opened
            mock_sk.Connected = Connected
            mock_sk.Opened = Opened

            task = asyncio.create_task(cover.status_publish())
            await asyncio.sleep(0.15)
            task.cancel()

            assert cover.device_connected is True

            # Find the Opened publish call
            opened_calls = [
                c for c in mock_device.publish.call_args_list
                if isinstance(c.args[0], Opened)
            ]
            assert len(opened_calls) >= 1
            assert opened_calls[0].args[0].is_open is True
