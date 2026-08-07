# SPDX-License-Identifier: Apache-2.0
"""Test PWI4 mirror cover device."""

import pytest

from sensorkit.pwi4.cover import PWI4Cover, PWI4CoverConfig
from sensorkit.pwi4.device import DeviceConnectionError
from sensorkit.std import Opened, Stop

from .fakes import FakePWI4Client


class TestPWI4CoverConfig:
    def test_defaults(self):
        config = PWI4CoverConfig()
        assert config.device_type == "cover"
        assert config.status_frequency == 1.0

    def test_create_device(self):
        config = PWI4CoverConfig()
        client = FakePWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Cover)


class TestPWI4Cover:
    @pytest.mark.asyncio
    async def test_init_connects(self):
        client = FakePWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)

        await cover._initialize()

        reqs = client.find_requests("/mirrorcover/connect")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_open(self):
        # cover_open polls until the cover reports "Open"; reflect that end-state.
        client = FakePWI4Client(status_overrides={"mirrorcover.overall_state_name": "Open"})
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        from sensorkit.std.optics import OpenMirrorCover

        await cover.cover_open(OpenMirrorCover())

        reqs = client.find_requests("/mirrorcover/open")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_close(self):
        client = FakePWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        from sensorkit.std.optics import CloseMirrorCover

        await cover.cover_close(CloseMirrorCover())

        reqs = client.find_requests("/mirrorcover/close")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_open_requires_connected(self):
        client = FakePWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = False

        from sensorkit.std.optics import OpenMirrorCover

        with pytest.raises(DeviceConnectionError):
            await cover.cover_open(OpenMirrorCover())

    @pytest.mark.asyncio
    async def test_stop(self):
        client = FakePWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        await cover.cover_stop(Stop())

        reqs = client.find_requests("/mirrorcover/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_stops_and_closes(self):
        client = FakePWI4Client()
        config = PWI4CoverConfig()
        cover = PWI4Cover(config=config, client=client)
        cover.device_connected = True

        await cover._deinitialize()

        # cover_deinit stops and closes; disconnect happens on detach (entity_deinit).
        stop_reqs = client.find_requests("/mirrorcover/stop")
        close_reqs = client.find_requests("/mirrorcover/close")
        assert len(stop_reqs) == 1
        assert len(close_reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publishes_open_state(self, recorder):
        client = FakePWI4Client()
        client.set_status(**{"mirrorcover.overall_state_name": "Open"})
        config = PWI4CoverConfig(status_frequency=0.05)
        cover = PWI4Cover(config=config, client=client)
        published = await recorder()

        await cover.status_publish()

        assert (await published.wait_for(Opened)).is_open is True
        assert cover.device_connected is True
