# SPDX-License-Identifier: Apache-2.0
"""Test PWI4 focuser device."""

import asyncio

import pytest

from sensorkit.pwi4.device import DeviceConnectionError
from sensorkit.pwi4.focuser import PWI4Focuser, PWI4FocuserConfig
from sensorkit.std import ChangeFocusPosition, FocusPosition, Stop

from .fakes import FakePWI4Client


class TestPWI4FocuserConfig:
    def test_defaults(self):
        config = PWI4FocuserConfig()
        assert config.device_type == "focuser"
        assert config.status_frequency == 1.0
        assert config.timeout == 60.0

    def test_create_device(self):
        config = PWI4FocuserConfig()
        client = FakePWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Focuser)


class TestPWI4Focuser:
    @pytest.mark.asyncio
    async def test_init_connects_and_enables(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)

        await focuser._initialize()

        connect_reqs = client.find_requests("/focuser/connect")
        enable_reqs = client.find_requests("/focuser/enable")
        assert len(connect_reqs) == 1
        assert len(enable_reqs) == 1

    @pytest.mark.asyncio
    async def test_move(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser.focuser_change(ChangeFocusPosition(position=20000))

        reqs = client.find_requests("/focuser/goto")
        assert len(reqs) == 1
        assert reqs[0][1] == {"target": 20000}

    @pytest.mark.asyncio
    async def test_move_requires_connected(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await focuser.focuser_change(ChangeFocusPosition(position=20000))

    @pytest.mark.asyncio
    async def test_stop(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser.focuser_stop(Stop())

        reqs = client.find_requests("/focuser/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_stops_and_disables(self):
        client = FakePWI4Client()
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
    async def test_status_publish(self, recorder):
        client = FakePWI4Client()
        config = PWI4FocuserConfig(status_frequency=0.05)
        focuser = PWI4Focuser(config=config, client=client)
        published = await recorder()

        task = asyncio.create_task(focuser.status_publish())

        try:
            assert (await published.wait_for(FocusPosition)).position == 15000.0
            assert focuser.device_connected is True
            assert published.keys() >= {"Connected", "Enabled", "FocusPosition"}
        finally:
            task.cancel()
