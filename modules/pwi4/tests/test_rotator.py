# SPDX-License-Identifier: Apache-2.0
"""Test PWI4 rotator device."""

import asyncio

import pytest

from .fakes import FakePWI4Client
from sensorkit.pwi4.device import DeviceConnectionError
from sensorkit.pwi4.rotator import PWI4Rotator, PWI4RotatorConfig
from sensorkit.std import Stop
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


class TestPWI4RotatorConfig:
    def test_defaults(self):
        config = PWI4RotatorConfig()
        assert config.device_type == "rotator"
        assert config.status_frequency == 1.0

    def test_create_device(self):
        config = PWI4RotatorConfig()
        client = FakePWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Rotator)


class TestPWI4Rotator:
    @pytest.mark.asyncio
    async def test_init_connects_and_enables(self):
        client = FakePWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)

        await rotator._initialize()

        connect_reqs = client.find_requests("/rotator/connect")
        enable_reqs = client.find_requests("/rotator/enable")
        assert len(connect_reqs) == 1
        assert len(enable_reqs) == 1

    @pytest.mark.asyncio
    async def test_move(self):
        client = FakePWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        await rotator.rotator_change(ChangeRotatorPosition(position=135.0))

        reqs = client.find_requests("/rotator/goto_mech")
        assert len(reqs) == 1
        assert reqs[0][1] == {"degs": 135.0}

    @pytest.mark.asyncio
    async def test_move_requires_connected(self):
        client = FakePWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await rotator.rotator_change(ChangeRotatorPosition(position=135.0))

    @pytest.mark.asyncio
    async def test_stop(self):
        client = FakePWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        await rotator.rotator_stop(Stop())

        reqs = client.find_requests("/rotator/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_stops_and_disables(self):
        client = FakePWI4Client()
        config = PWI4RotatorConfig()
        rotator = PWI4Rotator(config=config, client=client)
        rotator.device_connected = True

        await rotator._deinitialize()

        # rotator_deinit stops and disables; disconnect happens on detach (entity_deinit).
        stop_reqs = client.find_requests("/rotator/stop")
        disable_reqs = client.find_requests("/rotator/disable")
        assert len(stop_reqs) == 1
        assert len(disable_reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publish(self, recorder):
        client = FakePWI4Client()
        config = PWI4RotatorConfig(status_frequency=0.05)
        rotator = PWI4Rotator(config=config, client=client)
        published = await recorder()

        task = asyncio.create_task(rotator.status_publish())

        try:
            assert (await published.wait_for(RotatorPosition)).position == 90.0
            assert rotator.device_connected is True
            assert published.keys() >= {"Connected", "Enabled", "RotatorPosition"}
        finally:
            task.cancel()
