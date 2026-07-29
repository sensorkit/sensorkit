# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform cover device."""

import pytest

from .fakes import FakeNodePlatformAPI, make_cover_status
from sensorkit.node_platform.cover import (
    NodePlatformCover,
    NodePlatformCoverConfig,
    NodePlatformCoverState,
)
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.std import Stop
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


@pytest.fixture
def api():
    return FakeNodePlatformAPI()


@pytest.fixture
def cover(api):
    config = NodePlatformCoverConfig(device_type="cover", host="localhost")
    c = NodePlatformCover(config)
    c._api = api
    c.state = NodePlatformCoverState()
    c.device_connected = True
    return c


class TestCoverConfig:
    def test_defaults(self):
        config = NodePlatformCoverConfig(device_type="cover", host="localhost")
        assert config.device_type == "cover"
        assert config.timeout == 60.0

    def test_create_device(self):
        config = NodePlatformCoverConfig(device_type="cover", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformCover)


class TestCoverOpenClose:
    @pytest.mark.asyncio
    async def test_open(self, cover, api):
        api.set_response("v1_get_optical_tube_cover_status", make_cover_status(is_open=True))

        await cover.cover_open(OpenMirrorCover())

        assert len(api.find_calls("v1_open_optical_tube_cover")) == 1

    @pytest.mark.asyncio
    async def test_close(self, cover, api):
        api.set_response("v1_get_optical_tube_cover_status", make_cover_status(is_open=False))

        await cover.cover_close(CloseMirrorCover())

        assert len(api.find_calls("v1_close_optical_tube_cover")) == 1

    @pytest.mark.asyncio
    async def test_stop(self, cover, api):
        await cover.cover_stop(Stop())

        assert len(api.find_calls("v1_halt_optical_tube_cover")) == 1

    @pytest.mark.asyncio
    async def test_open_requires_connected(self, cover):
        cover.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await cover.cover_open(OpenMirrorCover())

    @pytest.mark.asyncio
    async def test_close_requires_connected(self, cover):
        cover.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await cover.cover_close(CloseMirrorCover())
