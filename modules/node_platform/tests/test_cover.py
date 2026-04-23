"""Tests for Node Platform cover device."""

import pytest
from unittest.mock import MagicMock

from conftest import MockNodePlatformAPI, make_cover_status
from sensorkit.node_platform.cover import (
    NodePlatformCover,
    NodePlatformCoverConfig,
    NodePlatformCoverState,
)
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
import sensorkit.api as sk


@pytest.fixture
def api():
    return MockNodePlatformAPI()


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
        cover.cover_is_open = True  # Simulate status loop updating

        await cover.cover_open(OpenMirrorCover())

        assert len(api.find_calls("v1_open_optical_tube_cover")) == 1

    @pytest.mark.asyncio
    async def test_close(self, cover, api):
        cover.cover_is_open = False  # Simulate status loop updating

        await cover.cover_close(CloseMirrorCover())

        assert len(api.find_calls("v1_close_optical_tube_cover")) == 1

    @pytest.mark.asyncio
    async def test_stop(self, cover, api):
        await cover.cover_stop(sk.Stop())

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
