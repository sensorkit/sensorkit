import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sensorkit.api as sk
from sensorkit.pwi4.mount import PWI4Mount, PWI4MountConfig, PWI4MountState

from conftest import MockPWI4Client


@pytest.fixture
def client():
    return MockPWI4Client()


@pytest.fixture
def mount(client):
    config = PWI4MountConfig(device_type="mount")
    m = PWI4Mount(config=config, client=client)
    m.state = PWI4MountState()
    m.device_connected = True
    return m


class TestMountConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, mount):
        mount.device_connected = None

        with patch("sensorkit.pwi4.mount.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device

            await mount.mount_connect(sk.Connect())

        reqs = client.find_requests("/mount/connect")
        assert len(reqs) == 1
        assert mount.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, mount):
        client.set_status(**{"mount.is_connected": "false"})

        with patch("sensorkit.pwi4.mount.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device

            await mount.mount_disconnect(sk.Disconnect())

        reqs = client.find_requests("/mount/disconnect")
        assert len(reqs) == 1
        assert mount.device_connected is False


class TestMountHome:
    @pytest.mark.asyncio
    async def test_home_skips_when_initialized(self, client, mount):
        await mount.mount_home(sk.Home())

        reqs = client.find_requests("/mount/find_home")
        assert len(reqs) == 0

    @pytest.mark.asyncio
    async def test_home_issues_find_home(self, client, mount):
        client.set_status(
            **{
                "mount.axis0.is_position_initialized": "false",
                "mount.axis1.is_position_initialized": "false",
            }
        )
        await mount.mount_home(sk.Home())

        reqs = client.find_requests("/mount/find_home")
        assert len(reqs) == 1


class TestMountStop:
    @pytest.mark.asyncio
    async def test_stop(self, client, mount):
        client.set_status(
            **{"mount.is_slewing": "false", "mount.is_tracking": "false"}
        )
        await mount.mount_stop(sk.Stop())

        reqs = client.find_requests("/mount/stop")
        assert len(reqs) == 1


class TestMountPark:
    @pytest.mark.asyncio
    async def test_park_default(self, client, mount):
        await mount.mount_park(sk.MoveToPark())

        reqs = client.find_requests("/mount/park")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_park_waits_for_stop(self, client, mount):
        """Park should poll until mount is not slewing and not tracking."""
        client.set_status(
            **{"mount.is_slewing": "false", "mount.is_tracking": "false"}
        )
        await mount.mount_park(sk.MoveToPark())

        reqs = client.find_requests("/mount/park")
        assert len(reqs) == 1


class TestMountEnableDisableAxis:
    @pytest.mark.asyncio
    async def test_enable_azimuth(self, client, mount):
        await mount.mount_enable_axis(sk.EnableAxis(axis=sk.MountAxis.AZIMUTH))

        reqs = client.find_requests("/mount/enable")
        assert len(reqs) == 1
        assert reqs[0][1] == {"axis": 0}

    @pytest.mark.asyncio
    async def test_enable_altitude(self, client, mount):
        await mount.mount_enable_axis(sk.EnableAxis(axis=sk.MountAxis.ALTITUDE))

        reqs = client.find_requests("/mount/enable")
        assert len(reqs) == 1
        assert reqs[0][1] == {"axis": 1}

    @pytest.mark.asyncio
    async def test_disable_azimuth(self, client, mount):
        await mount.mount_disable_axis(sk.DisableAxis(axis=sk.MountAxis.AZIMUTH))

        reqs = client.find_requests("/mount/disable")
        assert len(reqs) == 1
        assert reqs[0][1] == {"axis": 0}


class TestMountInitOT:
    @pytest.mark.asyncio
    async def test_init_ot_with_heaters(self, client):
        config = PWI4MountConfig(
            device_type="mount",
            heaters={"m1": 50, "m2": 30},
        )
        m = PWI4Mount(config=config, client=client)
        m.state = PWI4MountState()
        m.device_connected = True

        await m.init_ot()

        reqs = client.find_requests("/heaters/set")
        assert len(reqs) == 2

    @pytest.mark.asyncio
    async def test_init_ot_with_fans(self, client):
        config = PWI4MountConfig(
            device_type="mount",
            fans=["m1", "m2"],
        )
        m = PWI4Mount(config=config, client=client)
        m.state = PWI4MountState()
        m.device_connected = True

        await m.init_ot()

        reqs = client.find_requests("/fans/on")
        assert len(reqs) == 1
        assert reqs[0][1] == {"roles": "m1,m2"}

    @pytest.mark.asyncio
    async def test_init_ot_empty(self, client, mount):
        await mount.init_ot()

        reqs_h = client.find_requests("/heaters/set")
        reqs_f = client.find_requests("/fans/on")
        assert len(reqs_h) == 0
        assert len(reqs_f) == 0

    @pytest.mark.asyncio
    async def test_deinit_ot_with_heaters(self, client):
        config = PWI4MountConfig(
            device_type="mount",
            heaters={"m1": 50, "m2": 30},
        )
        m = PWI4Mount(config=config, client=client)
        m.state = PWI4MountState()
        m.device_connected = True

        await m.deinit_ot()

        reqs = client.find_requests("/heaters/set")
        assert len(reqs) == 2
        # All heaters set to 0
        assert all(r[1]["power"] == 0 for r in reqs)

    @pytest.mark.asyncio
    async def test_deinit_ot_with_fans(self, client):
        config = PWI4MountConfig(
            device_type="mount",
            fans=["m1", "m2"],
        )
        m = PWI4Mount(config=config, client=client)
        m.state = PWI4MountState()
        m.device_connected = True

        await m.deinit_ot()

        reqs = client.find_requests("/fans/off")
        assert len(reqs) == 1
        assert reqs[0][1] == {"roles": "m1,m2"}
