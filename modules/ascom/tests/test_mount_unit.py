import pytest

import sensorkit.api as sk
from sensorkit.ascom.mount import MountService


class FakeBinding:
    def __init__(self):
        self.published = []

    async def publish(self, model):
        self.published.append(model)


class FakeMount:
    def __init__(self):
        self.Connected = False
        self.Slewing = False
        self.Tracking = False
        self.SiteLatitude = 10.0
        self.SiteLongitude = 20.0
        self.SiteElevation = 100.0
        self.Altitude = 30.0
        self.Azimuth = 40.0
        self.RightAscension = 12.5
        self.Declination = -5.2

    def Park(self):
        pass

    def FindHome(self):
        pass

    def SlewToAltAzAsync(self, Azimuth, Altitude):
        self.Slewing = True
        self.Altitude = Altitude
        self.Azimuth = Azimuth
        self.Slewing = False

    def SlewToCoordinatesAsync(self, RightAscension, Declination):
        self.Slewing = True
        self.RightAscension = RightAscension
        self.Declination = Declination
        self.Slewing = False


@pytest.mark.asyncio
async def test_mount_lifecycle_and_publishers():
    m = FakeMount()
    svc = MountService(device=m, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert m.Connected is True

    # publish_position called on startup
    kinds = {type(mdl).__name__ for mdl in svc.device.binding.published}
    assert "Connected" in kinds and "SitePosition" in kinds

    # publishers
    altaz = await svc.altaz_pointing()
    radec = await svc.radec_pointing()
    assert altaz.altitude_degrees == m.Altitude
    assert altaz.azimuth_degrees == m.Azimuth
    assert radec.right_ascension_hours == m.RightAscension
    assert radec.declination_degrees == m.Declination


@pytest.mark.asyncio
async def test_mount_follow_target_altaz_and_radec():
    m = FakeMount()
    svc = MountService(device=m, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.mount_connect(sk.Connect())

    await svc.mount_slew_alt_az(sk.FollowTarget(target=sk.AltAz(altitude_degrees=15.0, azimuth_degrees=200.0)))
    assert m.Tracking is False
    assert (m.Altitude, m.Azimuth) == (15.0, 200.0)

    await svc.mount_slew_alt_az(sk.FollowTarget(target=sk.RADec(right_ascension_hours=1.5, declination_degrees=-10.0)))
    assert m.Tracking is True
    assert (m.RightAscension, m.Declination) == (1.5, -10.0)
