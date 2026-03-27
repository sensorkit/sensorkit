import asyncio
from types import SimpleNamespace

import pytest

import sensorkit.api as sk
from sensorkit.models.devices import (
    AltAz,
    AltAzArcseconds,
    ApplyOffset,
    ModelAddPoint,
    ModelClearPoints,
    ModelDeletePoint,
    ModelDisablePoint,
    ModelEnablePoint,
    ModelLoad,
    ModelSave,
    RADec,
    RADecArcseconds,
    ReferenceFrame,
    SetAzimuthWrapRangeMin,
    SetParkPosition,
)
from sensorkit.pwi4.mount import MountDevice, MountTelemetryPublisher


class FakeBinding:
    def __init__(self):
        self.published: list = []

    async def publish(self, model):
        self.published.append(model)


class FakeMountAPI:
    def __init__(self):
        self.calls = []
        self._status = SimpleNamespace(
            site=SimpleNamespace(latitude_degs=10.0, longitude_degs=20.0, height_meters=100.0),
            mount=SimpleNamespace(
                altitude_degs=30.0,
                azimuth_degs=40.0,
                ra_j2000_hours=12.5,
                dec_j2000_degs=-5.2,
                axis0=SimpleNamespace(
                    dist_to_target_arcsec=1.1,
                    rms_error_arcsec=0.1,
                    max_velocity_degs_per_sec=3.0,
                    acceleration_degs_per_sec_sqr=0.2,
                    measured_velocity_degs_per_sec=0.5,
                    min_mech_position_degs=-180.0,
                    max_mech_position_degs=180.0,
                ),
                axis1=SimpleNamespace(
                    dist_to_target_arcsec=2.2,
                    rms_error_arcsec=0.2,
                    max_velocity_degs_per_sec=2.0,
                    acceleration_degs_per_sec_sqr=0.1,
                    measured_velocity_degs_per_sec=0.3,
                    min_mech_position_degs=-90.0,
                    max_mech_position_degs=90.0,
                ),
                is_connected=True,
            )
        )

    async def status(self):
        self.calls.append(("status", ()))
        return self._status

    async def connect(self): self.calls.append(("connect", ()))
    async def disconnect(self): self.calls.append(("disconnect", ()))
    async def enable(self, axis): self.calls.append(("enable", (axis,)))
    async def disable(self, axis): self.calls.append(("disable", (axis,)))
    async def find_home(self): self.calls.append(("find_home", ()))
    async def park(self): self.calls.append(("park", ()))
    async def goto_ra_dec_j2000(self, ra_hours, dec_degs): self.calls.append(("goto_ra_dec_j2000", (ra_hours, dec_degs)))
    async def goto_alt_az(self, alt_degs, az_degs, tolerance_deg=None): self.calls.append(("goto_alt_az", (alt_degs, az_degs, tolerance_deg)))
    async def tracking_on(self): self.calls.append(("tracking_on", ()))
    async def stop(self): self.calls.append(("stop", ()))
    async def offset(self, **kwargs): self.calls.append(("offset", (kwargs,)))
    async def set_axis0_wrap_range_min(self, minv): self.calls.append(("set_axis0_wrap_range_min", (minv,)))
    async def model_add_point(self, ra, dec): self.calls.append(("model_add_point", (ra, dec)))
    async def model_delete_point(self, *idx): self.calls.append(("model_delete_point", (idx,)))
    async def model_enable_point(self, *idx): self.calls.append(("model_enable_point", (idx,)))
    async def model_disable_point(self, *idx): self.calls.append(("model_disable_point", (idx,)))
    async def model_clear_points(self): self.calls.append(("model_clear_points", ()))
    async def model_save(self, filename): self.calls.append(("model_save", (filename,)))
    async def model_load(self, filename): self.calls.append(("model_load", (filename,)))
    async def set_park_here(self): self.calls.append(("set_park_here", ()))


@pytest.mark.asyncio
async def test_mount_telemetry_publishes_models():
    api = FakeMountAPI()
    binding = FakeBinding()
    pub = MountTelemetryPublisher(binding=SimpleNamespace(publish=binding.publish), api=api, frequency=0.01)

    await pub.start()
    await asyncio.sleep(0.03)
    await pub.stop()

    kinds = {type(m).__name__ for m in binding.published}

    assert "AltAzPointing" in kinds
    assert "RADecPointing" in kinds
    assert "SitePosition" in kinds
    assert "Connected" in kinds


@pytest.mark.asyncio
async def test_mount_device_lifecycle_and_commands():
    api = FakeMountAPI()
    device = MountDevice(pwi=SimpleNamespace(mount=api))
    device.device.binding = SimpleNamespace(publish=FakeBinding().publish)

    # startup should connect and start telemetry
    await device.startup()
    assert ("connect", ()) in api.calls

    # init should enable axes and find home if not initialized
    async def fake_status_uninit():
        return SimpleNamespace(
            mount=SimpleNamespace(axis0=SimpleNamespace(is_position_initialized=False), axis1=SimpleNamespace(is_position_initialized=False))
        )
    api.status = fake_status_uninit

    await device.init(sk.Init())
    assert ("enable", (0,)) in api.calls
    assert ("enable", (1,)) in api.calls
    assert ("find_home", ()) in api.calls

    await device.follow_target(sk.FollowTarget(target=RADec(right_ascension_hours=1.5, declination_degrees=-10.0)))
    assert ("goto_ra_dec_j2000", (1.5, -10.0)) in api.calls

    await device.follow_target(sk.FollowTarget(target=AltAz(altitude_degrees=20.0, azimuth_degrees=100.0)))
    assert ("goto_alt_az", (20.0, 100.0, None)) in api.calls

    await device.set_park_here(SetParkPosition(altitude_degrees=15.0, azimuth_degrees=200.0))
    assert ("goto_alt_az", (15.0, 200.0, 0.05)) in api.calls
    assert ("set_park_here", ()) in api.calls

    await device.offset(ApplyOffset(offset=RADecArcseconds(right_ascension_arcseconds=1.0, declination_arcseconds=-2.0)))
    await device.offset(ApplyOffset(offset=AltAzArcseconds(azimuth_arcseconds=3.0, altitude_arcseconds=4.0)))
    assert ("offset", ({"ra_arcsec": 1.0, "dec_arcsec": -2.0},)) in api.calls or any(k == "offset" and 1.0 in v[0].get("ra_arcsec", []) for k, v in api.calls)
    assert any(k == "offset" for k, _ in api.calls)

    await device.model_add_point(ModelAddPoint(reference_frame=ReferenceFrame.J2000, right_ascension_hours=2.0, declination_degrees=30.0))
    await device.model_delete_point(ModelDeletePoint(indexes=(1, 2)))
    await device.model_enable_point(ModelEnablePoint(indexes=(1, 2)))
    await device.model_disable_point(ModelDisablePoint(indexes=(3,)))
    await device.model_clear_points(ModelClearPoints())
    await device.model_save(ModelSave(filename="model.dat"))
    await device.model_load(ModelLoad(filename="model.dat"))

    assert ("model_add_point", (2.0, 30.0)) in api.calls
    assert ("model_delete_point", ((1, 2),)) in api.calls
    assert ("model_enable_point", ((1, 2),)) in api.calls
    assert ("model_disable_point", ((3,),)) in api.calls
    assert ("model_clear_points", ()) in api.calls
    assert ("model_save", ("model.dat",)) in api.calls
    assert ("model_load", ("model.dat",)) in api.calls

    await device.set_azimuth_wrap_range_min(SetAzimuthWrapRangeMin(min=10.0))
    assert ("set_axis0_wrap_range_min", (10.0,)) in api.calls

    await device.deinit(sk.Deinit())
    await device.shutdown()
    assert ("park", ()) in api.calls
    assert ("disable", (0,)) in api.calls
    assert ("disable", (1,)) in api.calls
    assert ("disconnect", ()) in api.calls
