from __future__ import annotations

import asyncio
from typing import Literal

from astropy import units as u
from astropy.coordinates import ICRS, AltAz as AstropyAltAz, EarthLocation, SkyCoord
from astropy.time import Time
from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import Geodetic
from sensorkit.astro.target import AltAzTarget, EphemerisTarget, FrameTarget, ICRSTarget, TLETarget
from sensorkit.models.devices import (
    AltAzArcseconds,
    AltAzPointing,
    ApplyOffset,
    AxisEnabled,
    AxisRate,
    AxisRates,
    AzimuthWrapRange,
    Connected,
    ModelAddPoint,
    ModelClearPoints,
    ModelDeletePoint,
    ModelDisablePoint,
    ModelEnablePoint,
    ModelLoad,
    ModelSave,
    MountAxis,
    RADecArcseconds,
    RADecPointing,
    ReferenceFrame,
    SetAzimuthWrapRangeMin,
    SetParkPosition,
)
from sensorkit.pwi4._async_driver import AsyncPWI4
from sensorkit.pwi4.api import PWI4, MountAPI
from sensorkit.pwi4.models import PWI4StatusModel


class MountConfig(BaseModel):
    """Configuration specific to mount devices."""
    device_type: Literal["mount"] = "mount"
    entity: str
    status_frequency: float = 1.0

    park_absolute: bool = False
    park_axis0_degrees: float | None = None
    park_axis1_degrees: float | None = None

    disable_axis_on_deinit: bool = False

@sk.declare_keyword
class MountAxisRates(BaseModel):
    axis: list[AxisRate]

@sk.declare_keyword
class MountAxisEnabled(BaseModel):
    axis: list[AxisEnabled]

@sk.declare_keyword
class MountTargetDistance(BaseModel):
    axis: list[sk.AxisTargetDistance]

class CustomPathNew(sk.DeviceCommand):
    command_id: Literal["CustomPathNew"] = "CustomPathNew"
    coord_type: str


class CustomPathAddPointList(sk.DeviceCommand):
    command_id: Literal["CustomPathAddPointList"] = "CustomPathAddPointList"
    points: list[tuple[float, float, float]]  # (jdUTC, altDeg, azDeg)


class CustomPathApply(sk.DeviceCommand):
    command_id: Literal["CustomPathApply"] = "CustomPathApply"
    update_wrap: bool = False


class SetSlewTimeConstant(sk.DeviceCommand):
    command_id: Literal["SetSlewTimeConstant"] = "SetSlewTimeConstant"
    value: float = Field(...)


@sk.declare_device
class MountDevice:

    def __init__(
        self,
        pwi: PWI4,
        config: MountConfig,
    ):
        self.mount: MountAPI = pwi.mount
        self.config = config
        self.telemetry: MountTelemetryPublisher | None = None

    @sk.on_attach
    async def startup(self):
        try:
            async with asyncio.timeout(10.0):
                await self.mount.connect()

                # Fetch initial status before any commands can arrive.
                self.current_status = await self.mount.status()

            # Start polling status.
            self._monitor_task = asyncio.create_task(self.monitor())

            self.telemetry = MountTelemetryPublisher(
                binding=sk.device(),
                api=self.mount,
                frequency=self.config.status_frequency,
            )
            await self.telemetry.start()
        except Exception as e:
            logger.exception("PWI4 mount startup failed")
            raise RuntimeError("Mount startup failed") from e

    @sk.on_detach
    async def shutdown(self):
        try:
            await self.mount.disconnect()
            await self.telemetry.stop()
        except Exception:
            logger.exception("PWI4 mount shutdown encountered an error")

    async def monitor(self):
        backoff = 1.0
        while True:
            try:
                self.current_status = await self.mount.status()
                backoff = 1.0
                await asyncio.sleep(self.config.status_frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"PWI4 status polling failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def park_mount(self):
        if self.config.park_absolute:
            await self.mount.goto_raw(
                self.config.park_axis0_degrees,
                self.config.park_axis1_degrees,
            )
            await self.mount.stop()
            await self.mount.set_park_here()
        else:
            await self.mount.park()

    @sk.command_handler
    async def connect(self, cmd: sk.Connect):
        try:
            await self.mount.connect()
        except Exception as e:
            logger.exception("Mount connect failed")
            raise RuntimeError("Connect failed") from e

    @sk.command_handler
    async def disconnect(self, cmd: sk.Disconnect):
        try:
            await self.mount.disconnect()
        except Exception as e:
            logger.exception("Mount disconnect failed")
            raise RuntimeError("Disconnect failed") from e

    @sk.command_handler
    async def init(self, cmd: sk.Init):
        try:
            await self.mount.connect()
            await asyncio.gather(
                self.mount.enable(0),
                self.mount.enable(1)
            )

            status: PWI4StatusModel = await self.mount.status()
            if (
                not status.mount.axis0.is_position_initialized
                or not status.mount.axis1.is_position_initialized
            ):
                await self.mount.find_home()
        except Exception as e:
            logger.exception("Mount init failed")
            raise RuntimeError("Init failed") from e

    @sk.command_handler
    async def deinit(self, cmd: sk.Deinit):
        try:
            await self.park_mount()

            if self.config.disable_axis_on_deinit:
                await asyncio.gather(
                    self.mount.disable(0),
                    self.mount.disable(1)
                )
        except Exception as e:
            logger.exception("Mount deinit failed")
            raise RuntimeError("Deinit failed") from e


    @sk.command_handler
    async def enable_altitude(self, cmd: sk.EnableAxis):
        try:
            if cmd.axis == MountAxis.ALTITUDE:
                await self.mount.enable(1)
            elif cmd.axis == MountAxis.AZIMUTH:
                await self.mount.enable(0)
        except Exception as e:
            logger.exception("Enable axis failed")
            raise RuntimeError("EnableAxis failed") from e

    @sk.command_handler
    async def disable_axis(self, cmd: sk.DisableAxis):
        try:
            if cmd.axis == MountAxis.ALTITUDE:
                await self.mount.disable(1)
            elif cmd.axis == MountAxis.AZIMUTH:
                await self.mount.disable(0)
        except Exception as e:
            logger.exception("Disable axis failed")
            raise RuntimeError("DisableAxis failed") from e

    @sk.command_handler
    async def follow_target(self, cmd: sk.FollowTarget):
        target = await cmd.target.adapt(
            ICRSTarget,
            AltAzTarget,
            (FrameTarget, ReferenceFrame.ICRF),
            (FrameTarget, ReferenceFrame.ALTAZ),
            TLETarget,
            (EphemerisTarget, ReferenceFrame.ICRF),
            observer=Geodetic(
                lon=self.current_status.site.longitude_degs,
                lat=self.current_status.site.latitude_degs,
                elev=self.current_status.site.height_meters / 1000,
            )
        )

        match target:
            case ICRSTarget():
                await self.mount.goto_ra_dec_j2000(
                    ra_hours=target.coords.ra,
                    dec_degs=target.coords.dec,
                )
            case AltAzTarget():
                await self.mount.goto_alt_az(
                    alt_degs=target.coords.alt,
                    az_degs=target.coords.az,
                )
            case TLETarget():
                await self.mount.follow_tle(
                    line1=target.tle.line0,
                    line2=target.tle.line1,
                    line3=target.tle.line2,
                )
            case EphemerisTarget():
                await self.mount.radecpath_new()

                for i in range(len(target.jds)):
                    await self.mount.radecpath_add_point(
                        jd=target.jds[i],
                        ra_j2000_hours=target.points[i].ra / 15,
                        dec_j2000_degs=target.points[i].dec,
                    )

                await self.mount.radecpath_apply()
            case FrameTarget():
                match cmd.target.frame:
                    case ReferenceFrame.ALTAZ:
                        await self.mount.tracking_off()
                    case ReferenceFrame.ICRF:
                        await self.mount.tracking_on()
                    case _:
                        raise RuntimeError(f"Need specific target to track {cmd.target.frame}")


    @sk.command_handler
    async def park(self, cmd: sk.MoveToPark):
        try:
            await self.park_mount()

        except Exception as e:
            logger.exception("MoveToPark failed")
            raise RuntimeError("MoveToPark failed") from e

    @sk.command_handler
    async def set_park_here(self, cmd: SetParkPosition):
        try:
            # Move to requested park position before setting it
            await self.mount.goto_alt_az(
                alt_degs=cmd.altitude_degrees, az_degs=getattr(cmd, "azimuth_degrees", None) or cmd.azimuth_degrees, tolerance_deg=0.05
            )
            await self.mount.set_park_here()
        except Exception as e:
            logger.exception("SetParkPosition failed")
            raise RuntimeError("SetParkPosition failed") from e

    @sk.command_handler
    async def move_home(self, cmd: sk.Home):
        try:
            await self.mount.find_home()
        except Exception as e:
            logger.exception("Home failed")
            raise RuntimeError("Home failed") from e

    @sk.command_handler
    async def halt(self, cmd: sk.Stop):
        try:
            await self.mount.stop()
        except Exception as e:
            logger.exception("Stop failed")
            raise RuntimeError("Stop failed") from e

    @sk.command_handler
    async def offset(self, cmd: ApplyOffset):
        try:
            if isinstance(cmd.offset, RADecArcseconds):
                await self.mount.offset(
                    ra_arcsec=cmd.offset.right_ascension_arcseconds,
                    dec_arcsec=cmd.offset.declination_arcseconds,
                )
            elif isinstance(cmd.offset, AltAzArcseconds):
                await self.mount.offset(
                    axis0_arcsec=cmd.offset.azimuth_arcseconds,
                    axis1_arcsec=cmd.offset.altitude_arcseconds,
                )
            else:
                raise ValueError("Unsupported offset type")
        except Exception as e:
            logger.exception("ApplyOffset failed")
            raise RuntimeError("ApplyOffset failed") from e

    @sk.command_handler
    async def set_slew_time_constant(self, cmd: SetSlewTimeConstant):
        try:
            await self.mount.set_slew_time_constant(cmd.value)
        except Exception as e:
            logger.exception("SetSlewTimeConstant failed")
            raise RuntimeError("SetSlewTimeConstant failed") from e

    @sk.command_handler
    async def set_azimuth_wrap_range_min(self, cmd: SetAzimuthWrapRangeMin):
        try:
            await self.mount.set_axis0_wrap_range_min(cmd.min)
            await sk.device().publish(
                AzimuthWrapRange(min=cmd.min, max=(cmd.min + 360))
            )
        except Exception as e:
            logger.exception("SetAzimuthWrapRangeMin failed")
            raise RuntimeError("SetAzimuthWrapRangeMin failed") from e

    @sk.command_handler
    async def model_add_point(self, cmd: ModelAddPoint):
        if cmd.reference_frame == ReferenceFrame.ICRF:
            await self.mount.model_add_point(
                cmd.right_ascension_hours, cmd.declination_degrees
            )
        else:
            raise ValueError("Mount model points must be ICRF")

    @sk.command_handler
    async def model_delete_point(self, cmd: ModelDeletePoint):
        try:
            await self.mount.model_delete_point(*cmd.indexes)
        except Exception as e:
            logger.exception("ModelDeletePoint failed")
            raise RuntimeError("ModelDeletePoint failed") from e

    @sk.command_handler
    async def model_enable_point(self, cmd: ModelEnablePoint):
        try:
            await self.mount.model_enable_point(*cmd.indexes)
        except Exception as e:
            logger.exception("ModelEnablePoint failed")
            raise RuntimeError("ModelEnablePoint failed") from e

    @sk.command_handler
    async def model_disable_point(self, cmd: ModelDisablePoint):
        try:
            await self.mount.model_disable_point(*cmd.indexes)
        except Exception as e:
            logger.exception("ModelDisablePoint failed")
            raise RuntimeError("ModelDisablePoint failed") from e

    @sk.command_handler
    async def model_clear_points(self, cmd: ModelClearPoints):
        try:
            await self.mount.model_clear_points()
        except Exception as e:
            logger.exception("ModelClearPoints failed")
            raise RuntimeError("ModelClearPoints failed") from e

    @sk.command_handler
    async def model_save(self, cmd: ModelSave):
        try:
            await self.mount.model_save(cmd.filename)
        except Exception as e:
            logger.exception("ModelSave failed")
            raise RuntimeError("ModelSave failed") from e

    @sk.command_handler
    async def model_load(self, cmd: ModelLoad):
        try:
            await self.mount.model_load(cmd.filename)
        except Exception as e:
            logger.exception("ModelLoad failed")
            raise RuntimeError("ModelLoad failed") from e

class MountTelemetryPublisher:
    def __init__(self, *, binding: sk.DeviceImpl, api: AsyncPWI4, frequency: float = 1.0):
        self.binding = binding
        self.api = api
        self.frequency = frequency
        self._task: asyncio.Task | None = None

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._task.cancel()

    async def _run(self):
        backoff = 1.0
        while True:
            try:
                status: PWI4StatusModel = await self.api.status()
                backoff = 1.0
                await asyncio.gather(
                    self.publish_altaz_pointing(status),
                    self.publish_axisrates(status),
                    self.publish_connected(status),
                    self.publish_radec_pointing(status),
                    self.publish_sensor_position(status),
                    self.publish_tracking_error(status),
                    self.publish_axis_enabled(status),
                )
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"PWI4 status polling failed: {e}")
                # Exponential backoff up to 10s
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_sensor_position(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.SitePosition(
                    latitude_degrees=s.site.latitude_degs,
                    longitude_degrees=s.site.longitude_degs,
                    altitude_km=((s.site.height_meters) / 1000),
                )
            )
        except Exception:
            return None

    async def publish_altaz_pointing(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(AltAzPointing(
                altitude_degrees=s.mount.altitude_degs,
                azimuth_degrees=s.mount.azimuth_degs,
            ))
        except Exception:
            return None

    async def publish_radec_pointing(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(RADecPointing(
                right_ascension_hours=s.mount.ra_j2000_hours,
                declination_degrees=s.mount.dec_j2000_degs,
                reference_frame=ReferenceFrame.ICRF,
            ))
        except Exception:
            return None


    async def publish_tracking_error(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                MountTargetDistance(
                    axis=[
                        sk.AxisTargetDistance(
                            distance_arcseconds=s.mount.axis0.dist_to_target_arcsec,
                            rms_error_arcseconds=s.mount.axis0.rms_error_arcsec,
                            axis=sk.MountAxis.AZIMUTH
                        ),
                        sk.AxisTargetDistance(
                            distance_arcseconds=s.mount.axis1.dist_to_target_arcsec,
                            rms_error_arcseconds=s.mount.axis1.rms_error_arcsec,
                            axis=sk.MountAxis.ALTITUDE
                        )
                    ]
                )
            )

        except Exception:
            return None


    async def publish_axisrates(self, s: PWI4StatusModel):
        try:
            az_rate_dps, alt_rate_dps, t = (
                s.mount.axis0.measured_velocity_degs_per_sec,
                s.mount.axis1.measured_velocity_degs_per_sec,
                Time.now()
            )

            _, _, ra_rate_dps, dec_rate_dps = self.altaz_rates_to_radec_rates(
                alt_deg=s.mount.altitude_degs,
                az_deg=s.mount.azimuth_degs,
                alt_rate_deg_per_sec=alt_rate_dps,
                az_rate_deg_per_sec=az_rate_dps,
                time=t,
                location=EarthLocation.from_geodetic(
                    lon=s.site.longitude_degs,
                    lat=s.site.latitude_degs,
                    height=s.site.height_meters
                ),
            )

            await self.binding.publish(
                AxisRates(
                    azimuth=AxisRate(
                        max_acceleration=s.mount.axis0.acceleration_degs_per_sec_sqr,
                        velocity=s.mount.axis0.measured_velocity_degs_per_sec,
                        max_velocity=s.mount.axis0.max_velocity_degs_per_sec,
                        mechanical_position=s.mount.axis0.position_degs,
                        min_mechanical_position=s.mount.axis0.min_mech_position_degs,
                        max_mechanical_position=s.mount.axis0.max_mech_position_degs,
                        axis=sk.MountAxis.AZIMUTH,
                    ),
                    altitude=AxisRate(
                        max_velocity=s.mount.axis1.max_velocity_degs_per_sec,
                        max_acceleration=s.mount.axis1.acceleration_degs_per_sec_sqr,
                        velocity=s.mount.axis1.measured_velocity_degs_per_sec,
                        mechanical_position=s.mount.axis1.position_degs,
                        min_mechanical_position=s.mount.axis1.min_mech_position_degs,
                        max_mechanical_position=s.mount.axis1.max_mech_position_degs,
                        axis=sk.MountAxis.ALTITUDE,
                    ),
                    declination=AxisRate(
                        velocity=dec_rate_dps,
                        axis=sk.MountAxis.DECLINATION,
                    ),
                    right_ascension=AxisRate(
                        velocity=ra_rate_dps,
                        axis=sk.MountAxis.RIGHT_ASCENSION,
                    ),
                )
            )
        except Exception:
            return None

    async def publish_axis_enabled(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                MountAxisEnabled(
                    axis=[
                        AxisEnabled(axis=sk.MountAxis.AZIMUTH, enabled=s.mount.axis0.is_enabled),
                        AxisEnabled(axis=sk.MountAxis.ALTITUDE, enabled=s.mount.axis1.is_enabled)
                    ]
                )
            )
        except Exception:
            return None

    async def publish_connected(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                Connected(is_connected=bool(s.mount.is_connected) if s else False)
            )
        except Exception:
            await self.binding.publish(
                Connected(is_connected=False)
            )

    def altaz_rates_to_radec_rates(
            self,
            alt_deg: float,
            az_deg: float,
            alt_rate_deg_per_sec: float,
            az_rate_deg_per_sec: float,
            location: EarthLocation,
            time: Time,
    ) -> tuple[float, float, float, float]:
        """
        Convert Alt/Az position and angular rates (deg/sec) into RA/Dec position and rates.

        Returns
        -------
        ra_deg : float
            Right ascension (degrees)
        dec_deg : float
            Declination (degrees)
        ra_rate_deg_per_sec : float
            d(RA)/dt in deg/sec
        dec_rate_deg_per_sec : float
            d(Dec)/dt in deg/sec
        """

        # Current AltAz coordinate
        altaz_frame = AstropyAltAz(obstime=time, location=location)
        coord_altaz = SkyCoord(alt=alt_deg * u.deg, az=az_deg * u.deg, frame=altaz_frame)

        # Convert to RA/Dec
        coord_radec = coord_altaz.transform_to(ICRS())
        ra_deg = coord_radec.ra.deg
        dec_deg = coord_radec.dec.deg

        # === Numerical derivative for rates ===
        dt = 0.01  # seconds

        # New alt/az after dt
        new_alt = alt_deg + alt_rate_deg_per_sec * dt
        new_az = az_deg + az_rate_deg_per_sec * dt

        # Create coordinate at new Alt/Az
        new_coord_altaz = SkyCoord(alt=new_alt * u.deg, az=new_az * u.deg, frame=altaz_frame)

        # Transform to RA/Dec
        new_coord_radec = new_coord_altaz.transform_to(ICRS())

        # Compute rates in deg/sec
        ra_rate_deg_per_sec = (new_coord_radec.ra.deg - ra_deg) / dt
        dec_rate_deg_per_sec = (new_coord_radec.dec.deg - dec_deg) / dt

        return ra_deg, dec_deg, ra_rate_deg_per_sec, dec_rate_deg_per_sec
