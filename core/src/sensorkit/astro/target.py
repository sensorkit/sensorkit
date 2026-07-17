# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any, Literal, override

import astropy.units as u
import numpy as np
import satkit
from astropy.coordinates import (
    CIRS,
    ICRS,
    TEME,
    AltAz,
    EarthLocation,
    SkyCoord,
    get_body,
    get_sun,
)
from astropy.coordinates import AltAz as AltAzFrame
from astropy.time import Time
from loguru import logger
from pydantic import BaseModel, Discriminator

from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.coords import (
    Coordinates,
    Equatorial,
    Geodetic,
    Horizontal,
    StateVector,
)
from sensorkit.astro.trajectory import OrbitalTrajectory, TLETrajectory, Trajectory

if TYPE_CHECKING:
    from sensorkit.astro.observer import EarthObserver
    from sensorkit.std.weather import BasicWeather


class BaseTarget(BaseModel, ABC):
    """Abstract base for all target types; discriminated on `target_type`."""
    target_type: Literal[None]

    # FUTURE: When/if bounds for variadic generics are supported...
    # def adapt[*Ts: BaseTarget](self, *types: *tuple[type[*Ts], ...]) -> Union[*Ts]:

    async def adapt(
        self,
        *accepts: type[BaseTarget | Trajectory] | tuple,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
        _catalog: Any = None,
    ) -> BaseTarget | Trajectory:
        """Convert this target to the best matching type from *accepts*, propagating as needed."""
        mytype = type(self)
        supported: dict[type[BaseTarget | Trajectory], list[ReferenceFrame]] = collections.defaultdict(list)

        for val in accepts:
            match val:
                case (obj, *frames):
                    supported[obj].extend(frames)
                case obj:
                    supported[obj].clear()

        # Check whether this target is directly supported.
        if mytype in supported:
            # If it has a reference frame, check that too.
            if not supported[mytype] or self.frame in supported[mytype]:
                return self

        if mytype is CatalogTarget:
            # TODO: Do catalog lookup.
            raise NotImplementedError("Catalog lookup is not yet supported")

        assert observer is not None, "an observer location is required for trajectory propagation"

        # Prefer a path over a rate stream, if both are supported.
        if EphemerisTarget in supported:
            assert len(supported[EphemerisTarget]) == 1
            frame = supported[EphemerisTarget][0]

            logger.debug(f"adapting {type(self).__name__} to EphemerisTarget in {supported[EphemerisTarget]}")
            return await self.to_ephemeris_target(
                start_time=datetime.now(UTC),
                duration=timedelta(minutes=1),
                step=timedelta(seconds=2),
                frame=frame,
                observer=observer,
                weather=weather,
            )

        # Generate a rate stream if supported.
        if Trajectory in supported:
            logger.debug(f"adapting {type(self).__name__} to Trajectory in {supported[Trajectory]}")
            return self.to_trajectory()

        raise RuntimeError("Could not adapt target to a supported type")

    def to_trajectory(self) -> Trajectory:
        """Return a Trajectory for this target. Subclasses that support propagation override this."""
        raise NotImplementedError

    async def to_ephemeris_target(
        self,
        start_time: datetime,
        duration: timedelta,
        step: timedelta,
        frame: ReferenceFrame = ReferenceFrame.GCRF,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
    ):
        """Propagate this target into a pre-computed EphemerisTarget over the given time window."""
        trajectory = await self.to_trajectory().propagate(start_time + duration)
        location = observer.to_astropy() if observer else None

        # Define a function to construct the output frame with parameters it needs.
        # FIXME: Clean this up.
        match frame:
            case ReferenceFrame.ICRF:
                assert location is not None

                def transform_to_output_frame(gcrs: SkyCoord):
                    altaz = gcrs.transform_to(
                        AltAz(obstime=gcrs.obstime, location=location)
                    )
                    # FIXME: This returns the ICRF projected from the observer's location. This
                    #        is what we want, but it feels like this should be explicitly encoded
                    #        somehow so it's obvious that this isn't "real" ICRF.
                    return AltAz(
                        alt=altaz.alt,
                        az=altaz.az,
                        obstime=gcrs.obstime,
                        location=location,
                    ).transform_to(ICRS())
            case ReferenceFrame.CIRF:
                assert location is not None

                def transform_to_output_frame(gcrs: SkyCoord):
                    return gcrs.transform_to(CIRS(obstime=gcrs.obstime, location=location))
            case ReferenceFrame.ALTAZ:
                assert location is not None

                def transform_to_output_frame(gcrs: SkyCoord):
                    return gcrs.transform_to(
                        AltAz(
                            obstime=gcrs.obstime,
                            location=location,
                            pressure=weather.pressure if weather else None,
                            temperature=weather.temperature if weather else None,
                            relative_humidity=weather.humidity if weather else None,
                        )
                    )
            case _:
                def transform_to_output_frame(gcrs: SkyCoord):
                    return gcrs.transform_to(frame.to_astropy())

        # Build the output by sampling the propagated trajectory. The heavy lifting should have
        # already been done by the propagator, but the operations performed here can also amount to
        # significant work depending on the number of samples, the reference frame transform
        # being applied, and the details of the particular `sample()` implementation. We background
        # the entire loop to be safe.
        def _sample_series():
            t = start_time
            jds = []
            points = []
            coord = None

            for _ in range(duration // step):
                t += step
                gcrs = trajectory.sample(epoch=t)
                coord = transform_to_output_frame(gcrs)
                jds.append(gcrs.obstime.jd)
                points.append(Equatorial(ra=coord.ra.deg, dec=coord.dec.deg))

            if coord:
                logger.debug(f"generated EphemerisTarget ending at ra={coord.ra} dec={coord.dec}")

            return jds, points

        jds, points = await asyncio.to_thread(_sample_series)
        return EphemerisTarget(
            frame=frame,
            jds=jds,
            points=points,
        )


class CompositeTarget(BaseTarget):
    """A sequence of targets.

    The timing semantics of constituent targets are undefined.
    """
    target_type: Literal["composite"] = "composite"
    sequence: Sequence[BaseTarget]


class FrameTarget(BaseTarget):
    """An unspecified target in a particular reference frame.

    An object of type `FrameTarget` (as opposed to one of its subclasses) does not specify the
    position within the target reference frame. The default semantics in this case are that the
    target refers to the previous position in the user context converted to the target reference
    frame. If there is no previous position in this context, use of this target is considered an
    error.
    """
    target_type: Literal["frame"] = "frame"
    frame: ReferenceFrame


class FixedTarget[T: Coordinates](FrameTarget, ABC):
    """A target at a fixed position in the given reference frame."""
    target_type: Literal["fixed"] = "fixed"
    coords: T

    @abstractmethod
    def to_astropy(self, **kwargs) -> SkyCoord:
        """Convert this fixed target to an astropy SkyCoord."""
        ...


class AltAzTarget(FixedTarget[Horizontal]):
    """A target at a fixed position in the alt-azimuth frame."""
    frame: Literal[ReferenceFrame.ALTAZ] = ReferenceFrame.ALTAZ

    @override
    def to_astropy(
        self,
        time: Time | None = None,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
        wavelength: float | None = None,
    ):
        return self.coords.to_astropy(
            obstime=time,
            location=EarthLocation(observer.to_astropy()) if observer else None,
            pressure=weather.pressure if weather else None,  # FIXME: check units; expects hPa
            temperature=weather.temperature if weather else None,
            relative_humidity=weather.humidity if weather else None,
            obswl=wavelength,
        )


class ICRSTarget(FixedTarget[Equatorial]):
    """A target at a fixed position in the International Celestial Reference Frame."""
    frame: Literal[ReferenceFrame.ICRF] = ReferenceFrame.ICRF

    @override
    def to_astropy(
        self,
        time: Time | None = None,
    ):
        return self.coords.to_astropy(frame=self.frame, obstime=time)


class RateTarget(FrameTarget):
    """A target moving at a fixed rate relative to an initial position."""
    target_type: Literal["rate"] = "rate"
    rates: Coordinates
    initial_time: datetime
    initial_frame: ReferenceFrame
    initial_coords: Coordinates


class EphemerisTarget(FrameTarget):
    """A target moving according to a precomputed ephemeris."""
    target_type: Literal["ephemeris"] = "ephemeris"
    jds: Sequence[float]
    points: Sequence[Coordinates]
    # FIXME: Needs velocity too.


class TLETarget(FrameTarget):
    """A target moving according to the input Two-Line Element set."""
    target_type: Literal["tle"] = "tle"
    frame: Literal[ReferenceFrame.TEME] = ReferenceFrame.TEME
    tle: TLE

    @override
    def to_trajectory(self):
        return TLETrajectory(self.tle)


class StateVectorTarget(FrameTarget):
    """A target moving according to the input state vector.

    Position units must be meters, and velocity units must be meters per second.
    """
    target_type: Literal["state_vector"] = "state_vector"
    sv: StateVector

    @override
    def to_trajectory(self):
        return OrbitalTrajectory(self.sv_gcrf())

    def sv_gcrf(self):
        """Return the state vector converted to GCRF, transforming if necessary."""
        if self.frame == ReferenceFrame.GCRF:
            return self.sv

        return StateVector.from_astropy(
            self.sv.to_astropy(frame=self.frame).transform_to("gcrs")
        )


class CatalogTarget(BaseTarget):
    """A target identified by name in an astronomical catalog."""
    target_type: Literal["catalog"] = "catalog"
    object: str


Target = Annotated[
    Annotated[AltAzTarget | ICRSTarget, Discriminator("frame")]
    | RateTarget
    | EphemerisTarget
    | TLETarget
    | StateVectorTarget
    | CatalogTarget
    | FrameTarget,
    Discriminator("target_type"),
]


@dataclass
class ObserveWindow:
    """A time window for observability calculations, sampled at *step_seconds* intervals."""
    start_time: datetime
    end_time: datetime
    step_seconds: int = 60


def time_grid(win: ObserveWindow) -> list[datetime]:
    """Return a list of timezone-aware datetimes spanning the window at the configured step."""
    if win.start_time.tzinfo is None or win.end_time.tzinfo is None:
        raise ValueError("start_time and end_time must be timezone-aware (UTC).")
    if win.end_time < win.start_time:
        raise ValueError("end_time must be >= start_time.")
    if win.step_seconds <= 0:
        raise ValueError("step_seconds must be > 0.")

    t = win.start_time
    out: list[datetime] = []
    while t <= win.end_time:
        out.append(t)
        t += timedelta(seconds=win.step_seconds)
    return out


def make_altaz_frame(loc: EarthLocation, obstime: Time) -> AltAzFrame:
    """Return an astropy AltAz frame for the given location and time."""
    return AltAzFrame(obstime=obstime, location=loc)

def norm_az_deg(az_deg: float) -> float:
    """Normalise an azimuth value to the [0, 360) degree range."""
    return az_deg % 360.0


def sample_altaz_series(
    target: Target,
    loc: EarthLocation,
    times: list[datetime],
) -> list[tuple[float, float]]:
    """Return a list of `(altitude_deg, azimuth_deg)` tuples for the target at each time."""
    out: list[tuple[float, float]] = []

    match target:
        case ICRSTarget():
            sc = SkyCoord(
                ra=target.coords.ra * u.deg,
                dec=target.coords.dec * u.deg,
                frame=ICRS(),
            )
            for dt in times:
                t_ast = Time(dt, scale="utc")
                a = sc.transform_to(make_altaz_frame(loc, t_ast))
                out.append((a.alt.deg, norm_az_deg(a.az.deg)))
            return out

        case AltAzTarget():
            az = norm_az_deg(target.coords.az)
            return [(target.coords.alt, az) for _ in times]

        case TLETarget():
            tle = satkit.TLE.from_lines(target.tle.to_list())
            for dt in times:
                t_ast = Time(dt, scale="utc")
                t_sk = satkit.time.from_datetime(dt)
                try:
                    teme_p, _teme_v = satkit.sgp4(tle, t_sk)
                except Exception:
                    out.append((-90.0, 0.0))
                    continue
                teme = TEME(x=teme_p[0]*u.km, y=teme_p[1]*u.km, z=teme_p[2]*u.km, obstime=t_ast)
                a = teme.transform_to(make_altaz_frame(loc, t_ast))
                out.append((a.alt.deg, norm_az_deg(a.az.deg)))
            return out

    raise TypeError(f"Unsupported target type: {type(target)}")


def altitude_mask(altaz: list[tuple[float, float]], *, min_altitude_deg: float) -> np.ndarray:
    """Return a boolean mask that is True where altitude >= *min_altitude_deg*."""
    return np.array([alt >= min_altitude_deg for alt, _ in altaz], dtype=bool)

def darkness_mask(
    times: list[datetime],
    loc: EarthLocation,
    max_sun_alt_deg: float,
) -> np.ndarray:
    """Return a boolean mask that is True where the Sun is at or below *max_sun_alt_deg*."""
    mask = np.zeros(len(times), dtype=bool)
    for i, dt in enumerate(times):
        t_ast = Time(dt, scale="utc")
        sun_alt = get_sun(t_ast).transform_to(make_altaz_frame(loc, t_ast)).alt.deg
        mask[i] = (sun_alt <= max_sun_alt_deg)
    return mask


def sun_avoidance_mask(
    altaz: list[tuple[float, float]],
    times: list[datetime],
    loc: EarthLocation,
    min_sun_sep_deg: float,
) -> np.ndarray:
    """Return a boolean mask that is True where the target is at least *min_sun_sep_deg* from the Sun."""
    mask = np.zeros(len(times), dtype=bool)
    for i, dt in enumerate(times):
        alt_deg, az_deg = altaz[i]
        t_ast = Time(dt, scale="utc")
        frame = make_altaz_frame(loc, t_ast)
        tgt = SkyCoord(AltAzFrame(az=az_deg * u.deg, alt=alt_deg * u.deg, obstime=t_ast, location=loc))
        sun = get_sun(t_ast).transform_to(frame)
        mask[i] = tgt.separation(sun).deg >= min_sun_sep_deg
    return mask

def moon_avoidance_mask(
    altaz: list[tuple[float, float]],
    times: list[datetime],
    loc: EarthLocation,
    min_moon_sep_deg: float,
) -> np.ndarray:
    """Return a boolean mask that is True where the target is at least *min_moon_sep_deg* from the Moon."""
    mask = np.zeros(len(times), dtype=bool)
    for i, dt in enumerate(times):
        alt_deg, az_deg = altaz[i]
        t_ast = Time(dt, scale="utc")
        frame = make_altaz_frame(loc, t_ast)
        tgt = SkyCoord(AltAzFrame(az=az_deg * u.deg, alt=alt_deg * u.deg, obstime=t_ast, location=loc))
        moon = get_body("moon", t_ast, location=loc).transform_to(frame)
        mask[i] = tgt.separation(moon).deg >= min_moon_sep_deg
    return mask

def is_observable(
    target: Target,
    site: Geodetic,
    start_time: datetime,
    end_time: datetime,
    step_seconds: int = 10,
    min_altitude_deg: float | None = None,
    sun_max_altitude_deg: float | None = None,
    sun_separation_deg: float | None = None,
    moon_separation_deg: float | None = None,
) -> bool:
    """Return True if the target satisfies all supplied observability constraints throughout the window."""
    win = ObserveWindow(start_time, end_time, step_seconds)

    times = time_grid(win)
    if not times:
        return False

    loc = EarthLocation(site.to_astropy())

    try:
        altaz = sample_altaz_series(target, loc, times)
    except TypeError:
        logger.warning(f"Observability check for {type(target).__name__} is not implemented!")
        return True

    mask = np.ones(len(times), dtype=bool)

    if min_altitude_deg is not None:
        mask &= altitude_mask(altaz, min_altitude_deg=min_altitude_deg)

    if sun_max_altitude_deg is not None:
        mask &= darkness_mask(times, loc, max_sun_alt_deg=sun_max_altitude_deg)

    if sun_separation_deg is not None:
        mask &= sun_avoidance_mask(altaz, times, loc, min_sun_sep_deg=sun_separation_deg)

    if moon_separation_deg is not None:
        mask &= moon_avoidance_mask(altaz, times, loc, min_moon_sep_deg=moon_separation_deg)

    return np.all(mask)


