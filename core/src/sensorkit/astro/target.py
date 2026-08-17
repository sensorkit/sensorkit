# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any, Literal, override

import astropy.units as u
import numpy as np
import satkit
from astropy.coordinates import (
    CIRS,
    GCRS,
    ICRS,
    TEME,
    AltAz,
    BaseCoordinateFrame,
    CartesianRepresentation,
    EarthLocation,
    SkyCoord,
    UnitSphericalRepresentation,
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


# Frames built from the site-to-target vector, and so unusable without an observer.
_OBSERVER_RELATIVE_FRAMES = (
    ReferenceFrame.ALTAZ,
    ReferenceFrame.CIRF,
    ReferenceFrame.ICRF,
)

# How far past a requested window a track propagates, so that successive windows are
# usually served without propagating again.
_PROPAGATION_HORIZON = timedelta(hours=1)

# Window covering consumers that ask adapt() for a materialized EphemerisTarget. A track
# leaves this choice to the consumer; these apply only where one is not accepted.
_ADAPTED_EPHEMERIS_DURATION = timedelta(minutes=1)
_ADAPTED_EPHEMERIS_STEP = timedelta(seconds=2)


def _topocentric_cartesian(gcrs: GCRS, location: EarthLocation) -> CartesianRepresentation:
    """Return the site-to-target vector in GCRS axes.

    Relies on *gcrs* carrying the default geocentric origin; a GCRS with a non-zero
    obsgeoloc is already observer-relative and would be shifted twice.
    """
    obsgeoloc, _ = location.get_gcrs_posvel(gcrs.obstime)
    return gcrs.cartesian.without_differentials() - obsgeoloc


def _topocentric_icrs(gcrs: GCRS, location: EarthLocation) -> ICRS:
    return ICRS(
        _topocentric_cartesian(gcrs, location).represent_as(UnitSphericalRepresentation)
    )


def _topocentric_cirs(gcrs: GCRS, location: EarthLocation) -> CIRS:
    topo = _topocentric_cartesian(gcrs, location)
    cirs = GCRS(topo, obstime=gcrs.obstime).transform_to(CIRS(obstime=gcrs.obstime))
    return CIRS(cirs.cartesian, obstime=gcrs.obstime, location=location)


def _output_frame_transform(
    frame: ReferenceFrame,
    location: EarthLocation | None,
    weather: BasicWeather | None,
) -> Callable[[GCRS], BaseCoordinateFrame]:
    """Return the GCRS-to-*frame* conversion used to build ephemeris points.

    ICRF, CIRF and ALTAZ are observer-relative and are all built from the topocentric
    vector; anything else is a geocentric frame astropy can reach from GCRS on its own.
    """
    if location is None and frame in _OBSERVER_RELATIVE_FRAMES:
        raise RuntimeError(f"an observer location is required for {frame}")

    match frame:
        case ReferenceFrame.ICRF:
            return lambda gcrs: _topocentric_icrs(gcrs, location)
        case ReferenceFrame.CIRF:
            return lambda gcrs: _topocentric_cirs(gcrs, location)
        case ReferenceFrame.ALTAZ:
            return lambda gcrs: _topocentric_cirs(gcrs, location).transform_to(
                AltAz(
                    obstime=gcrs.obstime,
                    location=location,
                    **(weather.to_astropy() if weather else {}),
                )
            )
        case _:
            output_frame = frame.to_astropy()

            if "obstime" in output_frame.frame_attributes:
                return lambda gcrs: gcrs.transform_to(output_frame(obstime=gcrs.obstime))

            return lambda gcrs: gcrs.transform_to(output_frame())


def _ephemeris_points(frame: ReferenceFrame, coord: BaseCoordinateFrame) -> list[Coordinates]:
    """Pack a sampled coordinate into the point type native to *frame*.

    Equatorial frames expose ra/dec directly, but ITRF and TEME carry cartesian data and
    have to be resolved first: ITRF to the sub-satellite geodetic point, TEME to the
    spherical angles its cartesian representation implies.
    """
    match frame:
        case ReferenceFrame.ALTAZ:
            return [
                Horizontal(az=az, alt=alt)
                for az, alt in zip(coord.az.deg, coord.alt.deg, strict=True)
            ]
        case ReferenceFrame.ITRF:
            lons, lats, heights = coord.earth_location.to_geodetic()
            return [
                Geodetic(lon=lon, lat=lat, elev=elev)
                for lon, lat, elev in zip(
                    lons.deg, lats.deg, heights.to_value(u.m), strict=True
                )
            ]
        case ReferenceFrame.TEME:
            usph = coord.represent_as(UnitSphericalRepresentation)
            return [
                Equatorial(ra=ra, dec=dec)
                for ra, dec in zip(usph.lon.deg, usph.lat.deg, strict=True)
            ]
        case _:
            return [
                Equatorial(ra=ra, dec=dec)
                for ra, dec in zip(coord.ra.deg, coord.dec.deg, strict=True)
            ]


class TargetTrack:
    """The unbounded on-sky path of a single propagatable target."""

    def __init__(
        self,
        trajectory: Trajectory,
        frame: ReferenceFrame,
        *,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
        propagation_horizon: timedelta = _PROPAGATION_HORIZON,
    ):
        """Bind *trajectory* to the frame and observing context it is sampled in.

        Args:
            trajectory: The path to sample, propagated in GCRS.
            frame: Reference frame the sampled points are returned in.
            observer: Site position. Required for observer-relative frames.
            weather: Ambient conditions, applied as refraction in ALTAZ.
            propagation_horizon: How far past a requested window to propagate.
        """
        self.trajectory = trajectory
        self.frame = frame
        self.observer = observer
        self.weather = weather
        self.propagation_horizon = propagation_horizon
        self._propagated: Trajectory | None = None
        self._propagated_until: datetime | None = None

    async def sample(
        self,
        start_time: datetime,
        duration: timedelta,
        step: timedelta,
    ) -> tuple[list[float], list[Coordinates]]:
        """Sample the track over a window, returning Julian dates and points in `frame`.

        The first sample sits one *step* after *start_time*, and the last at
        *start_time* plus *duration*.
        """
        trajectory = await self._propagate_through(start_time + duration)
        location = self.observer.to_astropy() if self.observer else None
        transform_to_output_frame = _output_frame_transform(self.frame, location, self.weather)

        def _sample_series():
            epochs = [start_time + (i + 1) * step for i in range(duration // step)]

            if not epochs:
                return [], []

            gcrs = trajectory.sample(epochs)
            coord = transform_to_output_frame(gcrs)
            jds = list(gcrs.obstime.jd)
            points = _ephemeris_points(self.frame, coord)
            logger.debug(f"sampled track ending at {points[-1]}")

            return jds, points

        return await asyncio.to_thread(_sample_series)

    async def stream(
        self,
        *,
        window: timedelta,
        step: timedelta,
        lead: timedelta,
    ) -> AsyncIterator[tuple[list[float], list[Coordinates]]]:
        """Yield consecutive windows of the track, indefinitely.

        Each iteration yields the next *window* of samples, then waits until *lead*
        remains before that window elapses. A consumer therefore always holds samples
        reaching at least *lead* into the future, and both producing the next window
        and acting on it have to fit within *lead*.

        Args:
            window: Time span covered by each batch of samples.
            step: Interval between samples within a batch.
            lead: How long before a window elapses to produce the next one.

        Raises:
            ValueError: If *lead* leaves no time to consume a window.
        """
        if lead >= window:
            raise ValueError("lead must be shorter than window")

        while True:
            start = datetime.now(UTC) - step
            yield await self.sample(start, window, step)

            delay = (start + window - lead - datetime.now(UTC)).total_seconds()

            if delay > 0:
                await asyncio.sleep(delay)

    async def _propagate_through(self, until: datetime) -> Trajectory:
        """Return a trajectory propagated at least as far as *until*."""
        if self._propagated is None or until > self._propagated_until:
            self._propagated_until = until + self.propagation_horizon
            # Propagate from the original trajectory rather than the last result, which
            # would accumulate error over a long-running track.
            self._propagated = await self.trajectory.propagate(self._propagated_until)

        return self._propagated


def _accepted_frame(
    accepted: type,
    supported: dict[type, list[ReferenceFrame]],
    observer: EarthObserver | Geodetic | None,
) -> ReferenceFrame:
    """Return the single frame requested for *accepted*, rejecting an unusable request."""
    frames = supported[accepted]

    if len(frames) != 1:
        raise RuntimeError(f"exactly one reference frame is required for {accepted.__name__}")

    frame = frames[0]

    if observer is None and frame in _OBSERVER_RELATIVE_FRAMES:
        raise RuntimeError(f"an observer location is required for {frame}")

    return frame


class BaseTarget(BaseModel, ABC):
    """Abstract base for all target types; discriminated on `target_type`."""
    target_type: Literal[None]

    async def adapt(
        self,
        *accepts: type[BaseTarget | TargetTrack] | tuple,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
        _catalog: Any = None,
    ) -> BaseTarget | TargetTrack:
        """Convert this target to the best matching type from *accepts*, propagating as needed."""
        mytype = type(self)
        supported: dict[type[BaseTarget | TargetTrack], list[ReferenceFrame]] = collections.defaultdict(list)

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

        # Fixed and rate targets have no trajectory and fall through to the error below.
        trajectory = self.to_trajectory()

        # Prefer a track over a materialized ephemeris where both are accepted.
        if trajectory is not None and TargetTrack in supported:
            frame = _accepted_frame(TargetTrack, supported, observer)

            logger.debug(f"adapting {mytype.__name__} to TargetTrack in {frame}")
            return TargetTrack(trajectory, frame, observer=observer, weather=weather)

        if trajectory is not None and EphemerisTarget in supported:
            frame = _accepted_frame(EphemerisTarget, supported, observer)

            logger.debug(f"adapting {mytype.__name__} to EphemerisTarget in {frame}")
            return await self.to_ephemeris_target(
                start_time=datetime.now(UTC),
                duration=_ADAPTED_EPHEMERIS_DURATION,
                step=_ADAPTED_EPHEMERIS_STEP,
                frame=frame,
                observer=observer,
                weather=weather,
            )

        raise RuntimeError("Could not adapt target to a supported type")

    def to_trajectory(self) -> Trajectory | None:
        """Return a Trajectory for this target, or None if it cannot be propagated."""
        return None

    def to_track(
        self,
        frame: ReferenceFrame = ReferenceFrame.GCRF,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
    ) -> TargetTrack:
        """Return a TargetTrack sampling this target in the given frame.

        Raises:
            TypeError: If this target cannot be propagated.
        """
        trajectory = self.to_trajectory()

        if trajectory is None:
            raise TypeError(f"{type(self).__name__} cannot be propagated")

        return TargetTrack(trajectory, frame, observer=observer, weather=weather)

    async def to_ephemeris_target(
        self,
        start_time: datetime,
        duration: timedelta,
        step: timedelta,
        frame: ReferenceFrame = ReferenceFrame.GCRF,
        observer: EarthObserver | Geodetic | None = None,
        weather: BasicWeather | None = None,
    ) -> EphemerisTarget:
        """Propagate this target into a pre-computed EphemerisTarget over the given time window."""
        track = self.to_track(frame, observer, weather)
        jds, points = await track.sample(start_time, duration, step)

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
            obswl=wavelength,
            **(weather.to_astropy() if weather else {}),
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

        case EphemerisTarget(frame=ReferenceFrame.ICRF):
            if not target.jds:
                raise ValueError("EphemerisTarget has no samples")
            jds = np.asarray(target.jds, dtype=float)
            for dt in times:
                t_ast = Time(dt, scale="utc")
                i = int(np.argmin(np.abs(jds - t_ast.jd)))
                pt = target.points[i]
                sc = SkyCoord(ra=pt.ra * u.deg, dec=pt.dec * u.deg, frame=ICRS())
                a = sc.transform_to(make_altaz_frame(loc, t_ast))
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


