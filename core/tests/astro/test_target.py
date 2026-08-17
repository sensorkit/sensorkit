# SPDX-License-Identifier: Apache-2.0
import math
from datetime import UTC, datetime, timedelta

import astropy.units as u
import numpy as np
import pytest
import satkit
from astropy.coordinates import ICRS, AltAz, SkyCoord, angular_separation
from astropy.time import Time
from pydantic import TypeAdapter
from skyfield import api as skyfield

from sensorkit.astro.common import (
    TLE,
    ReferenceFrame,
)
from sensorkit.astro.coords import Cartesian, Equatorial, Geodetic, Horizontal, StateVector
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    ICRSTarget,
    ObserveWindow,
    StateVectorTarget,
    Target,
    TargetTrack,
    TLETarget,
    altitude_mask,
    is_observable,
    norm_az_deg,
    sample_altaz_series,
    time_grid,
)
from sensorkit.astro.trajectory import TLETrajectory

_ISS_TLE = TLE(
    line0="0 ISS (ZARYA)",
    line1="1 25544U 98067A   25015.39697524  .00024300  00000-0  43163-3 0  9997",
    line2="2 25544  51.6408 343.8792 0001934 100.3261   3.0329 15.50054085491466",
)

_NYC = Geodetic(lon=-74.0060, lat=40.7128, elev=10.0)

# Tolerance for the skyfield cross-checks below. The satkit/skyfield SGP4 and TEME-to-GCRF
# floor is a ~20 m position difference, so its angular size runs inversely with slant
# range: ~0.5 arcsec at the 6473 km of the epoch both tests pin, but ~9 arcsec on a close
# overhead pass. Retuning either test to a new epoch means re-deriving this number.
_SKYFIELD_TOL_ARCSEC = 1.5


def test_icrs_target():
    t = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))
    assert t.frame == ReferenceFrame.ICRF
    sc = t.to_astropy()
    assert sc.ra.deg == pytest.approx(180.0)
    assert sc.dec.deg == pytest.approx(45.0)


def test_altaz_target():
    t = AltAzTarget(coords=Horizontal(az=90.0, alt=30.0))
    assert t.frame == ReferenceFrame.ALTAZ
    assert t.coords.az == 90.0
    assert t.coords.alt == 30.0


def test_tle_target():
    t = TLETarget(tle=_ISS_TLE)
    assert t.frame == ReferenceFrame.TEME
    traj = t.to_trajectory()
    assert isinstance(traj, TLETrajectory)


def test_ephemeris_target():
    jds = [2460000.5, 2460001.5]
    pts = [Equatorial(ra=10.0, dec=20.0), Equatorial(ra=11.0, dec=21.0)]
    t = EphemerisTarget(frame=ReferenceFrame.ICRF, jds=jds, points=pts)
    assert len(t.jds) == 2
    assert len(t.points) == 2


def test_state_vector_target():
    sv = StateVector(
        t=datetime(2024, 6, 15, tzinfo=UTC),
        r=Cartesian(6778000.0, 0.0, 0.0),
        v=Cartesian(0.0, 7660.0, 0.0),
    )
    t = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=sv)
    sv_gcrf = t.sv_gcrf()
    assert isinstance(sv_gcrf, StateVector)
    # Frame is already GCRF, so sv_gcrf should be the same.
    assert sv_gcrf.r.x == pytest.approx(sv.r.x)


def test_target_discriminator():
    adapter = TypeAdapter(Target)

    icrs = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))
    data = adapter.dump_python(icrs)
    restored = adapter.validate_python(data)
    assert isinstance(restored, ICRSTarget)
    assert restored.coords.ra == 180.0

    tle = TLETarget(tle=_ISS_TLE)
    data = adapter.dump_python(tle)
    restored = adapter.validate_python(data)
    assert isinstance(restored, TLETarget)
    assert restored.tle.line1 == _ISS_TLE.line1


def test_time_grid():
    start = datetime(2024, 6, 21, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(minutes=10)
    win = ObserveWindow(start_time=start, end_time=end, step_seconds=60)
    grid = time_grid(win)
    # 0, 60, 120, ..., 600 → 11 points
    assert len(grid) == 11
    assert grid[0] == start
    assert grid[-1] == end


def test_time_grid_naive_raises():
    with pytest.raises(ValueError, match="timezone-aware"):
        time_grid(ObserveWindow(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
        ))


def test_time_grid_inverted_raises():
    with pytest.raises(ValueError, match="end_time must be"):
        time_grid(ObserveWindow(
            start_time=datetime(2024, 1, 2, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, tzinfo=UTC),
        ))


def test_time_grid_zero_step_raises():
    with pytest.raises(ValueError, match="step_seconds must be"):
        time_grid(ObserveWindow(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            step_seconds=0,
        ))


def test_norm_az_deg():
    assert norm_az_deg(0.0) == 0.0
    assert norm_az_deg(360.0) == pytest.approx(0.0)
    assert norm_az_deg(-90.0) == pytest.approx(270.0)
    assert norm_az_deg(450.0) == pytest.approx(90.0)


def test_altitude_mask():
    altaz = [(10.0, 0.0), (30.0, 90.0), (60.0, 180.0), (20.0, 270.0)]
    mask = altitude_mask(altaz, min_altitude_deg=25.0)
    np.testing.assert_array_equal(mask, [False, True, True, False])


def test_sample_altaz_series_altaz():
    """AltAzTarget returns constant (alt, az) for all times."""
    target = AltAzTarget(coords=Horizontal(az=180.0, alt=60.0))
    loc = _NYC.to_astropy()
    times = [datetime(2024, 6, 21, h, tzinfo=UTC) for h in range(3)]
    series = sample_altaz_series(target, loc, times)
    assert len(series) == 3
    for alt, az in series:
        assert alt == pytest.approx(60.0)
        assert az == pytest.approx(180.0)


def test_sample_altaz_series_icrs():
    """ICRSTarget returns varying altaz across time grid."""
    target = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))
    loc = _NYC.to_astropy()
    times = [datetime(2024, 6, 21, h, tzinfo=UTC) for h in range(0, 24, 6)]
    series = sample_altaz_series(target, loc, times)
    assert len(series) == 4
    # Alt/az should vary across 6-hour intervals due to Earth's rotation.
    alts = [alt for alt, _ in series]
    assert len(set(round(a, 2) for a in alts)) > 1


def test_sample_altaz_series_ephemeris():
    """EphemerisTarget maps precomputed ICRF RA/Dec samples to alt/az per time."""

    def _jd(dt):
        return dt.timestamp() / 86400.0 + 2440587.5

    t0 = datetime(2024, 6, 21, 0, 0, 0, tzinfo=UTC)
    times = [t0, t0 + timedelta(minutes=1)]
    target = EphemerisTarget(
        frame=ReferenceFrame.ICRF,
        jds=[_jd(t) for t in times],
        points=[Equatorial(ra=180.0, dec=45.0), Equatorial(ra=180.2, dec=45.0)],
    )
    loc = _NYC.to_astropy()
    series = sample_altaz_series(target, loc, times)
    assert len(series) == 2
    for alt, az in series:
        assert -90.0 <= alt <= 90.0
        assert 0.0 <= az < 360.0


def test_is_observable_above_horizon():
    target = AltAzTarget(coords=Horizontal(az=180.0, alt=60.0))
    start = datetime(2024, 6, 21, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)
    assert is_observable(target, _NYC, start, end, min_altitude_deg=30.0)


def test_is_observable_below_horizon():
    target = AltAzTarget(coords=Horizontal(az=180.0, alt=10.0))
    start = datetime(2024, 6, 21, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)
    assert not is_observable(target, _NYC, start, end, min_altitude_deg=30.0)


_OBSERVER = Geodetic(lon=-115.0, lat=35.0, elev=0.0)

# GOES 18 GEO state vector (meters, GCRF, 2025-11-13 00:00 UTC)
_GEO_SV = StateVector(
    t=datetime(2025, 11, 13, tzinfo=UTC),
    r=Cartesian(3_489_446.6, -42_016_287.6, -6_988.5),
    v=Cartesian(3_064.4, 254.5, -9.2),
)


@pytest.mark.asyncio
async def test_adapt_altaz_returns_self():
    """AltAzTarget is returned unchanged when it appears in the accepts list."""
    target = AltAzTarget(coords=Horizontal(az=90.0, alt=45.0))
    result = await target.adapt(AltAzTarget, ICRSTarget)
    assert result is target


@pytest.mark.asyncio
async def test_adapt_icrs_returns_self():
    """ICRSTarget is returned unchanged when it appears in the accepts list."""
    target = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))
    result = await target.adapt(AltAzTarget, ICRSTarget)
    assert result is target


@pytest.mark.asyncio
async def test_adapt_with_matching_frame_constraint_returns_self():
    """adapt() returns self when the (type, frame) tuple matches the target."""
    target = AltAzTarget(coords=Horizontal(az=90.0, alt=45.0))
    result = await target.adapt((AltAzTarget, ReferenceFrame.ALTAZ), ICRSTarget)
    assert result is target


@pytest.mark.asyncio
async def test_adapt_raises_when_unsupported():
    """adapt() raises RuntimeError when no accepted type can represent the target."""
    target = AltAzTarget(coords=Horizontal(az=90.0, alt=45.0))
    with pytest.raises(RuntimeError, match="Could not adapt"):
        await target.adapt(ICRSTarget, observer=_OBSERVER)


@pytest.mark.asyncio
async def test_adapt_requires_observer_for_observer_relative_frame():
    """adapt() rejects an observer-relative frame when no observer was given."""
    target = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=_GEO_SV)
    with pytest.raises(RuntimeError, match="observer location is required"):
        await target.adapt((EphemerisTarget, ReferenceFrame.CIRF))


@pytest.mark.asyncio
async def test_adapt_allows_geocentric_frame_without_observer():
    """A geocentric frame needs no observer, so adapt() propagates without one."""
    target = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=_GEO_SV)
    eph = await target.adapt((EphemerisTarget, ReferenceFrame.GCRF))
    assert isinstance(eph, EphemerisTarget)
    assert eph.points


@pytest.mark.asyncio
async def test_tle_to_icrf_ephemeris_matches_skyfield():
    """An ICRF ephemeris carries the astrometric topocentric place, free of aberration.

    Skyfield is the reference: its topocentric `radec()` is the geometric direction from
    the site to the satellite, which is what an equatorial mount must be commanded to.
    Routing the propagated GCRS position through AltAz and back would instead remove an
    annual aberration the satellite never had, biasing RA/Dec by ~13 arcsec at this epoch
    and up to ~20 arcsec in general.
    """
    start = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)

    eph = await TLETarget(tle=_ISS_TLE).to_ephemeris_target(
        start_time=start,
        duration=timedelta(seconds=2),
        step=timedelta(seconds=1),
        frame=ReferenceFrame.ICRF,
        observer=_NYC,
    )

    ts = skyfield.load.timescale()
    topo = (
        skyfield.EarthSatellite(_ISS_TLE.line1, _ISS_TLE.line2, ts=ts)
        - skyfield.wgs84.latlon(_NYC.lat, _NYC.lon, _NYC.elev)
    ).at(ts.from_datetime(start + timedelta(seconds=1)))
    ra, dec, _ = topo.radec()

    expected = SkyCoord(ra=ra.degrees * u.deg, dec=dec.degrees * u.deg, frame=ICRS())
    actual = SkyCoord(ra=eph.points[0].ra * u.deg, dec=eph.points[0].dec * u.deg, frame=ICRS())

    assert expected.separation(actual).arcsec < _SKYFIELD_TOL_ARCSEC


@pytest.mark.asyncio
async def test_tle_to_altaz_ephemeris_matches_skyfield():
    """An ALTAZ ephemeris carries the topocentric horizontal place, as Horizontal points.

    Deriving alt/az from the topocentric CIRS place keeps astropy from re-deriving the
    parallax through ICRS, which biases the result by ~6 arcsec. No weather is supplied
    and skyfield refracts only when given a temperature, so both sides are geometric.
    """
    start = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)

    eph = await TLETarget(tle=_ISS_TLE).to_ephemeris_target(
        start_time=start,
        duration=timedelta(seconds=2),
        step=timedelta(seconds=1),
        frame=ReferenceFrame.ALTAZ,
        observer=_NYC,
    )

    assert all(isinstance(p, Horizontal) for p in eph.points)

    when = start + timedelta(seconds=1)
    topo = (
        skyfield.EarthSatellite(_ISS_TLE.line1, _ISS_TLE.line2, ts=(ts := skyfield.load.timescale()))
        - skyfield.wgs84.latlon(_NYC.lat, _NYC.lon, _NYC.elev)
    ).at(ts.from_datetime(when))
    alt, az, _ = topo.altaz()

    frame = AltAz(obstime=Time(when), location=_NYC.to_astropy())
    expected = SkyCoord(az=az.degrees * u.deg, alt=alt.degrees * u.deg, frame=frame)
    actual = SkyCoord(az=eph.points[0].az * u.deg, alt=eph.points[0].alt * u.deg, frame=frame)

    assert expected.separation(actual).arcsec < _SKYFIELD_TOL_ARCSEC


@pytest.mark.asyncio
async def test_tle_to_itrf_ephemeris_matches_skyfield_subpoint():
    """ITRF is Earth-fixed, so its points are the geodetic sub-satellite position.

    RA/Dec is meaningless in an Earth-fixed frame; the ITRS place resolves to the point
    on the WGS84 ellipsoid beneath the target plus its height, which is what skyfield's
    `subpoint()` returns.
    """
    start = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)

    eph = await TLETarget(tle=_ISS_TLE).to_ephemeris_target(
        start_time=start,
        duration=timedelta(seconds=2),
        step=timedelta(seconds=1),
        frame=ReferenceFrame.ITRF,
        observer=_NYC,
    )

    assert all(isinstance(p, Geodetic) for p in eph.points)

    ts = skyfield.load.timescale()
    sat = skyfield.EarthSatellite(_ISS_TLE.line1, _ISS_TLE.line2, ts=ts)
    sub = skyfield.wgs84.subpoint(sat.at(ts.from_datetime(start + timedelta(seconds=1))))

    point = eph.points[0]
    assert abs(point.lat - sub.latitude.degrees) * 3600 < _SKYFIELD_TOL_ARCSEC
    # A ~20 m along-track difference subtends more longitude than latitude this far north.
    assert abs(point.lon - sub.longitude.degrees) * 3600 < _SKYFIELD_TOL_ARCSEC / math.cos(
        math.radians(sub.latitude.degrees)
    )
    assert abs(point.elev - sub.elevation.m) < 50.0


@pytest.mark.asyncio
async def test_tle_to_teme_ephemeris_round_trips_satkit():
    """TEME points inverse the TEME-to-GCRF rotation the trajectory applied.

    satkit propagates SGP4 in TEME and rotates into GCRF; asking for a TEME ephemeris
    sends that back through astropy's own rotation, so the result should reproduce the
    raw SGP4 direction to within the two libraries' precession/nutation differences.
    """
    start = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)
    when = start + timedelta(seconds=1)

    eph = await TLETarget(tle=_ISS_TLE).to_ephemeris_target(
        start_time=start,
        duration=timedelta(seconds=2),
        step=timedelta(seconds=1),
        frame=ReferenceFrame.TEME,
        observer=_NYC,
    )

    assert all(isinstance(p, Equatorial) for p in eph.points)

    teme_p, _ = satkit.sgp4(
        satkit.TLE.from_lines(_ISS_TLE.to_list()), satkit.time.from_datetime(when)
    )
    x, y, z = np.asarray(teme_p).ravel()

    point = eph.points[0]
    sep = angular_separation(
        math.atan2(y, x) * u.rad,
        math.asin(z / math.hypot(math.hypot(x, y), z)) * u.rad,
        point.ra * u.deg,
        point.dec * u.deg,
    )

    assert sep.to_value(u.arcsec) < _SKYFIELD_TOL_ARCSEC


@pytest.mark.asyncio
async def test_adapt_state_vector_to_ephemeris():
    """StateVectorTarget.adapt() propagates into an EphemerisTarget in the requested frame."""
    target = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=_GEO_SV)
    result = await target.adapt(
        AltAzTarget,
        ICRSTarget,
        (EphemerisTarget, ReferenceFrame.CIRF),
        observer=_OBSERVER,
    )
    assert isinstance(result, EphemerisTarget)
    assert result.frame == ReferenceFrame.CIRF
    assert len(result.jds) > 0
    assert len(result.points) == len(result.jds)


@pytest.mark.asyncio
async def test_adapt_to_track_binds_observing_context():
    """adapt() hands back a track already bound to the requested frame and observer."""
    target = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=_GEO_SV)
    track = await target.adapt(
        ICRSTarget,
        (TargetTrack, ReferenceFrame.CIRF),
        observer=_OBSERVER,
    )
    assert isinstance(track, TargetTrack)
    assert track.frame == ReferenceFrame.CIRF
    assert track.observer is _OBSERVER


@pytest.mark.asyncio
async def test_adapt_prefers_track_over_ephemeris():
    """A consumer accepting both gets the track, which imposes no bound on the dwell."""
    target = TLETarget(tle=_ISS_TLE)
    result = await target.adapt(
        (EphemerisTarget, ReferenceFrame.ICRF),
        (TargetTrack, ReferenceFrame.ICRF),
        observer=_OBSERVER,
    )
    assert isinstance(result, TargetTrack)


@pytest.mark.asyncio
async def test_adapt_fixed_target_to_track_is_unsupported():
    """A target with no trajectory reports that it cannot be adapted, not that it is unimplemented."""
    target = AltAzTarget(coords=Horizontal(az=90.0, alt=45.0))
    with pytest.raises(RuntimeError, match="Could not adapt"):
        await target.adapt((TargetTrack, ReferenceFrame.ICRF), observer=_OBSERVER)


@pytest.mark.asyncio
async def test_track_sample_matches_ephemeris_target():
    """A track samples the same points the equivalent EphemerisTarget is built from."""
    start = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)
    window = dict(duration=timedelta(seconds=30), step=timedelta(seconds=10))
    target = TLETarget(tle=_ISS_TLE)

    track = target.to_track(ReferenceFrame.ICRF, _OBSERVER)
    jds, points = await track.sample(start, **window)
    eph = await target.to_ephemeris_target(
        start_time=start, frame=ReferenceFrame.ICRF, observer=_OBSERVER, **window
    )

    assert jds == list(eph.jds)
    assert [(p.ra, p.dec) for p in points] == [(p.ra, p.dec) for p in eph.points]


@pytest.mark.asyncio
async def test_track_stream_yields_successive_windows_covering_the_present():
    """Each window starts at or before the moment it is produced, and advances in time."""
    track = TLETarget(tle=_ISS_TLE).to_track(ReferenceFrame.ICRF, _OBSERVER)
    windows = []

    async for jds, points in track.stream(
        window=timedelta(seconds=2), step=timedelta(seconds=1), lead=timedelta(seconds=1.9)
    ):
        assert len(points) == len(jds)
        assert jds[0] <= Time.now().jd
        windows.append(jds)

        if len(windows) == 2:
            break

    assert windows[1][0] > windows[0][0]


@pytest.mark.asyncio
async def test_track_stream_rejects_lead_longer_than_window():
    """A lead that leaves no time to consume a window is rejected."""
    track = TLETarget(tle=_ISS_TLE).to_track(ReferenceFrame.ICRF, _OBSERVER)
    stream = track.stream(
        window=timedelta(seconds=10), step=timedelta(seconds=1), lead=timedelta(seconds=10)
    )

    with pytest.raises(ValueError, match="lead must be shorter"):
        await anext(stream)
