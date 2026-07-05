# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from pydantic import TypeAdapter

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
async def test_adapt_requires_observer_for_propagation():
    """adapt() raises AssertionError when propagation is needed but observer is absent."""
    target = StateVectorTarget(frame=ReferenceFrame.GCRF, sv=_GEO_SV)
    with pytest.raises(AssertionError):
        await target.adapt((EphemerisTarget, ReferenceFrame.CIRF))


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
