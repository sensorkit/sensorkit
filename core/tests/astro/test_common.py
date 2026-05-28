from datetime import UTC, datetime

import numpy as np
import pytest
from astropy.coordinates import CIRS, GCRS, ICRS, ITRS, EarthLocation, SkyCoord

from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.coords import Cartesian, Equatorial, Geodetic, Horizontal, StateVector


def test_reference_frame_to_astropy():
    assert ReferenceFrame.ICRF.to_astropy() is ICRS
    assert ReferenceFrame.GCRF.to_astropy() is GCRS
    assert ReferenceFrame.CIRF.to_astropy() is CIRS
    assert ReferenceFrame.ITRF.to_astropy() is ITRS


def test_horizontal():
    h = Horizontal(az=180.0, alt=45.0)
    assert h.az == 180.0
    assert h.alt == 45.0

    sc = h.to_astropy()
    assert isinstance(sc, SkyCoord)
    assert sc.az.deg == pytest.approx(180.0)
    assert sc.alt.deg == pytest.approx(45.0)


def test_equatorial():
    eq = Equatorial(ra=180.0, dec=45.0)
    sc = eq.to_astropy()
    assert isinstance(sc, SkyCoord)
    assert sc.ra.deg == pytest.approx(180.0)
    assert sc.dec.deg == pytest.approx(45.0)
    assert isinstance(sc.frame, ICRS)


def test_equatorial_custom_frame():
    eq = Equatorial(ra=10.0, dec=20.0)
    sc = eq.to_astropy(frame=ReferenceFrame.GCRF)
    assert isinstance(sc.frame, GCRS)


def test_geodetic():
    g = Geodetic(lon=-74.0, lat=40.7, elev=10.0)
    loc = g.to_astropy()
    assert isinstance(loc, EarthLocation)
    assert loc.lon.deg == pytest.approx(-74.0)
    assert loc.lat.deg == pytest.approx(40.7)


def test_geodetic_cached():
    g = Geodetic(lon=-74.0, lat=40.7, elev=10.0)
    loc1 = g.to_astropy()
    loc2 = g.to_astropy()
    assert loc1 is loc2


def test_cartesian_scalar_mul():
    c = Cartesian(1.0, 2.0, 3.0)
    result = c * 2.0
    assert result == Cartesian(2.0, 4.0, 6.0)

    result_int = c * 3
    assert result_int == Cartesian(3.0, 6.0, 9.0)


def test_cartesian_elementwise_mul():
    a = Cartesian(1.0, 2.0, 3.0)
    b = Cartesian(4.0, 5.0, 6.0)
    result = a * b
    assert result == Cartesian(4.0, 10.0, 18.0)


def test_cartesian_invalid_mul():
    c = Cartesian(1.0, 2.0, 3.0)
    with pytest.raises(RuntimeError, match="cannot multiply"):
        c * "bad"


def test_state_vector_to_numpy():
    sv = StateVector(
        t=datetime(2024, 1, 1, tzinfo=UTC),
        r=Cartesian(1.0, 2.0, 3.0),
        v=Cartesian(4.0, 5.0, 6.0),
    )
    arr = sv.to_numpy()
    assert arr.shape == (6,)
    np.testing.assert_array_equal(arr, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])


def test_state_vector_astropy_roundtrip():
    sv = StateVector(
        t=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        r=Cartesian(6778000.0, 0.0, 0.0),  # ~LEO altitude in meters
        v=Cartesian(0.0, 7660.0, 0.0),  # ~LEO velocity in m/s
    )
    coord = sv.to_astropy()
    sv2 = StateVector.from_astropy(coord)

    assert sv2.r.x == pytest.approx(sv.r.x, rel=1e-6)
    assert sv2.r.y == pytest.approx(sv.r.y, abs=1e-3)
    assert sv2.r.z == pytest.approx(sv.r.z, abs=1e-3)
    assert sv2.v.x == pytest.approx(sv.v.x, abs=1e-3)
    assert sv2.v.y == pytest.approx(sv.v.y, rel=1e-6)
    assert sv2.v.z == pytest.approx(sv.v.z, abs=1e-3)


_ISS_TLE = TLE(
    line0="0 ISS (ZARYA)",
    line1="1 25544U 98067A   25015.39697524  .00024300  00000-0  43163-3 0  9997",
    line2="2 25544  51.6408 343.8792 0001934 100.3261   3.0329 15.50054085491466",
)


def test_tle_to_list_with_line0():
    lst = _ISS_TLE.to_list()
    assert len(lst) == 3
    assert lst[0] == "0 ISS (ZARYA)"
    assert lst[1].startswith("1 ")
    assert lst[2].startswith("2 ")


def test_tle_to_list_without_line0():
    tle = TLE(line0=None, line1=_ISS_TLE.line1, line2=_ISS_TLE.line2)
    lst = tle.to_list()
    assert len(lst) == 2
    assert lst[0].startswith("1 ")
