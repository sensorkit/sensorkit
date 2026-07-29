# SPDX-License-Identifier: Apache-2.0
"""Tests for star name resolution and fixed-target visibility."""

import math
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import astropy.units as u
import pytest
from astropy.coordinates import SkyCoord

from sensorkit.astro.coords import Equatorial
from sensorkit.otto import stars

# Vega, as CDS Sesame returns it
VEGA = Equatorial(ra=279.23473, dec=38.78369)

# Sesame is a network service, so the lookup is stubbed; everything downstream of it is real
VEGA_SKYCOORD = SkyCoord(ra=VEGA.ra * u.deg, dec=VEGA.dec * u.deg)


class TestResolve:
    @pytest.mark.asyncio
    async def test_resolves_a_name(self):
        with patch("sensorkit.otto.stars.SkyCoord.from_name", return_value=VEGA_SKYCOORD):
            coords = await stars.resolve("Vega")

        assert coords.ra == pytest.approx(VEGA.ra)
        assert coords.dec == pytest.approx(VEGA.dec)

    @pytest.mark.asyncio
    async def test_strips_surrounding_whitespace(self):
        looked_up = []

        def from_name(name):
            looked_up.append(name)
            return VEGA_SKYCOORD

        with patch("sensorkit.otto.stars.SkyCoord.from_name", from_name):
            await stars.resolve("  HR 7001  ")

        assert looked_up == ["HR 7001"]

    @pytest.mark.asyncio
    async def test_unknown_name_is_unresolved(self):
        with patch(
            "sensorkit.otto.stars.SkyCoord.from_name",
            side_effect=Exception("Unable to find coordinates for name 'Vegga'"),
        ):
            assert await stars.resolve("Vegga") is None


class TestPosition:
    """A fixed equatorial target, evaluated in closed form from the hour angle."""

    def _at_hour_angle(self, hours, dec, latitude):
        """Find a time when a star of the given dec sits at `hours` hour angle."""
        now = datetime.now(UTC)
        # Solve for the RA that puts the star at this hour angle right now
        _, _, _, ha = stars.position(
            Equatorial(ra=0.0, dec=dec), latitude=latitude, longitude=0.0, now=now
        )
        ra = ((ha - hours) * 15.0) % 360.0
        return Equatorial(ra=ra, dec=dec), now

    def test_transits_due_south_below_the_zenith(self):
        """At HA=0 with dec < latitude, the star is on the meridian to the south."""
        coords, now = self._at_hour_angle(0.0, dec=0.0, latitude=40.0)
        altitude, azimuth, _, ha = stars.position(coords, latitude=40.0, longitude=0.0, now=now)

        assert ha == pytest.approx(0.0, abs=1e-6)
        assert azimuth == pytest.approx(180.0, abs=1e-6)
        # Altitude at southern transit = 90 - latitude + dec
        assert altitude == pytest.approx(50.0, abs=1e-6)

    def test_transits_due_north_above_the_zenith(self):
        """With dec > latitude the meridian crossing is on the north side."""
        coords, now = self._at_hour_angle(0.0, dec=70.0, latitude=40.0)
        altitude, azimuth, _, _ = stars.position(coords, latitude=40.0, longitude=0.0, now=now)

        assert azimuth == pytest.approx(0.0, abs=1e-6)
        assert altitude == pytest.approx(60.0, abs=1e-6)

    def test_east_of_meridian_is_rising(self):
        coords, now = self._at_hour_angle(-3.0, dec=0.0, latitude=40.0)
        altitude, azimuth, rising, ha = stars.position(
            coords, latitude=40.0, longitude=0.0, now=now
        )

        assert ha == pytest.approx(-3.0, abs=1e-6)
        assert rising is True
        assert 0.0 < azimuth < 180.0  # eastern half of the sky

    def test_west_of_meridian_is_setting(self):
        coords, now = self._at_hour_angle(3.0, dec=0.0, latitude=40.0)
        _, azimuth, rising, ha = stars.position(coords, latitude=40.0, longitude=0.0, now=now)

        assert ha == pytest.approx(3.0, abs=1e-6)
        assert rising is False
        assert 180.0 < azimuth < 360.0  # western half of the sky

    def test_circumpolar_star_stays_up(self):
        """dec > 90 - latitude never sets, so altitude stays above the pole gap."""
        latitude = 55.0
        coords = Equatorial(ra=0.0, dec=80.0)
        now = datetime.now(UTC)
        altitudes = [
            stars.position(coords, latitude=latitude, longitude=0.0, now=now + timedelta(hours=h))[
                0
            ]
            for h in range(24)
        ]
        assert min(altitudes) > latitude - (90.0 - 80.0)

    def test_altitude_never_leaves_the_valid_range(self):
        """The asin argument is clamped, so poles and extreme latitudes stay finite."""
        now = datetime.now(UTC)
        for dec in (-90.0, -45.0, 0.0, 45.0, 90.0):
            for latitude in (-90.0, -33.0, 0.0, 42.0, 90.0):
                altitude, azimuth, _, ha = stars.position(
                    Equatorial(ra=123.0, dec=dec), latitude=latitude, longitude=0.0, now=now
                )
                assert -90.0 <= altitude <= 90.0
                assert 0.0 <= azimuth < 360.0
                assert -12.0 <= ha < 12.0
                assert not math.isnan(altitude)

    def test_longitude_shifts_the_hour_angle(self):
        now = datetime.now(UTC)
        _, _, _, ha = stars.position(VEGA, latitude=40.0, longitude=0.0, now=now)
        _, _, _, ha_east = stars.position(VEGA, latitude=40.0, longitude=90.0, now=now)

        assert ((ha_east - ha + 12.0) % 24.0) - 12.0 == pytest.approx(6.0, abs=1e-6)


class TestAgreementWithAstropy:
    """Pins how coarse the closed-form alt/az is allowed to be.

    Fixed historical dates keep this deterministic — the disagreement is
    dominated by precession since J2000, so it would otherwise creep upward
    with the wall clock. IERS auto-download is disabled so the test stays
    offline; astropy falls back to its bundled IERS-B table.
    """

    # Comfortably above the ~0.28 deg observed, far below anything that would
    # matter to a 20 deg altitude floor.
    TOLERANCE_DEG = 0.5

    @pytest.mark.parametrize("when", ["2024-01-15 03:00:00", "2024-07-15 21:00:00"])
    @pytest.mark.parametrize(
        "name,ra,dec",
        [
            ("Vega", 279.23473, 38.78369),
            ("Sirius", 101.28716, -16.71612),
            ("Polaris", 37.95456, 89.26411),
            ("M31", 10.68471, 41.26875),
        ],
    )
    def test_altitude_tracks_a_rigorous_transform(self, when, name, ra, dec):
        import astropy.units as u
        from astropy.coordinates import AltAz, EarthLocation, SkyCoord
        from astropy.time import Time
        from astropy.utils import iers

        latitude, longitude = 42.3, -83.0
        with iers.conf.set_temp("auto_download", False):
            time = Time(when, scale="utc")
            altitude, azimuth, _, _ = stars.position(
                Equatorial(ra=ra, dec=dec),
                latitude=latitude,
                longitude=longitude,
                now=time.to_datetime(timezone=UTC),
            )
            reference = SkyCoord(ra=ra * u.deg, dec=dec * u.deg).transform_to(
                AltAz(
                    obstime=time,
                    location=EarthLocation(
                        lat=latitude * u.deg, lon=longitude * u.deg, height=200 * u.m
                    ),
                )
            )

        assert altitude == pytest.approx(reference.alt.deg, abs=self.TOLERANCE_DEG)

    def test_hour_angle_matches_apparent_sidereal_time(self):
        """The hour angle, unlike alt/az, is exact — scan ordering depends on it."""
        import astropy.units as u
        from astropy.time import Time
        from astropy.utils import iers

        longitude = -83.0
        with iers.conf.set_temp("auto_download", False):
            time = Time("2024-07-15 21:00:00", scale="utc")
            _, _, _, hour_angle = stars.position(
                VEGA, latitude=42.3, longitude=longitude, now=time.to_datetime(timezone=UTC)
            )
            lst = time.sidereal_time("apparent", longitude=longitude * u.deg).hour

        expected = ((lst - VEGA.ra / 15.0 + 12.0) % 24.0) - 12.0
        # Within a second of time
        assert hour_angle == pytest.approx(expected, abs=1 / 3600)
