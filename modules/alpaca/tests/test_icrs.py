# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope ICRS FollowTarget."""

from datetime import UTC, datetime

import astropy.units as u
import pytest
from alpaca.telescope import EquatorialCoordinateType
from astropy.coordinates import angular_separation
from skyfield import api as skyfield

from sensorkit.astro.common import RADecPointing, ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget, RateTarget
from sensorkit.std import AxisRates, FollowTarget


@pytest.mark.asyncio
async def test_telescope_follow_icrs(telescope):
    from sensorkit.astro.coords import Equatorial
    from sensorkit.astro.target import ICRSTarget

    # The fake reports slew onset on the next read, then a settled mount.
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0)))
    )

    # Tracking should be enabled for ICRS targets
    assert telescope._tracking is True


_STAR = ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0))


@pytest.mark.asyncio
async def test_topocentric_mount_slews_to_apparent_place(telescope):
    """Topocentric mounts receive apparent RA/Dec for the current date and site."""
    telescope._equatorial_system = EquatorialCoordinateType.equTopocentric

    await telescope.telescope_follow_target(FollowTarget(target=_STAR))

    (((ra_hours, dec_deg), _),) = telescope.telescope.calls("SlewToCoordinatesAsync")

    ts = skyfield.load.timescale()
    site = skyfield.load("de421.bsp")["earth"] + skyfield.wgs84.latlon(-31.0, 149.0, 1100.0)
    star = skyfield.Star(ra_hours=_STAR.coords.ra / 15.0, dec_degrees=_STAR.coords.dec)
    ra, dec, _ = site.at(ts.now()).observe(star).apparent().radec(epoch="date")

    sep = angular_separation(
        ra_hours * u.hourangle, dec_deg * u.deg, ra.hours * u.hourangle, dec.degrees * u.deg
    )
    assert sep.to_value(u.arcsec) < 1.0


@pytest.mark.parametrize(
    ("system", "converted"),
    [
        (EquatorialCoordinateType.equOther, False),
        (EquatorialCoordinateType.equTopocentric, True),
        (EquatorialCoordinateType.equJ2000, False),
        (EquatorialCoordinateType.equJ2050, True),
        (EquatorialCoordinateType.equB1950, True),
    ],
)
@pytest.mark.asyncio
async def test_mount_system_round_trips_to_icrs(telescope, recorder, system, converted):
    """Slew coordinates use the mount's frame and published pointing uses ICRS."""
    telescope._equatorial_system = system

    await telescope.telescope_follow_target(FollowTarget(target=_STAR))
    await telescope.fast_loop.stop()

    (((ra_hours, dec_deg), _),) = telescope.telescope.calls("SlewToCoordinatesAsync")
    slewed = angular_separation(
        ra_hours * u.hourangle, dec_deg * u.deg, _STAR.coords.ra * u.deg, _STAR.coords.dec * u.deg
    )
    assert (slewed.to_value(u.arcmin) > 1.0) == converted

    telescope.telescope._properties.update(RightAscension=ra_hours, Declination=dec_deg)
    published = await recorder()
    await telescope._publish_telescope_status()
    pointing = await published.wait_for(RADecPointing)

    reported = angular_separation(
        pointing.right_ascension_hours * u.hourangle,
        pointing.declination_degrees * u.deg,
        _STAR.coords.ra * u.deg,
        _STAR.coords.dec * u.deg,
    )
    assert reported.to_value(u.arcsec) < 0.01


# ICRS motion of the rate target, in deg/s: RA only, about 36 arcsec/s.
_ICRS_RA_RATE = 0.01


def apparent_motion():
    """Return apparent RA/Dec in degrees and angular rates in degrees per SI second."""
    ts = skyfield.load.timescale()
    site = skyfield.load("de421.bsp")["earth"] + skyfield.wgs84.latlon(-31.0, 149.0, 1100.0)
    t0 = ts.now()
    t1 = ts.tt_jd(t0.tt + 1.0 / 86400.0)

    places = []
    for t, ra in ((t0, _STAR.coords.ra), (t1, _STAR.coords.ra + _ICRS_RA_RATE)):
        star = skyfield.Star(ra_hours=ra / 15.0, dec_degrees=_STAR.coords.dec)
        ra_app, dec_app, _ = site.at(t).observe(star).apparent().radec(epoch="date")
        places.append((ra_app.hours * 15.0, dec_app.degrees))

    (ra0, dec0), (ra1, dec1) = places
    return ra0, dec0, ra1 - ra0, dec1 - dec0


@pytest.mark.asyncio
async def test_topocentric_mount_tracks_apparent_rates(telescope):
    """Topocentric mounts receive the apparent declination rate."""
    telescope._equatorial_system = EquatorialCoordinateType.equTopocentric
    target = RateTarget(
        frame=ReferenceFrame.ICRF,
        rates=Equatorial(ra=_ICRS_RA_RATE, dec=0.0),
        initial_time=datetime.now(UTC),
        initial_frame=ReferenceFrame.ICRF,
        initial_coords=_STAR.coords,
    )

    await telescope.telescope_follow_target(FollowTarget(target=target))

    # Frame of date rotation turns part of the RA motion into Dec motion.
    _, _, _, dec_rate = apparent_motion()
    assert telescope.telescope.DeclinationRate == pytest.approx(dec_rate * 3600.0, abs=0.01)


@pytest.mark.asyncio
async def test_topocentric_mount_rates_publish_in_icrs(telescope, recorder):
    """Published axis rates recover the target's ICRS RA/Dec motion."""
    telescope._equatorial_system = EquatorialCoordinateType.equTopocentric
    telescope._tracking = True

    ra, dec, ra_rate, dec_rate = apparent_motion()
    telescope.telescope._properties.update(
        RightAscension=ra / 15.0,
        Declination=dec,
        RightAscensionRate=ra_rate * 3600.0 / 15.0,
        DeclinationRate=dec_rate * 3600.0,
    )

    published = await recorder()
    await telescope._publish_telescope_status()
    rates = await published.wait_for(AxisRates)

    assert rates.right_ascension.velocity == pytest.approx(_ICRS_RA_RATE, abs=5e-6)
    assert rates.declination.velocity == pytest.approx(0.0, abs=5e-6)
