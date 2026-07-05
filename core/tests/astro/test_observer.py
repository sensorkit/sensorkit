# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta

import pytest
from skyfield.api import utc

from sensorkit.astro.observer import EarthObserver


@pytest.mark.asyncio
async def test_observer_bootstrap():
    # Ensure bootstrap can run and .get() returns an instance ready to use
    obs = await EarthObserver.get(40.7128, -74.0060, 10.0)  # New York City
    assert isinstance(obs, EarthObserver)
    # Ensure class-level resources initialized
    assert EarthObserver.timescale is not None
    assert EarthObserver.ephem is not None


@pytest.mark.asyncio
async def test_sunrise_sunset_times():
    # New York City, use a full day window around the summer solstice
    obs = await EarthObserver.get(40.7128, -74.0060, 10.0)
    start = datetime(2024, 6, 21, tzinfo=utc)
    end = start + timedelta(days=2)

    sunrises = obs.get_sunrise_times(start, end)
    sunsets = obs.get_sunset_times(start, end)

    # We should have at least one sunrise and one sunset in a 24h period for NYC
    assert isinstance(sunrises, tuple)
    assert isinstance(sunsets, tuple)
    assert len(sunrises) >= 1
    assert len(sunsets) >= 1

    # Elements should be datetimes and sorted in ascending order
    assert all(isinstance(t, datetime) for t in sunrises)
    assert all(isinstance(t, datetime) for t in sunsets)
    assert list(sunrises) == sorted(sunrises)
    assert list(sunsets) == sorted(sunsets)

    # All returned events should be within the requested window
    assert all(start <= t <= end for t in sunrises)
    assert all(start <= t <= end for t in sunsets)


@pytest.mark.asyncio
async def test_sunrise_sunset_times_latest():
    # Los Angeles, use a 2-day window to ensure multiple events
    obs = await EarthObserver.get(34.0522, -118.2437, 100.0)
    start = datetime(2024, 6, 20, tzinfo=utc)
    end = start + timedelta(days=2)

    first = obs.get_sunrise_time(start, end, latest=False)
    latest = obs.get_sunrise_time(start, end, latest=True)

    assert first is None or isinstance(first, datetime)
    assert latest is None or isinstance(latest, datetime)

    if first and latest:
        assert latest >= first

    first = obs.get_sunset_time(start, end, latest=False)
    latest = obs.get_sunset_time(start, end, latest=True)

    assert first is None or isinstance(first, datetime)
    assert latest is None or isinstance(latest, datetime)

    if first and latest:
        assert latest >= first
