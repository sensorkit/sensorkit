# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto utility functions."""

import pytest

from sensorkit.otto.program import OttoState
from sensorkit.otto.utils import (
    ListType,
    ObjectListManager,
    dither_offset,
    interpolate_angle,
    normalize_degrees,
    sidereal_frames,
)


class TestObjectListManager:
    @pytest.fixture
    def state(self):
        return OttoState(
            whitelist=["25544", "42738", "39120"],
            graylist=[],
            blacklist=[],
        )

    @pytest.fixture
    def manager(self, state):
        """The manager, paired with the list of states its save callback was handed."""
        saved: list[OttoState] = []

        async def save():
            saved.append(state.model_copy(deep=True))

        return ObjectListManager(state, save), saved

    @pytest.mark.asyncio
    async def test_move_whitelist_to_graylist(self, state, manager):
        mgr, saved = manager
        await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert "25544" not in state.whitelist
        assert "25544" in state.graylist
        assert saved[-1].graylist == ["25544"]

    @pytest.mark.asyncio
    async def test_move_whitelist_to_blacklist(self, state, manager):
        mgr, save = manager
        await mgr.move_object("42738", ListType.WHITELIST, ListType.BLACKLIST)
        assert "42738" not in state.whitelist
        assert "42738" in state.blacklist

    @pytest.mark.asyncio
    async def test_move_graylist_to_whitelist(self, state, manager):
        mgr, _ = manager
        # First move to graylist
        await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert "25544" in state.graylist

        # Then back to whitelist
        await mgr.move_object("25544", ListType.GRAYLIST, ListType.WHITELIST)
        assert "25544" in state.whitelist
        assert "25544" not in state.graylist

    @pytest.mark.asyncio
    async def test_move_nonexistent_object_raises(self, state, manager):
        mgr, _ = manager
        with pytest.raises(ValueError):
            await mgr.move_object("99999", ListType.WHITELIST, ListType.GRAYLIST)


class TestListType:
    def test_values(self):
        assert ListType.WHITELIST.value == "whitelist"
        assert ListType.GRAYLIST.value == "graylist"
        assert ListType.BLACKLIST.value == "blacklist"


class TestSiderealFrames:
    """track_mode -> per-frame sidereal switches on the StandardCollectTask."""

    def test_rate_pins_no_frames(self):
        assert sidereal_frames("rate", 5) == []

    def test_rate_sidereal_pins_final_frame(self):
        assert sidereal_frames("rate_sidereal", 5) == [4]

    def test_sidereal_pins_every_frame(self):
        assert sidereal_frames("sidereal", 3) == [0, 1, 2]


class TestDitherOffset:
    def test_offset_within_requested_magnitude(self):
        import math

        for _ in range(50):
            delta_ra, delta_dec = dither_offset(dec=0.0, dither_arcsec=500)
            on_sky = math.hypot(delta_ra, delta_dec) * 3600
            assert 0 <= on_sky <= 500

    def test_ra_is_deprojected_by_cos_dec(self, monkeypatch):
        """At high declination a given on-sky offset needs a larger RA step."""
        # Each call draws a magnitude then a position angle; pin both to the full offset
        # due east so only the cos(dec) de-projection varies.
        draws = iter([500.0, 0.0, 500.0, 0.0])
        monkeypatch.setattr("sensorkit.otto.utils.random.uniform", lambda *_: next(draws))

        equator_ra, _ = dither_offset(dec=0.0, dither_arcsec=500)
        polar_ra, _ = dither_offset(dec=60.0, dither_arcsec=500)

        # cos(60°) = 0.5, so the RA offset should double
        assert polar_ra == pytest.approx(equator_ra * 2, rel=1e-6)

    def test_no_offset_when_amount_is_zero(self):
        assert dither_offset(dec=0.0, dither_arcsec=0) == (0.0, 0.0)


class TestNormalizeDegrees:
    def test_leaves_in_range_angles_alone(self):
        assert normalize_degrees(0.0) == 0.0
        assert normalize_degrees(180.0) == 180.0
        assert normalize_degrees(359.9) == pytest.approx(359.9)

    def test_wraps_out_of_range_angles(self):
        assert normalize_degrees(370.0) == pytest.approx(10.0)
        assert normalize_degrees(-10.0) == pytest.approx(350.0)

    def test_never_returns_360(self):
        """A tiny negative wraps to exactly 360.0 under plain modulo."""
        assert (-1e-16) % 360.0 == 360.0  # the trap this guards
        assert normalize_degrees(-1e-16) == 0.0


class TestInterpolateAngle:
    def test_midpoint(self):
        assert interpolate_angle(10.0, 20.0, 0.5) == pytest.approx(15.0)

    def test_endpoints(self):
        assert interpolate_angle(10.0, 20.0, 0.0) == pytest.approx(10.0)
        assert interpolate_angle(10.0, 20.0, 1.0) == pytest.approx(20.0)

    def test_takes_the_short_way_across_north(self):
        assert interpolate_angle(359.0, 1.0, 0.5) == pytest.approx(0.0)
        assert interpolate_angle(1.0, 359.0, 0.5) == pytest.approx(0.0)

    def test_short_way_result_stays_in_range(self):
        assert 0.0 <= interpolate_angle(359.0, 1.0, 0.25) < 360.0
        assert interpolate_angle(359.0, 1.0, 0.25) == pytest.approx(359.5)
