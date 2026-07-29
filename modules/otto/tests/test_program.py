# SPDX-License-Identifier: Apache-2.0
"""Tests for OttoProgram task factory."""

import asyncio
import contextlib
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from .data import ISS_TLE, make_config, make_task
from sensorkit.astro.common import ReferenceFrame, SitePosition
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.otto.horizons import HorizonsCache, HorizonsObject, HorizonsSample
from sensorkit.otto.models import TaskConfig
from sensorkit.otto.program import OttoProgram, OttoState
from sensorkit.otto.stars import StarCache
from sensorkit.otto.task_queue import TaskQueue
from sensorkit.otto.utils import ObjectListManager, to_jd


def _resolved_execution():
    """An already-resolved awaitable standing in for the dispatched execution.

    The task factory resumes with `await (yield ...)`, awaiting the execution the tasking loop
    sends back; in these unit tests we drive the generator directly and feed it a settled future.
    """
    fut = asyncio.get_running_loop().create_future()
    fut.set_result(None)
    return fut


def set_state(program, **lists):
    """Replace the program's object lists, keeping its list manager pointed at them."""
    program.state = OttoState(**lists)
    program.list_manager = ObjectListManager(program.state, program._save_state)


@pytest.fixture
def program(program_impl):
    """An OttoProgram bound to a live program entity, as program_init leaves it."""
    p = OttoProgram()
    p.program = program_impl
    p.task_queue = TaskQueue(program_impl)
    p.config = make_config()
    set_state(p, whitelist=["25544", "42738"])
    return p


class TestGenerateTaskFactory:
    @pytest.mark.asyncio
    async def test_yields_task_from_queue(self, program):
        """generate() should yield a task when one is available."""
        task = make_task()
        await program.task_queue.push_task(task)

        gen = program.generate()
        result = await gen.__anext__()

        assert result is not None
        assert result.task is task

    @pytest.mark.asyncio
    async def test_yields_none_when_empty(self, program):
        """generate() should yield None when queue is empty."""
        gen = program.generate()
        result = await gen.__anext__()
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_expired_tasks(self, program):
        """generate() should skip expired tasks and return next valid one."""
        expired = make_task(end_time=datetime.now(UTC) - timedelta(hours=1))
        valid = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))
        await program.task_queue.push_task(expired)
        await program.task_queue.push_task(valid)

        gen = program.generate()
        result = await gen.__anext__()
        assert result.task is valid

    @pytest.mark.asyncio
    async def test_pops_in_end_time_order(self, program):
        """Tasks should be popped in end_time order (soonest first)."""
        later = make_task(end_time=datetime.now(UTC) + timedelta(hours=2))
        sooner = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))
        await program.task_queue.push_task(later)
        await program.task_queue.push_task(sooner)

        gen = program.generate()
        result = await gen.__anext__()
        assert result.task is sooner


class TestEndTimeRefresh:
    @pytest.mark.asyncio
    async def test_end_time_refreshed_on_pop(self, program):
        """generate() should recalculate end_time based on current time."""
        # Create a task with a stale end_time (as if generated 30 min ago)
        stale_end = datetime.now(UTC) + timedelta(seconds=30)
        task = make_task(
            end_time=stale_end,
            integration_time=10.0,
            frame_count=3,
        )
        await program.task_queue.push_task(task)

        program.config.task.end_time_deadband_seconds = 60

        before = datetime.now(UTC)
        gen = program.generate()
        result = await gen.__anext__()
        after = datetime.now(UTC)

        assert result is not None
        # end_time should be ~now + (10*3) + 60 = now + 90 seconds
        expected_min = before + timedelta(seconds=90)
        expected_max = after + timedelta(seconds=90)
        assert expected_min <= result.task.end_time <= expected_max

    @pytest.mark.asyncio
    async def test_end_time_not_stale(self, program):
        """Even tasks generated hours ago should get a fresh end_time."""
        # But it's still in queue (not yet popped — pop_task removes expired)
        task = make_task(
            end_time=datetime.now(UTC) + timedelta(seconds=1),  # barely valid
            integration_time=5.0,
            frame_count=2,
        )
        await program.task_queue.push_task(task)

        program.config.task.end_time_deadband_seconds = 30

        gen = program.generate()
        result = await gen.__anext__()

        # Should have a fresh end_time: ~now + (5*2) + 30 = now + 40s
        assert result.task.end_time > datetime.now(UTC) + timedelta(seconds=35)


class TestInterTaskDelay:
    DELAY = 0.2
    """Long enough to measure without mocking the clock, short enough not to drag the suite."""

    @pytest.mark.asyncio
    async def test_sleeps_after_task_when_delay_configured(self, program):
        """A configured delay should sleep after a task completes."""
        program.config.task.inter_task_delay_seconds = self.DELAY
        await program.task_queue.push_task(make_task())

        gen = program.generate()
        assert await gen.__anext__() is not None

        # Resume past the yield so the post-task delay runs. The factory awaits the value sent
        # back (the execution), so feed it an already-resolved awaitable.
        started = time.monotonic()
        with pytest.raises(StopAsyncIteration):
            await gen.asend(_resolved_execution())

        assert time.monotonic() - started >= self.DELAY

    @pytest.mark.asyncio
    async def test_no_sleep_when_delay_zero(self, program):
        """The default (0) delay should not sleep between tasks."""
        program.config.task.inter_task_delay_seconds = 0.0
        await program.task_queue.push_task(make_task())

        gen = program.generate()
        await gen.__anext__()

        started = time.monotonic()
        with pytest.raises(StopAsyncIteration):
            await gen.asend(_resolved_execution())

        assert time.monotonic() - started < self.DELAY


class TestObjectListManagement:
    def test_state_whitelist(self):
        state = OttoState(
            whitelist=["25544", "42738"],
            graylist=["39120"],
            blacklist=[],
        )
        assert "25544" in state.whitelist
        assert "39120" in state.graylist

    def test_state_all_lists(self):
        state = OttoState(
            whitelist=["25544"],
            graylist=["42738"],
            blacklist=["12345"],
        )
        all_objects = set(state.whitelist + state.graylist + state.blacklist)
        assert len(all_objects) == 3


@pytest.fixture
def orbit_program(program):
    """Program configured for orbits-only tasking with one fetched GEO member."""
    set_state(program)
    program.config.task.tles = []
    program.config.task.orbits = ["GEO"]
    program.config.collect.altitude_min = 20.0
    program.config.collect.scan_mode = False
    program.config.collect.scan_direction = "eastward"
    program.config.collect.track_mode = "rate"
    program.config.collect.dither = False
    program.config.collect.filters = []
    program.config.collect.exposure_min = 1
    program.config.collect.exposure_max = 1
    program.config.collect.exposure_delta = 1
    program.config.collect.binning = [1]
    program.config.collect.num_frames = 1
    program.latitude = 33.0
    program.longitude = -117.0
    program.altitude_km = 0.1
    program.tles = {
        "19548": {"line0": ISS_TLE.line0, "line1": ISS_TLE.line1, "line2": ISS_TLE.line2}
    }
    program.tles_dt = datetime.now(UTC)
    return program


class TestOrbitTargets:
    """Orbit-regime targets are visibility-selected at generation time, never list-managed."""

    @pytest.mark.asyncio
    async def test_visible_targets_filters_and_orders(self, program, monkeypatch):
        positions = {
            "11111": (45.0, 180.0, True, -2.0),  # up, east of meridian
            "22222": (5.0, 180.0, True, 1.0),  # below the altitude floor
            "33333": (60.0, 180.0, True, 3.0),  # up, west of meridian
        }
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: positions[kwargs["tle_data"]["id"]],
        )
        program.config.collect.altitude_min = 20.0
        program.config.collect.scan_direction = "eastward"
        program.latitude = 33.0
        program.longitude = -117.0
        program.altitude_km = 0.1
        program.tles = {oid: {"id": oid} for oid in positions}

        # eastward scan: descending hour angle, below-floor objects dropped
        targets = await program._visible_targets(["11111", "22222", "33333"])
        assert targets == ["33333", "11111"]

    @pytest.mark.asyncio
    async def test_orbit_member_tasked_without_list_management(self, orbit_program, monkeypatch):
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: (45.0, 180.0, True, 1.0),
        )
        gen = asyncio.create_task(orbit_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        queued = await orbit_program.task_queue.pop_task()
        assert queued is not None
        assert queued.task.target.tle.line1 == ISS_TLE.line1
        # Tasking the member left every list untouched
        assert orbit_program.state == OttoState()

    @pytest.mark.asyncio
    async def test_stale_orbit_member_skipped_not_blacklisted(self, orbit_program, monkeypatch):
        # Visible when the scan list is built, below the floor by execution time
        results = iter([(45.0, 180.0, True, 1.0)])
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: next(results, (5.0, 180.0, False, 1.0)),
        )
        orbit_program.config.collect.scan_mode = True

        gen = asyncio.create_task(orbit_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        assert len(orbit_program.task_queue) == 0
        assert orbit_program.state == OttoState()


class TestProgramDeinit:
    @pytest.mark.asyncio
    async def test_deinit_saves_state(self, program, program_impl):
        """program_deinit should save state."""
        await program.program_deinit()
        assert await program_impl.kv_get_model(OttoState) == program.state


@pytest.fixture
def horizons_program(program):
    """Program configured for a single Horizons object with a cached ephemeris."""
    set_state(program, whitelist=["433"])
    program.config.task.tles = []
    program.config.task.orbits = []
    program.config.task.horizons = ["433"]
    program.config.task.horizons_update_interval_hours = 6
    program.config.collect.dither = False
    program.config.collect.dither_amount_arcsec = 0
    program.latitude = 33.0
    program.longitude = -117.0
    program.altitude_km = 0.1
    program.tles = {}
    program.horizons = {
        "433": HorizonsObject(
            command="433;",
            name="433 Eros (A898 PA)",
            samples=_horizons_samples(datetime.now(UTC) - timedelta(minutes=1)),
        )
    }
    return program


def _horizons_samples(start, count=3, step_minutes=15):
    return [
        HorizonsSample(
            jd=to_jd(start + timedelta(minutes=step_minutes * i)),
            ra=180.0 + i * 0.01,
            dec=-15.0 - i * 0.001,
            ra_rate=100.0,
            dec_rate=-30.0,
            azimuth=90.0,
            elevation=45.0 + i,
        )
        for i in range(count)
    ]


class TestObjectPositionDispatch:
    """Horizons objects and TLE objects share one visibility contract."""

    def test_horizons_object_uses_the_cached_ephemeris(self, horizons_program, monkeypatch):
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: pytest.fail("Horizons object must not take the TLE path"),
        )

        altitude, azimuth, rising, _ = horizons_program._object_position("433")

        # The fixture starts 1 minute back on a 15-minute step from 45° to 46°
        assert altitude == pytest.approx(45.0 + (1 / 15), abs=1e-3)
        assert azimuth == pytest.approx(90.0)
        assert rising is True

    def test_satellite_falls_through_to_the_tle_path(self, horizons_program, monkeypatch):
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: (12.0, 34.0, False, 1.5),
        )
        horizons_program.tles = {"25544": {"line0": "0 25544"}}
        assert horizons_program._object_position("25544") == (12.0, 34.0, False, 1.5)

    def test_missing_tle_reads_as_no_position(self, horizons_program):
        horizons_program.tles = {}
        assert horizons_program._object_position("25544") is None

    def test_stale_ephemeris_reads_as_no_position(self, horizons_program):
        horizons_program.horizons["433"].samples = _horizons_samples(
            datetime.now(UTC) - timedelta(days=2)
        )
        assert horizons_program._object_position("433") is None


class TestHorizonsTarget:
    @pytest.mark.asyncio
    async def test_builds_an_icrf_ephemeris_target(self, horizons_program):
        now = datetime.now(UTC)
        samples = _horizons_samples(now)
        with patch("sensorkit.otto.horizons.fetch_ephemeris", AsyncMock(return_value=samples)):
            target = await horizons_program._target_for("433", True, 120, now)

        assert target.frame == ReferenceFrame.ICRF
        assert target.jds == [s.jd for s in samples]
        assert target.points[0].ra == pytest.approx(samples[0].ra)
        assert target.points[0].dec == pytest.approx(samples[0].dec)

    @pytest.mark.asyncio
    async def test_window_pads_only_the_future_end_by_the_deadband(self, horizons_program):
        """A task dispatched late must still land inside the fetched ephemeris.

        The window starts at generation time: execution can never precede it,
        and padding the past end would only dilute the sampling.
        """
        now = datetime.now(UTC)
        horizons_program.config.task.end_time_deadband_seconds = 300
        fetch = AsyncMock(return_value=_horizons_samples(now))
        with patch("sensorkit.otto.horizons.fetch_ephemeris", fetch):
            await horizons_program._target_for("433", True, 600, now)

        kwargs = fetch.await_args.kwargs
        assert kwargs["command"] == "433;"
        assert kwargs["start"] == now
        assert kwargs["stop"] == now + timedelta(seconds=900)

    @pytest.mark.asyncio
    async def test_deadband_does_not_dilute_the_sampling(self, horizons_program):
        """A large deadband must not spend the interval budget on unobserved time."""
        now = datetime.now(UTC)
        fetch = AsyncMock(return_value=_horizons_samples(now))

        with patch("sensorkit.otto.horizons.fetch_ephemeris", fetch):
            horizons_program.config.task.end_time_deadband_seconds = 300
            await horizons_program._target_for("433", True, 600, now)
            modest = fetch.await_args.kwargs["intervals"]

            horizons_program.config.task.end_time_deadband_seconds = 3600
            await horizons_program._target_for("433", True, 600, now)
            huge = fetch.await_args.kwargs["intervals"]

        # A 12x deadband still buys intervals, but the 600s block keeps its own
        # density — it is no longer paying for the deadband twice over.
        assert huge > modest

    @pytest.mark.asyncio
    async def test_dither_shifts_every_sample_equally(self, horizons_program):
        """The whole path shifts together, so the object stays tracked off-center."""
        now = datetime.now(UTC)
        samples = _horizons_samples(now)
        horizons_program.config.collect.dither = True
        horizons_program.config.collect.dither_amount_arcsec = 500

        with (
            patch("sensorkit.otto.horizons.fetch_ephemeris", AsyncMock(return_value=samples)),
            patch("sensorkit.otto.horizons.dither_offset", return_value=(0.02, -0.01)),
        ):
            target = await horizons_program._target_for("433", True, 120, now)

        for point, sample in zip(target.points, samples, strict=True):
            assert point.ra == pytest.approx(sample.ra + 0.02)
            assert point.dec == pytest.approx(sample.dec - 0.01)

    @pytest.mark.asyncio
    async def test_fetch_failure_skips_the_target(self, horizons_program):
        with patch(
            "sensorkit.otto.horizons.fetch_ephemeris",
            AsyncMock(side_effect=RuntimeError("Horizons unreachable")),
        ):
            assert await horizons_program._target_for("433", True, 120, datetime.now(UTC)) is None

    @pytest.mark.asyncio
    async def test_single_sample_is_rejected(self, horizons_program):
        """Mounts finite-difference a rate from adjacent samples; one point can't."""
        now = datetime.now(UTC)
        with patch(
            "sensorkit.otto.horizons.fetch_ephemeris",
            AsyncMock(return_value=_horizons_samples(now, count=1)),
        ):
            assert await horizons_program._target_for("433", True, 120, now) is None


async def _one_horizons_pass(program):
    """Drive the Horizons updater through exactly one pass, then stop it.

    The loop's trailing sleep is the configured refresh interval — hours — so a brief yield
    lets the body run once and no more.
    """
    updater = asyncio.create_task(program.update_horizons_loop())
    await asyncio.sleep(0.1)
    updater.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await updater


class TestHorizonsUpdate:
    @pytest.mark.asyncio
    async def test_resolves_and_caches_new_objects(self, horizons_program, program_impl):
        horizons_program.horizons = {}
        now = datetime.now(UTC)

        with (
            patch(
                "sensorkit.otto.horizons.resolve",
                AsyncMock(return_value=("433;", "433 Eros (A898 PA)")),
            ),
            patch(
                "sensorkit.otto.horizons.fetch_ephemeris",
                AsyncMock(return_value=_horizons_samples(now)),
            ),
        ):
            await _one_horizons_pass(horizons_program)

        assert horizons_program.horizons["433"].command == "433;"
        assert horizons_program.horizons["433"].name == "433 Eros (A898 PA)"

        cached = await program_impl.kv_get_model(HorizonsCache)
        assert cached.objects == horizons_program.horizons

    @pytest.mark.asyncio
    async def test_known_object_is_not_re_resolved(self, horizons_program):
        """Resolutions are sticky, so a refresh costs one JPL call, not two."""
        resolve = AsyncMock()
        with (
            patch("sensorkit.otto.horizons.resolve", resolve),
            patch(
                "sensorkit.otto.horizons.fetch_ephemeris",
                AsyncMock(return_value=_horizons_samples(datetime.now(UTC))),
            ),
        ):
            await _one_horizons_pass(horizons_program)

        resolve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_name_is_blacklisted(self, horizons_program):
        horizons_program.horizons = {}
        horizons_program.config.task.horizons = ["Jupiter", "433"]

        async def resolve(name, *args, **kwargs):
            return None if name == "Jupiter" else ("433;", "433 Eros (A898 PA)")

        with (
            patch("sensorkit.otto.horizons.resolve", AsyncMock(side_effect=resolve)),
            patch(
                "sensorkit.otto.horizons.fetch_ephemeris",
                AsyncMock(return_value=_horizons_samples(datetime.now(UTC))),
            ),
        ):
            horizons_program.state.whitelist = ["Jupiter", "433"]
            await _one_horizons_pass(horizons_program)

        assert horizons_program.state.blacklist == ["Jupiter"]
        # The rest of the list keeps tasking
        assert horizons_program.state.whitelist == ["433"]
        assert "433" in horizons_program.horizons

    @pytest.mark.asyncio
    async def test_failed_refresh_keeps_the_previous_ephemeris(self, horizons_program):
        """A JPL outage must not strand a target that still has usable samples."""
        previous = horizons_program.horizons["433"].samples

        with patch(
            "sensorkit.otto.horizons.fetch_ephemeris",
            AsyncMock(side_effect=RuntimeError("Horizons unreachable")),
        ):
            await _one_horizons_pass(horizons_program)

        assert horizons_program.horizons["433"].samples == previous

    @pytest.mark.asyncio
    async def test_removed_object_drops_out_of_the_cache(self, horizons_program):
        horizons_program.config.task.horizons = []
        await _one_horizons_pass(horizons_program)
        assert horizons_program.horizons == {}


class TestEphemerisCoversTheBlock:
    """The fetched ephemeris must span every task it is attached to."""

    @pytest.mark.asyncio
    async def test_window_ends_at_the_last_task_deadline(self, horizons_program):
        """Ephemeris stop == the final queued task's end_time.

        One ephemeris is shared by the whole block of tasks generated for an
        object, so its window has to reach the last of them. `end_time` already
        carries `end_time_deadband_seconds`, so matching it exactly gives the
        block its integration time plus one deadband of slack.
        """
        horizons_program.config.task.end_time_deadband_seconds = 300
        horizons_program.config.task.inter_task_delay_seconds = 0
        horizons_program.config.collect.altitude_min = 20.0
        horizons_program.config.collect.scan_mode = False
        horizons_program.config.collect.track_mode = "rate"
        horizons_program.config.collect.dither = False
        horizons_program.config.collect.filters = ["Lum", "R"]
        horizons_program.config.collect.exposure_min = 2
        horizons_program.config.collect.exposure_max = 4
        horizons_program.config.collect.exposure_delta = 2
        horizons_program.config.collect.binning = [1, 2]
        horizons_program.config.collect.num_frames = 3

        fetch = AsyncMock(return_value=_horizons_samples(datetime.now(UTC)))
        with patch("sensorkit.otto.horizons.fetch_ephemeris", fetch):
            gen = asyncio.create_task(horizons_program.generate_tasks())
            await asyncio.sleep(0.1)
            gen.cancel()

        # 2 filters x 2 exposures x 2 binnings
        assert len(horizons_program.task_queue) == 8
        # An ephemeris target has no intrinsic id, so the operator's object identifier is
        # passed through as target_id (the collect's only source of one for this target type).
        assert all(q.task.target_id == "433" for q in horizons_program.task_queue.tasks)
        latest = max(q.task.end_time for q in horizons_program.task_queue.tasks)
        assert fetch.await_args.kwargs["stop"] == latest

    @pytest.mark.asyncio
    async def test_window_is_blind_to_slew_and_readout_overhead(self, horizons_program):
        """Known gap: the block estimate is integration time only.

        Real wall-clock adds slew/settle, readout, and inter_task_delay, so a
        long block can outrun its ephemeris and the mounts fall back to
        extrapolating from the final samples.
        """
        now = datetime.now(UTC)
        horizons_program.config.task.end_time_deadband_seconds = 0
        fetch = AsyncMock(return_value=_horizons_samples(now))
        with patch("sensorkit.otto.horizons.fetch_ephemeris", fetch):
            await horizons_program._target_for("433", True, 600, now)

        assert fetch.await_args.kwargs["stop"] == now + timedelta(seconds=600)


@pytest.fixture
def star_program(program):
    """Program configured for a single resolved star."""
    set_state(program, whitelist=["Vega"])
    program.config.task.tles = []
    program.config.task.orbits = []
    program.config.task.horizons = []
    program.config.task.stars = ["Vega"]
    program.config.collect.dither = False
    program.config.collect.dither_amount_arcsec = 0
    program.config.collect.altitude_min = 20.0
    program.latitude, program.longitude, program.altitude_km = 40.0, 0.0, 0.1
    program.tles = {}
    program.horizons = {}
    program.stars = {"Vega": Equatorial(ra=279.23473, dec=38.78369)}
    return program


class TestStarTargets:
    def test_position_uses_the_fixed_coordinates(self, star_program, monkeypatch):
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: pytest.fail("a star must not take the TLE path"),
        )
        altitude, azimuth, rising, ha = star_program._object_position("Vega")

        assert -90.0 <= altitude <= 90.0
        assert 0.0 <= azimuth < 360.0
        assert isinstance(rising, bool)
        assert -12.0 <= ha < 12.0

    @pytest.mark.asyncio
    async def test_builds_a_fixed_icrf_target(self, star_program):
        target = await star_program._target_for("Vega", True, 0, datetime.now(UTC))

        assert isinstance(target, ICRSTarget)
        assert target.frame == ReferenceFrame.ICRF
        assert target.coords.ra == pytest.approx(279.23473)
        assert target.coords.dec == pytest.approx(38.78369)

    @pytest.mark.asyncio
    async def test_dither_offsets_the_pointing(self, star_program):
        star_program.config.collect.dither = True
        star_program.config.collect.dither_amount_arcsec = 500

        with patch("sensorkit.otto.stars.dither_offset", return_value=(0.02, -0.01)):
            target = await star_program._target_for("Vega", True, 0, datetime.now(UTC))

        assert target.coords.ra == pytest.approx(279.23473 + 0.02)
        assert target.coords.dec == pytest.approx(38.78369 - 0.01)


class TestStarListManagement:
    """A star always comes back, so it must never be permanently blacklisted."""

    @pytest.mark.asyncio
    async def test_set_star_graylists_rather_than_blacklists(self, star_program, monkeypatch):
        # Below the floor and west of the meridian: setting
        monkeypatch.setattr(star_program, "_object_position", lambda obj: (5.0, 270.0, False, 3.0))
        star_program.config.collect.scan_mode = False

        gen = asyncio.create_task(star_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        assert star_program.state == OttoState(graylist=["Vega"])

    @pytest.mark.asyncio
    async def test_set_satellite_still_blacklists(self, orbit_program, monkeypatch):
        """The diurnal-return exemption must not change satellite behaviour."""
        orbit_program.state.whitelist = ["25544"]
        orbit_program.stars = {}
        orbit_program.tles["25544"] = {"line0": "0 25544"}
        monkeypatch.setattr(
            "sensorkit.otto.tles.position",
            lambda **kwargs: (5.0, 270.0, False, 3.0),
        )

        gen = asyncio.create_task(orbit_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        assert orbit_program.state == OttoState(blacklist=["25544"])


class TestStarResolution:
    @pytest.mark.asyncio
    async def test_resolves_and_caches(self, star_program, program_impl):
        star_program.stars = {}
        with patch(
            "sensorkit.otto.stars.resolve",
            AsyncMock(return_value=Equatorial(ra=279.23473, dec=38.78369)),
        ):
            await star_program.update_stars()

        assert star_program.stars["Vega"].ra == pytest.approx(279.23473)

        cached = await program_impl.kv_get_model(StarCache)
        assert cached.stars == star_program.stars

    @pytest.mark.asyncio
    async def test_known_star_is_not_re_resolved(self, star_program):
        """Positions are static, so a cached name never hits the network again."""
        resolve = AsyncMock()
        with patch("sensorkit.otto.stars.resolve", resolve):
            await star_program.update_stars()

        resolve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_name_is_blacklisted(self, star_program):
        star_program.stars = {}
        with patch("sensorkit.otto.stars.resolve", AsyncMock(return_value=None)):
            await star_program.update_stars()

        assert star_program.stars == {}
        assert star_program.state == OttoState(blacklist=["Vega"])

    @pytest.mark.asyncio
    async def test_previously_blacklisted_name_recovers_when_it_resolves(self, star_program):
        """A resolver outage blacklists; resolving on a later run must undo that.

        For stars that later run is a restart or a `task.stars` edit, since
        resolution is one-shot.
        """
        star_program.stars = {}
        set_state(star_program, blacklist=["Vega"])

        with patch(
            "sensorkit.otto.stars.resolve",
            AsyncMock(return_value=Equatorial(ra=279.23473, dec=38.78369)),
        ):
            await star_program.update_stars()

        assert star_program.state == OttoState(whitelist=["Vega"])

    @pytest.mark.asyncio
    async def test_removed_star_drops_out_of_the_cache(self, star_program):
        star_program.config.task.stars = []
        await star_program.update_stars()
        assert star_program.stars == {}


class TestDiurnalReturnExemption:
    """Sources that set diurnally and return (stars, Horizons objects) graylist;
    only a satellite whose pass has ended blacklists."""

    @pytest.mark.asyncio
    async def test_set_horizons_object_graylists_rather_than_blacklists(
        self, horizons_program, monkeypatch
    ):
        # Below the floor and west of the meridian: setting — but an asteroid
        # is back tomorrow night, so blacklisting would lose it forever
        monkeypatch.setattr(
            horizons_program, "_object_position", lambda obj: (5.0, 270.0, False, 3.0)
        )
        horizons_program.config.collect.scan_mode = False
        horizons_program.config.collect.altitude_min = 20.0
        horizons_program.tles = {}

        gen = asyncio.create_task(horizons_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        assert horizons_program.state == OttoState(graylist=["433"])


class TestUnresolvedNamesAreNotMisfiled:
    """A configured star/Horizons name whose resolution hasn't completed must
    not fall through to the TLE path and be blacklisted as 'no TLE available'."""

    @pytest.mark.asyncio
    async def test_unresolved_star_is_skipped_not_blacklisted(self, star_program):
        # TLEs are ready, but Vega hasn't resolved yet (startup race): the
        # whitelist filter passes it, and _object_position finds it nowhere
        star_program.stars = {}
        star_program.tles = {"99999": {"line0": "0 99999"}}
        star_program.tles_dt = datetime.now(UTC)
        star_program.config.collect.scan_mode = False

        gen = asyncio.create_task(star_program.generate_tasks())
        await asyncio.sleep(0.1)
        gen.cancel()

        assert star_program.state == OttoState(whitelist=["Vega"])


class TestStartupOrdering:
    @pytest.mark.asyncio
    async def test_site_position_is_set_before_the_updaters_start(
        self, program_impl, kit, monkeypatch
    ):
        """The Horizons updater asks JPL for ephemerides at the observing site.

        Starting it before the site position is fetched leaves every fetch
        raising AttributeError on self.latitude — caught per-object, so the
        cache silently comes up empty and is not retried until the next refresh.
        """
        program = OttoProgram()
        config = make_config(task=TaskConfig(horizons=["433"]))
        site = SitePosition(latitude_degrees=42.3, longitude_degrees=-83.0, altitude_km=0.2)

        await program_impl.kv_put_model(config)
        await kit.controller(config.controller).kv_put_model(site)

        # Record whether the site position was already resolved as each
        # background task was created, without actually running any of them
        had_site = {}

        def spy(coro, *args, **kwargs):
            had_site[coro.cr_code.co_name] = hasattr(program, "latitude")
            coro.close()
            return asyncio.get_running_loop().create_future()

        monkeypatch.setattr("sensorkit.otto.program.asyncio.create_task", spy)

        await program.program_init()

        assert had_site["update_horizons_loop"] is True
        assert had_site["generate_tasks"] is True
