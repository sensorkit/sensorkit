from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.observer import EarthObserver
from sensorkit.astro.target import ICRSTarget
from sensorkit.autofocus.analyzer import AutofocusAnalyzer
from sensorkit.autofocus.models import AutofocusConfig, VCurveStep
from sensorkit.autofocus.task_queue import TaskQueue
from sensorkit.common.time import parse_spec
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask
from sensorkit.std.optics import FocusPosition


@sk.declare_program
class AutofocusProgram:
    def __init__(self, config: AutofocusConfig, analyzer: AutofocusAnalyzer):
        self.config = config

        self.task_queue: TaskQueue | None = None
        self._scheduler_task: asyncio.Task | None = None

        self._observer: EarthObserver | None = None
        self._site_position: SitePosition | None = None

        self.analyzer = analyzer

    @sk.on_attach
    async def program_init(self):
        """Setup schedule, optionally queue a V-curve."""

        self.program = sk.program()

        logger.debug("starting Autofocus program")

        # Initialize task queue
        self.task_queue = TaskQueue(self.program)

        # Get site position from the controller's KV store
        try:
            kv = self.program.backend.key_value(sk.Entity.at(self.config.controller))
            entry = await kv.get("SitePosition")
            self._site_position = SitePosition.model_validate_json(entry.value)
            self._observer = await EarthObserver.get(
                self._site_position.latitude_degrees,
                self._site_position.longitude_degrees,
                self._site_position.altitude_km * 1000,
            )
        except Exception:
            logger.warning("Could not get site position; V-curve scheduling may be limited")

        # Start V-curve scheduler
        self._scheduler_task = asyncio.create_task(self._vcurve_scheduler())

        # Queue initial V-curve if configured
        if self.config.calibrate_on_startup:
            await self.queue_vcurve()

    @sk.on_detach
    async def program_deinit(self):
        """Stop scheduler."""

        logger.debug("stopping Autofocus program")

        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()

    @sk.task_factory
    async def generate(self):
        """Program task generator.

        Pulls the next queued V-curve step (a StandardCollectTask) and submits it for
        execution, attaching the sweep's VCurveStep context.

        Yields:
            The submitted collect task, or None when the queue is empty.
        """
        if queued := await self.task_queue.pop_task():
            task = queued.task

            # Fresh execution deadline from now (mirrors Otto), so a task that waited through
            # controller init isn't already expired. pop_task already pruned against the
            # queue-time deadline set in queue_vcurve.
            task.end_time = datetime.now(UTC) + timedelta(
                seconds=task.camera_params.integration_time_seconds
                + self.config.vcurve.frame_timeout_seconds
            )

            logger.info(
                f"task ({queued.id}): target -> {task.target}, "
                f"focus -> {task.focus_position}, camera -> {task.camera_params}"
            )
            try:
                await (yield task.submit(context=queued.context, expiry_time=task.end_time))
                logger.info(f"Task ({queued.id}) completed")
            except asyncio.CancelledError:
                logger.warning(f"Task ({queued.id}) cancelled")
                raise
            except Exception as e:
                # Dispatch failures surface here; re-raise like Otto so the loop stops and the
                # agent re-activates tasking when the controller is ready. A lost step is absorbed
                # by the analyzer's sweep stall-timeout.
                logger.exception(f"Task ({queued.id}) failed: {e}")
                raise
        else:
            yield None

    async def _vcurve_scheduler(self):
        """Schedules V-curve calibrations based on user-configured time(s)."""

        if self.config.vcurve.schedule is None:
            logger.info("No V-curve schedule configured; scheduled sweeps disabled")

        while True:
            try:
                # Quiet when unset — re-check hourly in case a schedule arrives via config reload.
                if self.config.vcurve.schedule is None:
                    await asyncio.sleep(3600)
                    continue

                next_time = self._compute_next_vcurve_time()
                if next_time is None:
                    logger.warning("Could not compute next V-curve time; retrying in 1h")
                    await asyncio.sleep(3600)
                    continue

                now = datetime.now(UTC)
                delay = (next_time - now).total_seconds()

                if delay > 0:
                    logger.info(
                        f"Next V-curve scheduled at {next_time.replace(tzinfo=None).isoformat()} ({delay / 3600:.1f}h from now)"
                    )
                    await asyncio.sleep(delay)

                # Queue the V-curve
                await self.queue_vcurve()

                # Wait a bit before computing the next one
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Keep the scheduler alive through transient failures (e.g. a broker hiccup while
                # publishing offers) — dying here would silently end all scheduled V-curves.
                logger.exception(f"Unable to schedule V-curve: {e}")
                await asyncio.sleep(60)

    def _compute_next_vcurve_time(self) -> datetime | None:
        """Parse the schedule spec to find the next V-curve time."""

        if self._observer is None:
            return None

        now = datetime.now(UTC)

        def sunset_handler(range_min, range_max, latest_match):
            return self._observer.get_sunset_time(range_min, range_max)

        def sunrise_handler(range_min, range_max, latest_match):
            return self._observer.get_sunrise_time(range_min, range_max)

        symbol_handlers = {
            "sunset": sunset_handler,
            "sunrise": sunrise_handler,
        }

        try:
            return parse_spec(
                self.config.vcurve.schedule,
                now,
                now + timedelta(days=1),
                symbol_handlers=symbol_handlers,
            )
        except Exception:
            logger.warning(f"Could not parse V-curve schedule: {self.config.vcurve.schedule}")
            return None

    async def queue_vcurve(self, target: ICRSTarget | None = None) -> bool:  # noqa: C901  (linear guard→select→queue sequence)
        """Queue a V-curve sweep as a batch of per-step collect tasks.

        Each step is a StandardCollectTask that drives the focuser to one position and captures
        a frame; all steps of one sweep share a VCurveStep(session) so the analyzer can regroup
        them and fit the curve.

        Args:
            target: Optional target to slew to. If None, auto-selects a bright star.

        Returns:
            True if the sweep was queued, False if it was skipped.
        """

        if self.task_queue is None:
            logger.warning("V-curve requested before the program attached; skipping sweep")
            return False

        # One sweep at a time (the analyzer accumulates a single sweep). peek_task prunes expired
        # steps first, so a dead sweep ages out instead of blocking new ones forever.
        if await self.task_queue.peek_task() is not None:
            logger.warning("A V-curve sweep is already queued; skipping new sweep")
            return False

        # Refresh config from the Autofocus entity (the config section lives there, not on the
        # V-Curve program entity) so `sensorkit config load` tuning applies without a restart.
        try:
            self.config = await (
                self.program.sensorkit().entity(self.config.entity).kv_get_model(AutofocusConfig)
            )
        except Exception as e:
            logger.debug(f"Could not refresh AutofocusConfig; using cached config ({e})")

        # Auto-select target if not provided (off-loop: catalog load + coord transforms).
        if target is None and self._site_position is not None:
            target = await asyncio.to_thread(
                _select_vcurve_target,
                self.config.min_altitude,
                self.config.min_solar_elongation,
                self.config.min_magnitude,
                self.config.max_magnitude,
                self._site_position.latitude_degrees,
                self._site_position.longitude_degrees,
                self.config.catalog_path,
            )

        if target is None:
            logger.warning("No V-curve target available; skipping sweep")
            return False

        # Center the sweep on current focus, falling back to the range midpoint. FocusPosition is
        # a published keyword with no direct getter, so monitor() replays the retained value.
        center = (self.config.focuser.min_position + self.config.focuser.max_position) / 2.0
        focuser = self.analyzer.sk_client.device(self.config.focuser.entity)
        try:
            async with asyncio.timeout(self.config.focuser.timeout):
                async for _, fp in await focuser.monitor(FocusPosition):
                    center = fp.current_position
                    break
        except Exception as e:
            logger.warning(
                f"Could not read current focus position; sweeping around midpoint ({e})"
            )

        # Compute the sweep positions.
        positions = self._sweep_positions(center)
        if len(positions) < 3:
            logger.warning(f"V-curve needs >=3 steps, got {len(positions)}; skipping")
            return False

        session = uuid.uuid4().hex[:12]
        count = len(positions)
        exposure = self.config.vcurve.exposure_time
        binning = self.config.vcurve.binning

        now = datetime.now(UTC)
        move_budget = self.config.focuser.timeout
        for index, position in enumerate(positions):
            # Stagger per-step deadlines so a step queued now does not expire before the
            # controller works through the earlier steps of the sweep: each step is budgeted
            # one focuser move + one exposure.
            end_time = now + timedelta(seconds=(index + 1) * (exposure + move_budget))
            task = StandardCollectTask(
                target=target,
                focus_position=position,
                end_time=end_time,
                camera_params=CameraParameterSet(
                    filter_name=self.config.vcurve.filter_name,
                    integration_time_seconds=exposure,
                    binning_x=binning,
                    binning_y=binning,
                    frame_count=1,
                ),
            )
            await self.task_queue.push_task(
                task,
                context=sk.KeywordDict(
                    VCurveStep(
                        session=session,
                        pipeline_mode=self.config.vcurve.pipeline_mode,
                    )
                ),
            )

        logger.info(
            f"Queued V-curve {session}: {count} steps over "
            f"[{positions[0]:.0f}, {positions[-1]:.0f}]"
        )

        # Tell the analyzer how many steps to expect, so it finalizes the fit the instant the last
        # frame arrives instead of waiting for its stall timeout (the timeout stays as the fallback).
        self.analyzer.expect_sweep(session, count)
        return True

    def _sweep_positions(self, center: float) -> list[float]:
        """Focuser positions for a V-curve centered on `center` (the current focus position).

        The sweep is `num_steps` samples spaced by `step_size` focuser steps, centered on
        `center`. `min_position`/`max_position` are the focuser's hard limits: any sample
        outside them is dropped, so the sweep truncates near a limit rather than pushing past
        it. If `step_size` is unset, `num_steps` samples span the full [min, max] range instead.
        """
        lo = self.config.focuser.min_position
        hi = self.config.focuser.max_position
        center = min(max(center, lo), hi)
        num_steps = max(self.config.vcurve.num_steps, 3)
        step_size = self.config.vcurve.step_size

        if step_size and step_size > 0:
            # num_steps samples spaced by step_size, centered on the current focus position.
            offset = (num_steps - 1) / 2.0
            raw = [center + (i - offset) * step_size for i in range(num_steps)]
        elif hi > lo:
            # No step_size: spread num_steps samples across the full focuser range.
            span = hi - lo
            raw = [lo + span * i / (num_steps - 1) for i in range(num_steps)]
        else:
            raw = []

        # Keep only samples within the focuser's hard limits.
        return [p for p in raw if lo <= p <= hi]


# Fallback pointing altitude when the galactic plane is unreachable: near zenith (airmass ~1.02)
# on the anti-sun azimuth, clear of the zenith tracking/dome degeneracy.
_FALLBACK_ALTITUDE = 78.0


def _select_vcurve_target(
    min_altitude: float,
    min_solar_elongation: float,
    min_magnitude: float,
    max_magnitude: float | None,
    site_lat: float,
    site_lon: float,
    catalog_path: str | None,
) -> ICRSTarget | None:
    """Select a star for a V-curve from the SSTRC7 catalog (SENPAI's, reused).

    Prefers the galactic plane — the densest star fields: takes the highest-altitude pointing
    along the galactic equator that clears `min_altitude` and `min_solar_elongation`, queries a
    1.5° box there, and slews to the brightest star no brighter than `min_magnitude` (brighter
    would saturate). `max_magnitude` optionally caps the faint end of the selection; None means
    the catalog's own faint limit. When the whole plane is unusable (it sets entirely below
    `min_altitude` when a galactic pole transits near zenith, or hugs the Sun), falls back to a
    near-zenith anti-sun pointing.

    Returns an ICRSTarget, or None if nothing suitable is found.
    """
    if not catalog_path:
        logger.warning("No catalog_path configured; cannot select a V-curve target")
        return None

    try:
        import astropy.units as u
        import numpy as np
        from astropy.coordinates import AltAz, EarthLocation, Galactic, SkyCoord, get_sun
        from astropy.time import Time
        from numpy import cos, degrees, radians
    except ImportError:
        logger.warning("astropy not available for target selection")
        return None

    now = Time.now()
    location = EarthLocation(lat=site_lat * u.deg, lon=site_lon * u.deg)
    altaz = AltAz(obstime=now, location=location)
    sun = get_sun(now)

    # The highest-altitude point on the galactic equator that satisfies the constraints.
    plane = SkyCoord(l=np.arange(0.0, 360.0, 5.0) * u.deg, b=0.0 * u.deg, frame=Galactic()).icrs
    plane_alt = plane.transform_to(altaz).alt.deg
    usable = plane_alt >= min_altitude
    if min_solar_elongation > 0:
        usable &= plane.separation(sun).deg >= min_solar_elongation

    if usable.any():
        best = int(np.argmax(np.where(usable, plane_alt, -np.inf)))
        pointing = plane[best]
        pointing_desc = f"galactic plane, alt~{plane_alt[best]:.0f}°"
    else:
        alt = max(_FALLBACK_ALTITUDE, min_altitude)
        az = (sun.transform_to(altaz).az.deg + 180.0) % 360.0  # anti-sun
        pointing = SkyCoord(alt=alt * u.deg, az=az * u.deg, frame=altaz).icrs
        if min_solar_elongation > 0 and pointing.separation(sun).deg < min_solar_elongation:
            logger.warning(
                "No pointing satisfies min_altitude/min_solar_elongation; "
                "cannot select a V-curve target"
            )
            return None
        pointing_desc = f"anti-sun fallback, alt~{alt:.0f}°"
        logger.info("Galactic plane too low or too close to the Sun; using anti-sun pointing")

    # Query SSTRC7 around the pointing. SENPAI's reader (`senpai.catalog.sstr7` — the module file
    # is misnamed upstream) takes RADIANS; rows carry 'ra'/'dec' in radians and 'mv'.
    radius = 1.5
    cosdec = max(float(cos(radians(pointing.dec.deg))), 0.05)
    try:
        from senpai.catalog.sstr7 import query_by_min_max

        stars = query_by_min_max(
            radians(pointing.ra.deg - radius / cosdec),
            radians(pointing.ra.deg + radius / cosdec),
            radians(pointing.dec.deg - radius),
            radians(pointing.dec.deg + radius),
            catalog_path,
            faint_lim=max_magnitude,  # None -> the catalog's faint end
            bright_lim=min_magnitude,
        )
    except Exception as e:
        logger.warning(f"SSTRC7 query failed ({e})")
        return None

    if not stars:
        logger.warning("No SSTRC7 star found at the V-curve pointing")
        return None

    star = min(stars, key=lambda s: s["mv"])  # brightest usable in the field
    ra, dec = float(degrees(star["ra"])), float(degrees(star["dec"]))
    logger.info(
        f"Selected V-curve target: mag={star['mv']:.1f}, RA={ra:.2f}°, Dec={dec:.2f}° "
        f"({pointing_desc}, {len(stars)} field stars)"
    )
    return ICRSTarget(coords=Equatorial(ra=ra, dec=dec))


@sk.service_entrypoint(version=sk.VERSION)
async def autofocus_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(AutofocusConfig)

    analyzer = AutofocusAnalyzer(config, service.client)
    program = AutofocusProgram(config, analyzer)
    analyzer.program = program  # mutual refs: the analyzer queues recalibration sweeps via us

    # Nameless include -> the analyzer is the service's primary entity (its id, e.g. "Autofocus"),
    # where AutofocusStatus is published. The program is a secondary entity referenced by tasking.
    service.include(analyzer)
    service.include(program, name="V-Curve")

    await service.run()
