# SPDX-License-Identifier: Apache-2.0
import asyncio
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Dict, List

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import BaseTarget
from sensorkit.otto import horizons, stars, tles
from sensorkit.otto.horizons import HorizonsCache, HorizonsObject
from sensorkit.otto.models import OttoConfig
from sensorkit.otto.stars import StarCache
from sensorkit.otto.task_queue import TaskQueue
from sensorkit.otto.tles import TLECache
from sensorkit.otto.utils import ListType, ObjectListManager, sidereal_frames
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask


@sk.declare_keyword
class OttoContext(BaseModel):
    """Otto's per-visit correlation id, attached to every collect task Otto emits for one
    target-visit.

    All the filter/exposure/binning tasks of a single visit — and every frame fanned out
    from them — share one `visit_id`, so downstream processing can regroup a whole visit
    (e.g. combine multi-band frames into a colour). Distinct visits differ. Reachable from
    FITS-header exprs and filename templates as `OttoContext.visit_id`. Namespaced so it
    does not collide with the flat keys the std controller injects (target_id, track_mode, ...).
    """

    visit_id: str


class OttoState(BaseModel):
    """Persistent state for Otto program."""

    blacklist: List[str] = Field(default_factory=list)
    graylist: List[str] = Field(default_factory=list)
    whitelist: List[str] = Field(default_factory=list)


@sk.declare_program
class OttoProgram:
    """Autonomous tasking program for satellites, solar-system objects, and stars.

    Each target source (`tles`, `horizons`, `stars`) keeps its own cache and
    module; this program is the skeleton that orchestrates them — one update
    loop per source, shared whitelist/graylist/blacklist management, task
    generation over the camera parameter grid, and imagery publishing. State is
    persisted to the KV store: object lists carry across restarts, and a failed
    refresh falls back to the recent cached set instead of stalling tasking.
    """

    def __init__(self):
        self.task_queue: TaskQueue | None = None

        self.tles: Dict[str, Dict[str, str]] = {}
        self._tle_updater: asyncio.Task | None = None

        self.horizons: Dict[str, HorizonsObject] = {}
        self._horizons_updater: asyncio.Task | None = None

        self.stars: Dict[str, Equatorial] = {}
        self._star_updater: asyncio.Task | None = None

        self._task_generator: asyncio.Task | None = None
        self._publisher: asyncio.Task | None = None

        self.state = OttoState()
        self.list_manager: ObjectListManager | None = None

    async def _save_state(self):
        try:
            await self.program.kv_put_model(self.state)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def _watch_config(self):
        async for config in self.program.kv_monitor_model(OttoConfig):
            prev, self.config = self.config, config

            # Determine whether the new configuration has a different object list from current.
            objs_from_state = set(
                self.state.whitelist + self.state.graylist + self.state.blacklist
            )

            if set(config.task.named_objects) != objs_from_state:
                # New config. Clear our object lists and start fresh.
                self.state.whitelist = config.task.named_objects
                self.state.graylist = []
                self.state.blacklist = []
                logger.info(f"Loaded {len(self.state.whitelist)} objects from config")

            # New targeting: refetch TLEs so it takes effect immediately
            if (
                set(config.task.tles) != set(prev.task.tles)
                or set(config.task.orbits) != set(prev.task.orbits)
            ) and self._tle_updater:
                self._tle_updater.cancel()
                self._tle_updater = asyncio.create_task(self.update_tles_loop())

            # Likewise for Horizons objects
            if set(config.task.horizons) != set(prev.task.horizons) and self._horizons_updater:
                self._horizons_updater.cancel()
                self._horizons_updater = asyncio.create_task(self.update_horizons_loop())

            # ...and for stars, which resolve once rather than on a cycle
            if set(config.task.stars) != set(prev.task.stars) and self._star_updater:
                self._star_updater.cancel()
                self._star_updater = asyncio.create_task(self.update_stars())

    @sk.on_attach
    async def program_init(self):
        """Load state, set up the controller, task queue, and optionally start task generation and image publishing."""
        self.program = sk.program()

        self.config = await self.program.kv_get_model(OttoConfig)

        # Restore last known state
        try:
            self.state = await self.program.kv_get_model(OttoState)
            logger.debug(f"restored state for {self.program.entity}")
        except Exception:
            logger.warning(f"No saved state for {self.program.entity}")

        # Seed whitelist from initial config if state is empty
        if not self.state.whitelist and not self.state.graylist and not self.state.blacklist:
            self.state.whitelist = self.config.task.named_objects
            logger.info(f"Loaded {len(self.state.whitelist)} objects from initial config")

        # Watch for live config changes
        self.program.task_group.create_task(self._watch_config())

        # Initialize list manager
        self.list_manager = ObjectListManager(self.state, self._save_state)

        # Restore TLE cache — lets tasking proceed if the startup fetch fails
        # (e.g. a Spacebook outage or transient error)
        try:
            tle_cache = await self.program.kv_get_model(TLECache)
            self.tles = tle_cache.tles
            self.tles_dt = tle_cache.dt
        except Exception:
            logger.debug("no cached TLEs found, will fetch on startup")

        # Restore the Horizons cache — carries object resolutions across restarts
        # so a restart doesn't re-query JPL for names it already knows
        try:
            horizons_cache = await self.program.kv_get_model(HorizonsCache)
            self.horizons = horizons_cache.objects
        except Exception:
            logger.debug("no cached Horizons ephemerides found")

        # Restore resolved star coordinates — they never change, so this just
        # avoids re-querying the name resolver after a restart
        try:
            star_cache = await self.program.kv_get_model(StarCache)
            self.stars = star_cache.stars
        except Exception:
            logger.debug("no cached star coordinates found")

        logger.debug("starting Otto program")

        # Initialize task queue
        self.task_queue = TaskQueue(self.program)

        # Get site location before starting anything that needs it — the
        # Horizons updater asks JPL for ephemerides at the observing location
        self.controller_client = self.program.sensorkit().controller(self.config.controller)
        self._controller_location = await self.controller_client.kv_get_model(SitePosition)
        self.latitude = self._controller_location.latitude_degrees
        self.longitude = self._controller_location.longitude_degrees
        self.altitude_km = self._controller_location.altitude_km

        # Start TLE updater
        self._tle_updater = asyncio.create_task(self.update_tles_loop())

        # Start the Horizons updater
        self._horizons_updater = asyncio.create_task(self.update_horizons_loop())

        # Resolve stars once
        self._star_updater = asyncio.create_task(self.update_stars())

        # Start graylist promoter
        self._graylist_promoter = asyncio.create_task(self.promote_graylist_loop())

        # Start automated task generation
        self._task_generator = asyncio.create_task(self.generate_tasks())

        # Start data publishing (if configured)
        if self.config.publish.upload:
            self._publisher = asyncio.create_task(self.data_publisher())

    @sk.on_detach
    async def program_deinit(self):
        """Cancel background tasks."""
        logger.debug("stopping Otto program")

        # Cancel background tasks
        for task in [
            self._task_generator,
            self._publisher,
            self._tle_updater,
            self._horizons_updater,
            self._star_updater,
        ]:
            if task and not task.done():
                task.cancel()

        # Save final state
        await self._save_state()

    async def update_tles_loop(self):
        """Background task that updates TLEs on startup and every 24 hours (default)."""
        while True:
            try:
                logger.debug("updating TLEs from Spacebook")

                # Fetch TLEs from Spacebook
                self.tles, response = await tles.fetch(
                    objects=self.config.task.tles, orbits=self.config.task.orbits
                )
                if response == 200:
                    logger.debug(f"updated {len(self.tles)} TLEs from Spacebook")
                    self.tles_dt = datetime.now(UTC)
                    # Large orbit regimes can exceed the KV payload limit; a failed
                    # put is caught below and only costs the restart cache
                    tle_cache = TLECache(tles=self.tles, dt=self.tles_dt)
                    await self.program.kv_put_model(tle_cache)
                elif response == 429:
                    logger.warning("Too many requests to Spacebook, using last saved TLEs")
                else:
                    logger.warning(f"Failed to fetch TLEs from Spacebook (status: {response})")

            except Exception as e:
                logger.exception(f"Error updating TLEs: {e}")

            # Wait configured hours before next update
            await asyncio.sleep(self.config.task.tle_update_interval_hours * 60 * 60)

    async def update_horizons_loop(self):
        """Background task that updates JPL Horizons resolutions and ephemerides, on startup and periodically."""
        while True:
            interval_hours = self.config.task.horizons_update_interval_hours
            try:
                now = datetime.now(UTC)
                # An hour of slack so the cache never runs out before the next refresh
                stop = now + timedelta(hours=interval_hours + 1)
                intervals = horizons.visibility_intervals((stop - now).total_seconds())

                objects: Dict[str, HorizonsObject] = {}
                for name in self.config.task.horizons:
                    entry = self.horizons.get(name)

                    if entry is None:
                        resolved = await horizons.resolve(name)
                        if resolved is None:
                            await self._blacklist_unresolved(name)
                            continue
                        command, resolved_name = resolved
                        await self._recover_blacklisted(name)
                    else:
                        command, resolved_name = entry.command, entry.name

                    try:
                        samples = await horizons.fetch_ephemeris(
                            command=command,
                            latitude=self.latitude,
                            longitude=self.longitude,
                            altitude_km=self.altitude_km,
                            start=now,
                            stop=stop,
                            intervals=intervals,
                        )
                    except Exception as e:
                        # Keep the previous ephemeris; it may still cover the near term
                        logger.warning(f"Horizons ephemeris for {name!r} failed: {e}")
                        if entry:
                            objects[name] = entry
                        continue

                    objects[name] = HorizonsObject(
                        command=command, name=resolved_name, samples=samples
                    )

                self.horizons = objects
                if objects:
                    logger.debug(f"updated {len(objects)} Horizons ephemerides")
                    await self.program.kv_put_model(HorizonsCache(objects=objects, dt=now))

            except Exception as e:
                logger.exception(f"Error updating Horizons ephemerides: {e}")

            await asyncio.sleep(interval_hours * 60 * 60)

    async def update_stars(self):
        """Resolve configured star names to fixed ICRF coordinates."""
        resolved: Dict[str, Equatorial] = {}
        for name in self.config.task.stars:
            if coords := self.stars.get(name):
                resolved[name] = coords
                continue

            coords = await stars.resolve(name)
            if coords is None:
                await self._blacklist_unresolved(name)
                continue

            resolved[name] = coords
            await self._recover_blacklisted(name)

        self.stars = resolved
        if resolved:
            logger.debug(f"resolved {len(resolved)} stars")
            await self.program.kv_put_model(StarCache(stars=resolved, dt=datetime.now(UTC)))

    async def _blacklist_unresolved(self, name: str):
        """Blacklist a name its resolver could not resolve.

        Just this name — the resolver's warning names any candidates — so the
        rest of the list keeps tasking. `_recover_blacklisted` is the inverse.
        """
        if name in self.state.whitelist:
            await self.list_manager.move_object(name, ListType.WHITELIST, ListType.BLACKLIST)

    async def _recover_blacklisted(self, name: str):
        """Return a name to the whitelist once it finally resolves.

        A lookup failure blacklists the object, but the resolvers can't tell an
        unknown name from an unreachable service — so a name that resolves on a
        later retry was a transient outage, not a bad config, and belongs back
        in rotation.
        """
        if name in self.state.blacklist:
            logger.info(f"{name} resolved on retry, returning it to the whitelist")
            await self.list_manager.move_object(name, ListType.BLACKLIST, ListType.WHITELIST)

    async def promote_graylist_loop(self):
        """Background task that promotes graylisted objects back to whitelist."""
        graylist_interval = self.config.task.graylist_interval_minutes

        while True:
            try:
                await asyncio.sleep(graylist_interval * 60)

                if self.state.graylist:
                    logger.debug(
                        f"promoting {len(self.state.graylist)} objects from graylist to whitelist"
                    )

                    # Move all graylisted objects back to whitelist
                    for obj in self.state.graylist.copy():
                        await self.list_manager.move_object(
                            obj, ListType.GRAYLIST, ListType.WHITELIST
                        )

            except Exception as e:
                logger.exception(f"Error in graylist promoter: {e}")

    @sk.task_factory
    async def generate(self):
        """
        Program task generator.

        Pulls a task from the queue and yields it for execution.

        Yields:
            StandardCollectTask: The next task to be executed.
            None: If no task is available.
        """
        if queued := await self.task_queue.pop_task():
            task = queued.task

            # Recalculate end_time based on current time so that tasks generated
            # before the controller started operating get a fresh execution deadline.
            task.end_time = (
                datetime.now(UTC)
                + timedelta(
                    seconds=task.camera_params.integration_time_seconds
                    * task.camera_params.frame_count
                )
                + timedelta(seconds=self.config.task.end_time_deadband_seconds)
            )

            logger.info(f"Executing {queued.id} with end_time={task.end_time}")

            try:
                # `end_time` is the domain scheduling deadline; pass it through as the
                # execution `expiry_time` so the controller enforces the same deadline. Await the
                # yielded execution so completion (and any failure) surfaces here.
                await (yield task.submit(context=queued.context, expiry_time=task.end_time))
                logger.info(f"Finished executing {queued.id}")
                # Optional fixed gap after each completed task. Skipped on
                # cancellation/failure so those propagate without delay.
                if self.config.task.inter_task_delay_seconds > 0:
                    logger.info(
                        f"Waiting {self.config.task.inter_task_delay_seconds} s before next task"
                    )
                    await asyncio.sleep(self.config.task.inter_task_delay_seconds)
            except asyncio.CancelledError as e:
                logger.warning(f"{queued.id} cancelled: {e}")
                raise
            except Exception as e:
                logger.exception(f"{queued.id} failed: {e}")
                raise
        else:
            # Peek at next task to provide info
            if queued := await self.task_queue.peek_task():
                time_until = (queued.task.end_time - datetime.now(UTC)).total_seconds()
                logger.info(f"Next task ({queued.id}) available for {time_until:.1f} s")
            else:
                logger.debug("no tasks available")
            yield None

    def _object_position(self, object: str) -> tuple[float, float, bool, float] | None:
        """Current (altitude, azimuth, rising, hour_angle) for any target source.

        Returns None when the object's source data is unusable: no TLE for a
        satellite, or a cached ephemeris that doesn't cover the current time.
        """
        if entry := self.horizons.get(object):
            return horizons.position(entry.samples, longitude=self.longitude)

        if coords := self.stars.get(object):
            return stars.position(coords, latitude=self.latitude, longitude=self.longitude)

        if tle_data := self.tles.get(object):
            return tles.position(
                tle_data=tle_data,
                latitude=self.latitude,
                longitude=self.longitude,
                altitude_km=self.altitude_km,
            )

        return None

    async def _target_for(
        self, object: str, managed: bool, block_seconds: float, now: datetime
    ) -> BaseTarget | None:
        """Build the collect target for an object, dispatching on its source.

        Returns None when the object can't be targeted right now; the caller
        skips it and picks it up on a later pass.
        """
        dither = self.config.collect.dither_amount_arcsec if self.config.collect.dither else 0.0

        if entry := self.horizons.get(object):
            # One dense ephemeris shared by the object's whole task block. Only
            # the future end is padded, by the same deadband slack the tasks
            # get: execution can never precede generation, and window time that
            # will never be observed only dilutes the sampling.
            return await horizons.make_target(
                entry,
                latitude=self.latitude,
                longitude=self.longitude,
                altitude_km=self.altitude_km,
                start=now,
                stop=now
                + timedelta(
                    seconds=block_seconds + self.config.task.end_time_deadband_seconds
                ),
                dither_arcsec=dither,
            )

        if coords := self.stars.get(object):
            return stars.make_target(coords, dither_arcsec=dither)

        tle_data = self.tles.get(object)
        if tle_data is None:
            # The TLE disappeared since the visibility check
            if managed:
                logger.warning(f"Removing object {object} from the whitelist (TLE removed)")
                await self.list_manager.move_object(
                    object, ListType.WHITELIST, ListType.BLACKLIST
                )
            return None
        return tles.make_target(
            tle_data,
            dither_arcsec=dither,
            latitude=self.latitude,
            longitude=self.longitude,
            altitude_km=self.altitude_km,
        )

    async def _visible_targets(self, objects: list[str]) -> list[str]:
        """Filter objects to those above `altitude_min`, ordered for a single-flip sky walk.

        Targets are sorted by hour angle so a scan walks the sky continuously,
        crossing the meridian exactly once. `eastward` (default) emits in
        descending HA — starting at the far western horizon, walking east
        through the meridian and continuing into the east. `westward` is the
        symmetric reverse. Runs in a worker thread so a pass over a large
        orbit-regime catalog doesn't stall the event loop.
        """
        direction = self.config.collect.scan_direction or "eastward"

        def compute() -> list[tuple[str, float]]:
            candidates: list[tuple[str, float]] = []
            for obj_id in objects:
                result = self._object_position(obj_id)
                if result is None:
                    continue
                altitude, _azimuth, _rising, hour_angle = result
                if altitude < self.config.collect.altitude_min:
                    continue
                candidates.append((obj_id, hour_angle))
            return candidates

        candidates = await asyncio.to_thread(compute)

        # eastward = descending HA (+west → 0 → −east); westward = ascending.
        reverse = direction == "eastward"
        candidates.sort(key=lambda x: x[1], reverse=reverse)

        return [obj_id for obj_id, _ in candidates]

    async def generate_tasks(self):
        """
        Background task that automatically generates observation tasks.

        Implement your automated task generation logic here:
        - Reading from Spacebook
        - Parsing TLE files
        - Scheduling based on visibility windows
        - etc.
        """

        while True:
            try:
                # Wait for a usable target source. TLEs older than a day are
                # treated as absent; Horizons objects and stars refresh on their
                # own loops, so they keep tasking even if Spacebook is unreachable.
                tles_ready = bool(self.tles) and (datetime.now(UTC) - self.tles_dt).days <= 1
                if not tles_ready and not self.horizons and not self.stars:
                    logger.debug("no TLEs, Horizons ephemerides, or stars available yet")
                    await asyncio.sleep(5)
                    continue

                # Orbit-regime members come straight from the TLE fetch; they are
                # selected by current visibility, never list-managed
                known = set(self.state.whitelist + self.state.graylist + self.state.blacklist)
                orbit_members = [oid for oid in self.tles if oid not in known] if tles_ready else []
                whitelist = [
                    obj
                    for obj in self.state.whitelist
                    if tles_ready or obj in self.horizons or obj in self.stars
                ]

                if not whitelist and not orbit_members:
                    if not self.state.graylist:
                        logger.warning(
                            "No viewable objects on whitelist nor graylist. Please pick new objects"
                        )
                        await asyncio.sleep(5)
                    else:
                        graylist_interval = self.config.task.graylist_interval_minutes
                        logger.warning(
                            f"No viewable objects on whitelist but {len(self.state.graylist)} objects on graylist. Sleeping {graylist_interval} minutes"
                        )
                        await asyncio.sleep(graylist_interval * 60)
                    continue

                # Build ordered target list based on scan mode
                if self.config.collect.scan_mode:
                    targets = await self._visible_targets(whitelist + orbit_members)
                    if not targets:
                        await asyncio.sleep(5)
                        continue
                else:
                    # Whitelist objects are visibility-checked (and list-managed) after
                    # selection; orbit members are filtered to currently-visible first
                    pool = whitelist + await self._visible_targets(orbit_members)
                    if not pool:
                        await asyncio.sleep(5)
                        continue
                    # TODO: weight selection by source when more than one of
                    # task.tles, task.orbits, task.horizons, and task.stars is
                    # configured. Uniform choice over the combined pool lets a
                    # large orbit regime swamp the explicitly named objects.
                    targets = [random.choice(pool)]

                for object in targets:
                    # Orbit-regime targets aren't list-managed: on a stale check
                    # they are simply skipped until the next pass
                    managed = object in self.state.whitelist

                    # Check current altitude, azimuth, and rising status
                    result = self._object_position(object)
                    if result is None:
                        if object in self.config.task.horizons or object in self.config.task.stars:
                            # Not resolved yet, or the cached ephemeris doesn't
                            # reach the current time; that source's own updater
                            # will refresh or blacklist it — don't misfile it
                            # as a missing TLE
                            logger.debug(f"object {object} has no current position, skipping")
                        elif managed:
                            logger.warning(
                                f"Removing object {object} from the whitelist (no TLE available)"
                            )
                            await self.list_manager.move_object(
                                object,
                                ListType.WHITELIST,
                                ListType.BLACKLIST,
                            )
                        continue
                    altitude, azimuth, rising, _hour_angle = result

                    # Determine which list this object should be on
                    if altitude < self.config.collect.altitude_min and not rising:
                        if managed:
                            # Stars and solar-system objects only ever set
                            # diurnally and are back the next night, so they
                            # graylist for re-promotion; a satellite whose
                            # pass has ended blacklists.
                            destination = (
                                ListType.BLACKLIST
                                if object in self.tles
                                else ListType.GRAYLIST
                            )
                            await self.list_manager.move_object(
                                object, ListType.WHITELIST, destination
                            )
                            logger.debug(
                                f"object {object} {destination.value}ed (alt={altitude:.1f}°, az={azimuth:.1f}°, falling)"
                            )
                        continue

                    if altitude < self.config.collect.altitude_min and rising:
                        if managed:
                            await self.list_manager.move_object(
                                object, ListType.WHITELIST, ListType.GRAYLIST
                            )
                            logger.debug(
                                f"object {object} graylisted (alt={altitude:.1f}°, az={azimuth:.1f}°, rising)"
                            )
                        continue

                    # FIXME: may want a custom controller for Otto. The standard controller re-slews between each different
                    # filter, binning, and exposure (but not frame number) setting.

                    # Create tasks for each combination of camera parameters
                    # exposure_max is inclusive (when reachable from exposure_min
                    # in steps of exposure_delta).
                    exposures = list(
                        range(
                            self.config.collect.exposure_min,
                            self.config.collect.exposure_max + 1,
                            self.config.collect.exposure_delta,
                        )
                    )
                    random.shuffle(exposures)

                    filters = list(self.config.collect.filters or [None])
                    random.shuffle(filters)

                    binnings = list(self.config.collect.binning)
                    random.shuffle(binnings)

                    now = datetime.now(UTC)

                    # Every task for this object shares one target. For a Horizons
                    # object that means one ephemeris fetch per pass, sized to the
                    # block's total integration time.
                    block_seconds = (
                        sum(exposures)
                        * len(filters)
                        * len(binnings)
                        * self.config.collect.num_frames
                    )
                    target = await self._target_for(object, managed, block_seconds, now)
                    if target is None:
                        continue

                    # One correlation id per visit to this target: every filter/exposure/binning
                    # task below shares it, so downstream can regroup the whole visit.
                    visit_id = str(uuid.uuid4())

                    # The collect's target_id is the operator's object identifier, passed
                    # through on the task (whitespace-collapsed so it stays filename/FITS-card
                    # safe). Ephemeris and star targets carry no intrinsic id, so this is their
                    # only source of one; for a TLE it just matches the configured id.
                    target_id = "_".join(object.split())

                    # Map track_mode onto per-frame sidereal switches
                    frames = sidereal_frames(
                        self.config.collect.track_mode, self.config.collect.num_frames
                    )

                    cumulative_exposure = 0
                    for filter in filters:
                        for exposure in exposures:
                            for binning in binnings:
                                cumulative_exposure += exposure * self.config.collect.num_frames
                                task = StandardCollectTask(
                                    target=target,
                                    target_id=target_id,
                                    end_time=(
                                        now
                                        + timedelta(seconds=cumulative_exposure)
                                        + timedelta(
                                            seconds=self.config.task.end_time_deadband_seconds
                                        )
                                    ),
                                    camera_params=CameraParameterSet(
                                        integration_time_seconds=exposure,
                                        frame_count=self.config.collect.num_frames,
                                        filter_name=filter,
                                        readout_mode=self.config.collect.readout_mode,
                                        gain=self.config.collect.gain,
                                        binning_x=binning,
                                        binning_y=binning,
                                    ),
                                    sidereal_frames=frames,
                                )
                                await self.task_queue.push_task(
                                    task, context=sk.KeywordDict(OttoContext(visit_id=visit_id))
                                )

                    # Wait for this target's tasks to drain before generating
                    # tasks for the next target.  This prevents far-future tasks
                    # from expiring while the controller is still working through
                    # earlier targets in the scan list.
                    while await self.task_queue.peek_task():
                        await asyncio.sleep(1)

            except Exception as e:
                logger.exception(f"Error in task generator: {e}")

            finally:
                while await self.task_queue.peek_task():
                    await asyncio.sleep(1)

    async def data_publisher(self):
        logger.debug("data publisher started")

        # Initialize configured publishers
        publishers = []
        cfg = self.config.publish
        if cfg.gdrive:
            from sensorkit.otto.publishers import GDrivePublisher

            publishers.append(GDrivePublisher(cfg.gdrive, env_file=cfg.env_file))
        if cfg.dropbox:
            from sensorkit.otto.publishers import DropboxPublisher

            publishers.append(DropboxPublisher(cfg.dropbox, env_file=cfg.env_file))
        if cfg.udl:
            from sensorkit.otto.publishers import UDLPublisher

            publishers.append(
                UDLPublisher(
                    cfg.udl,
                    env_file=cfg.env_file,
                    frame_count=self.config.collect.num_frames,
                    site=self._controller_location,
                )
            )

        if not publishers:
            logger.warning("publish.upload is true but no destinations configured")
            return

        logger.info(f"publishing to {len(publishers)} destination(s)")

        try:
            if graph := await self.program.data_graph():
                sink = graph.app_sink()
                async for context, data in sink.consume():
                    task_id = context.get("task_id")
                    for pub in publishers:
                        try:
                            await pub.publish(context, data)
                        except Exception as e:
                            logger.exception(f"Error publishing task {task_id} to {pub.name}: {e}")
        finally:
            for pub in publishers:
                await pub.close()


@sk.service_entrypoint(version=sk.VERSION)
async def otto_service(service: sk.Service):
    await service.register()

    service.include(OttoProgram())
    await service.run()
