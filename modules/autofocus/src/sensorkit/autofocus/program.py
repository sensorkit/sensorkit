from __future__ import annotations

import asyncio
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.observer import EarthObserver
from sensorkit.astro.target import ICRSTarget
from sensorkit.autofocus.calibrate import FocusCalibrateTask  # TEMPORARY: see calibrate.py
from sensorkit.autofocus.models import AutofocusConfig
from sensorkit.autofocus.task_queue import TaskQueue
from sensorkit.common.time import parse_spec
from sensorkit.std.optics import FocusPosition

if TYPE_CHECKING:
    from sensorkit.autofocus.analyzer import AutofocusAnalyzer


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
        """
        Program task generator.

        Pulls a task from the queue and yields it for execution.

        Yields:
            FocusCalibrateTask: The next task to be executed.
            None: If no task is available.
        """
        if task := await self.task_queue.pop_task():
            logger.info(
                f"task ({task.task_id}): "
                f"range=[{task.start_position:.0f}, {task.end_position:.0f}] "
                f"num_steps={task.num_steps}"
            )

            try:
                yield task
                logger.info(f"Task ({task.task_id}) completed")
            except asyncio.CancelledError:
                logger.warning(f"Task ({task.task_id}) cancelled")
            except Exception as e:
                logger.exception(f"Task ({task.task_id}) failed: {e}")
            finally:
                await self.task_queue.update_offers()
        else:
            yield None

    async def _vcurve_scheduler(self):
        """Schedules V-curve calibrations based on user-configured time(s)."""

        while True:
            try:
                next_time = self._compute_next_vcurve_time()
                if next_time is None:
                    logger.warning("Could not compute next V-curve time; retrying in 1h")
                    await asyncio.sleep(3600)
                    continue

                now = datetime.now(UTC)
                delay = (next_time - now).total_seconds()

                if delay > 0:
                    logger.info(f"Next V-curve scheduled at {next_time.replace(tzinfo=None).isoformat()} ({delay / 3600:.1f}h from now)")
                    await asyncio.sleep(delay)

                # Queue the V-curve
                await self.queue_vcurve()

                # Wait a bit before computing the next one
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Unable to schedule V-curve: {e}")
                raise

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

    async def queue_vcurve(self, target: ICRSTarget | None = None):
        """Queue a V-curve calibration task.

        Args:
            target: Optional target to slew to. If None, auto-selects a bright star.
        """

        # Auto-select target if not provided
        if target is None and self._site_position is not None:
            target = _select_vcurve_target(
                self.config.min_altitude,
                self._site_position.latitude_degrees,
                self._site_position.longitude_degrees,
            )

        # Get current focus position to center the sweep
        center_position = (self.config.focuser.min_position + self.config.focuser.max_position) / 2.0
        try:
            focuser = self.analyzer.sk_client.device(self.config.focuser.entity)
            fp = await focuser.get(FocusPosition)
            if fp is not None:
                center_position = fp.position
        except Exception:
            logger.debug("Could not get current focus position; using midpoint")

        # Compute sweep range
        half_range = (self.config.focuser.max_position - self.config.focuser.min_position) / 4.0
        start = max(center_position - half_range, self.config.focuser.min_position)
        end = min(center_position + half_range, self.config.focuser.max_position)

        # Estimate task duration
        est_seconds = self.config.vcurve.num_steps * (self.config.vcurve.exposure_time + 5.0) + 120.0

        task = FocusCalibrateTask(
            task_id=uuid.uuid1(),
            controller_id=str(self.config.controller),
            target=target,
            start_position=start,
            end_position=end,
            num_steps=self.config.vcurve.num_steps,
            exposure_time=self.config.vcurve.exposure_time,
            filter_name=self.config.vcurve.filter_name,
            binning=self.config.vcurve.binning,
            end_time=datetime.now(UTC) + timedelta(seconds=est_seconds),
        )

        await self.task_queue.push_task(task)
        logger.info(f"Queued V-curve task {task.task_id}: [{start:.0f}, {end:.0f}] in {self.config.vcurve.num_steps} steps")


def _select_vcurve_target(
    min_altitude: float,
    site_lat: float,
    site_lon: float,
) -> ICRSTarget | None:
    """Select a bright star near the Galactic plane above min_altitude.

    Uses an embedded catalog of bright stars from Hipparcos.

    Args:
        min_altitude: Minimum altitude in degrees.
        site_lat: Site latitude in degrees.
        site_lon: Site longitude in degrees.

    Returns:
        ICRSTarget for the selected star, or None.
    """
    try:
        import astropy.units as u
        from astropy.coordinates import AltAz, EarthLocation, Galactic, SkyCoord
        from astropy.time import Time
    except ImportError:
        logger.warning("astropy not available for target selection")
        return None

    now = Time.now()
    location = EarthLocation(lat=site_lat * u.deg, lon=site_lon * u.deg)
    altaz_frame = AltAz(obstime=now, location=location)

    candidates = []
    for ra, dec, mag, name in _BRIGHT_STARS:
        coord = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame="icrs")

        # Check altitude
        altaz = coord.transform_to(altaz_frame)
        if altaz.alt.deg < min_altitude:
            continue

        # Magnitude filter: bright enough for reliable PSF, not too bright (saturation)
        if mag < 2.0 or mag > 6.0:
            continue

        # Galactic latitude (prefer stars near the plane for more background stars)
        gal = coord.transform_to(Galactic())
        gal_b = abs(gal.b.deg)

        candidates.append((ra, dec, mag, gal_b, name))

    if not candidates:
        logger.warning("No suitable V-curve targets found above minimum altitude")
        return None

    # Sort by Galactic latitude (closest to plane first)
    candidates.sort(key=lambda c: c[3])

    # Pick randomly from top 10
    top = candidates[:10]
    chosen = random.choice(top)
    ra, dec, mag, gal_b, name = chosen

    logger.info(f"Selected V-curve target: {name} (mag={mag:.1f}, |b|={gal_b:.1f}°, RA={ra:.2f}°, Dec={dec:.2f}°)")

    return ICRSTarget(coords=Equatorial(ra=ra, dec=dec))


# Embedded catalog of ~200 bright stars (Hipparcos).
# Format: (RA_deg, Dec_deg, V_mag, name)
_BRIGHT_STARS = [
    (6.752, -16.716, 1.46, "Sirius"),
    (88.793, 7.407, 0.50, "Betelgeuse"),
    (101.287, -16.716, 1.50, "Adhara"),
    (95.988, -52.696, -0.72, "Canopus"),
    (219.902, -60.834, -0.04, "Alpha Centauri"),
    (213.915, 19.182, -0.05, "Arcturus"),
    (78.634, 45.998, 0.08, "Capella"),
    (279.235, 38.784, 0.03, "Vega"),
    (310.358, 45.280, 1.25, "Deneb"),
    (297.696, 8.868, 0.77, "Altair"),
    (24.429, -57.237, 0.46, "Achernar"),
    (68.980, 16.510, 0.87, "Aldebaran"),
    (152.093, 11.967, 1.35, "Regulus"),
    (116.329, 28.026, 1.58, "Pollux"),
    (344.413, -29.622, 1.16, "Fomalhaut"),
    (263.402, -37.104, 1.63, "Shaula"),
    (186.650, -63.099, 0.61, "Mimosa"),
    (191.930, -59.689, 1.25, "Gacrux"),
    (187.792, -57.113, 1.33, "Acrux A"),
    (201.298, -11.161, 0.98, "Spica"),
    (104.656, -28.972, 1.50, "Wezen"),
    (247.352, -26.432, 0.96, "Antares"),
    (83.002, -0.300, 1.69, "Bellatrix"),
    (81.283, -1.943, 1.70, "Alnilam"),
    (114.827, 5.225, 1.98, "Procyon"),
    (49.874, -10.330, 2.00, "Mira"),
    (206.885, 49.313, 1.77, "Alioth"),
    (193.507, 55.960, 1.81, "Dubhe"),
    (165.932, 61.751, 1.79, "Merak"),
    (210.956, 36.390, 2.06, "Alkaid"),
    (283.816, -26.296, 1.85, "Kaus Australis"),
    (264.330, -43.002, 1.86, "Sargas"),
    (252.166, -69.028, 2.06, "Atria"),
    (125.629, -59.510, 1.67, "Avior"),
    (138.300, -69.717, 1.86, "Miaplacidus"),
    (107.098, -26.393, 1.84, "Mirzam"),
    (30.975, 42.330, 2.06, "Mirach"),
    (28.599, 20.808, 2.00, "Hamal"),
    (37.955, 89.264, 2.02, "Polaris"),
    (14.177, 60.717, 2.23, "Schedar"),
    (9.243, 59.150, 2.27, "Caph"),
    (2.097, 29.091, 2.06, "Alpheratz"),
    (346.190, 15.205, 2.49, "Enif"),
    (340.751, -46.885, 1.74, "Alnair"),
    (326.046, -16.127, 2.87, "Sadalsuud"),
    (322.165, -0.320, 2.90, "Sadalmelik"),
    (305.557, -14.782, 2.87, "Dabih"),
    (300.199, -72.911, 2.82, "Peacock"),
    (286.353, 13.864, 2.72, "Tarazed"),
    (275.249, -29.828, 2.60, "Nunki"),
    (269.152, -37.296, 1.62, "Kaus Media"),
    (266.417, -34.384, 2.70, "Kaus Borealis"),
    (255.072, -22.622, 2.29, "Dschubba"),
    (253.084, -38.047, 2.69, "Epsilon Sco"),
    (241.359, -19.806, 2.56, "Pi Sco"),
    (239.713, -26.114, 2.31, "Acrab"),
    (234.256, -41.167, 2.55, "Epsilon Lup"),
    (233.672, -47.388, 2.30, "Alpha Lup"),
    (229.252, -9.383, 2.61, "Zubeneschamali"),
    (222.677, -16.042, 2.75, "Zubenelgenubi"),
    (217.957, -45.111, 2.06, "Menkent"),
    (211.671, -36.370, 2.55, "Muhlifain"),
    (204.972, -53.466, 2.17, "Epsilon Cen"),
    (200.149, -36.712, 2.06, "Zeta Cen"),
    (190.379, -48.960, 2.20, "Delta Cru"),
    (183.786, -58.749, 1.63, "Epsilon Cru"),
    (176.403, -18.299, 2.74, "Algorab"),
    (175.942, -14.846, 2.59, "Gienah"),
    (173.690, -63.020, 2.76, "Lambda Cen"),
    (167.147, -58.975, 2.29, "Iota Car"),
    (161.692, -49.420, 2.25, "Aspidiske"),
    (154.993, -61.332, 2.76, "Beta Vol"),
    (146.312, -65.072, 2.21, "Alsephina"),
    (139.273, -59.275, 2.47, "Turais"),
    (136.999, -43.432, 1.69, "Naos"),
    (131.176, -54.709, 2.21, "Markab Vel"),
    (128.868, 27.798, 2.98, "Theta Gem"),
    (122.383, -47.337, 1.78, "Regor"),
    (120.896, -40.003, 2.50, "Mu Vel"),
    (119.195, -52.982, 2.44, "Koo She"),
    (113.650, 31.889, 1.14, "Castor"),
    (109.286, -37.097, 2.45, "Pi Pup"),
    (105.430, -23.834, 1.83, "Aludra"),
    (100.983, -36.733, 2.93, "Sigma Pup"),
    (99.428, -43.196, 2.25, "Tau Pup"),
    (97.204, -7.033, 2.98, "Furud"),
    (95.675, 22.514, 1.93, "Alhena"),
    (93.720, -6.275, 2.58, "Arneb"),
    (89.930, -34.074, 2.46, "Phaet"),
    (87.740, -9.670, 2.06, "Cursa"),
    (86.939, -14.822, 2.78, "Nihal"),
    (84.053, -1.202, 1.69, "Mintaka"),
    (81.573, 28.608, 1.65, "Elnath"),
    (80.702, -6.844, 2.77, "Hatsya"),
    (76.365, -22.371, 2.97, "Beid"),
    (73.563, 2.441, 3.39, "Meissa"),
    (67.154, 15.871, 3.53, "Ain"),
    (66.009, -34.017, 2.97, "Angetenar"),
    (63.500, -42.294, 2.84, "Acamar"),
    (59.508, 40.955, 1.80, "Algol"),
    (56.871, 24.105, 2.87, "Atlas"),
    (56.457, 24.367, 3.70, "Pleione"),
    (51.081, 49.861, 1.79, "Mirfak"),
    (47.042, 40.956, 2.12, "Algol"),
    (42.674, 55.896, 2.84, "Delta Per"),
    (39.871, -11.872, 2.54, "Zibal"),
    (35.437, -40.305, 2.39, "Ankaa"),
    (31.793, 23.462, 2.00, "Sheratan"),
    (26.348, -15.939, 2.04, "Diphda"),
    (21.006, -8.184, 3.47, "Iota Cet"),
    (17.096, 35.621, 2.06, "Almach"),
    (10.127, 56.537, 2.15, "Ruchbah"),
    (5.438, -42.306, 2.40, "Fomalhaut B"),
    (3.309, 15.184, 2.83, "Delta And"),
    (350.743, -20.101, 2.94, "Skat"),
    (348.974, -9.088, 3.27, "Lambda Aqr"),
    (345.944, 28.082, 2.39, "Markab"),
    (342.420, -51.317, 1.94, "Al Na'ir"),
    (332.058, -32.988, 3.00, "Iota Psa"),
    (330.723, 30.221, 2.49, "Algenib"),
    (328.482, -37.365, 2.10, "Gruis A"),
    (319.645, 62.586, 2.44, "Alderamin"),
    (316.233, 38.045, 2.20, "Sadr"),
    (311.553, -25.271, 2.89, "Sham"),
    (306.412, -56.735, 1.87, "Peacock"),
    (290.418, -40.616, 1.83, "Shaula"),
    (288.139, 67.661, 3.17, "Eta Dra"),
    (276.993, -25.422, 2.72, "Ascella"),
    (271.452, 2.933, 3.85, "Theta Oph"),
    (268.382, 12.560, 2.08, "Rasalhague"),
    (265.868, -39.030, 2.95, "Zeta Sco"),
    (262.691, -37.296, 2.41, "Epsilon Sgr"),
    (258.662, 14.390, 2.43, "Cebalrai"),
    (257.595, -15.725, 2.54, "Yed Prior"),
    (249.290, -10.567, 2.56, "Yed Posterior"),
    (248.971, -28.216, 2.29, "Graffias"),
    (245.998, 61.514, 2.74, "Pherkad"),
    (243.586, 26.714, 2.22, "Alphecca"),
    (236.067, 6.426, 2.65, "Unukalhai"),
    (233.232, -41.167, 2.68, "Zeta Lup"),
    (226.018, -25.282, 2.75, "Sigma Lib"),
    (222.720, -47.288, 2.55, "Eta Cen"),
    (220.482, -60.373, 2.06, "Beta Cen"),
    (218.019, -42.158, 2.06, "Theta Cen"),
    (215.145, 72.732, 3.07, "Yildun"),
    (211.097, 27.074, 2.68, "Muphrid"),
    (208.671, 18.398, 2.06, "Izar"),
    (195.544, 10.959, 2.83, "Vindemiatrix"),
    (188.597, -16.196, 2.94, "Iota Vir"),
    (184.976, -0.668, 3.38, "Zaniah"),
    (182.090, -50.722, 1.92, "Delta Cen"),
    (177.266, 14.572, 2.14, "Denebola"),
    (174.170, -9.802, 3.02, "Minelauva"),
    (169.620, 33.094, 2.44, "Zosma"),
    (166.453, -55.992, 2.20, "Lambda Vel"),
    (162.406, -16.194, 3.11, "Alpha Crt"),
    (156.523, 41.499, 3.44, "23 UMa"),
    (148.191, 26.007, 2.61, "Algieba"),
    (141.897, -8.659, 2.97, "Alphard"),
    (137.740, -58.970, 2.46, "Markeb"),
    (134.801, -43.000, 2.23, "Kappa Vel"),
    (130.806, 21.469, 3.52, "Adhafera"),
    (127.566, -26.803, 2.93, "Sigma Pup"),
    (121.886, -24.304, 1.50, "Naos"),
    (117.324, -24.860, 2.81, "Omicron Pup"),
    (112.050, -43.302, 2.50, "Suhail"),
    (110.031, 21.982, 3.06, "Tejat"),
    (108.139, -26.353, 1.98, "Wezen"),
    (102.460, -32.509, 3.02, "Omicron CMa"),
    (100.246, 9.896, 3.06, "Gomeisa"),
    (98.225, 7.353, 3.17, "Mirzam B"),
    (96.699, -22.964, 2.45, "Muliphein"),
    (92.442, 6.961, 3.35, "Mu Gem"),
    (90.980, 20.276, 3.31, "Propus"),
    (85.190, -1.943, 1.64, "Alnitak"),
    (83.858, -5.910, 2.09, "Saiph"),
    (82.061, 6.350, 3.69, "Meissa A"),
    (79.172, 45.998, 0.91, "Menkalinan"),
    (75.492, 43.823, 3.03, "Hassaleh"),
    (74.249, 33.166, 2.64, "Elnath B"),
    (72.460, 6.961, 3.42, "Lambda Ori"),
    (70.561, 22.957, 3.40, "17 Tau"),
    (69.080, 41.234, 2.88, "Epsilon Per"),
    (64.949, 15.628, 3.54, "Delta Tau"),
    (62.165, 47.788, 2.85, "Zeta Per"),
    (60.789, -61.570, 2.86, "Gamma Ret"),
    (58.533, 31.884, 2.85, "Atik"),
    (54.274, -9.763, 3.73, "Tau 4 Eri"),
    (50.689, 56.537, 2.89, "Epsilon Per"),
    (46.199, 53.506, 2.83, "Gamma Per"),
    (44.565, -40.305, 2.40, "Alpha Phe"),
    (40.825, 3.236, 3.73, "Xi Ari"),
    (34.836, -51.067, 3.32, "Epsilon Phe"),
    (33.985, 42.330, 2.09, "Mirach"),
    (29.692, 63.670, 3.37, "Eta Cas"),
    (23.063, 15.346, 2.64, "Hamal A"),
    (19.847, 42.330, 2.06, "Almaak"),
    (15.736, -45.747, 2.40, "Ankaa A"),
    (11.884, 54.988, 3.44, "Achird"),
    (8.591, 15.184, 3.27, "Delta And B"),
    (4.857, -42.306, 2.39, "Diphda B"),
    (1.163, 29.091, 2.07, "Alpheratz A"),
    (355.512, 77.632, 2.08, "Errai"),
    (353.243, -37.819, 2.10, "Fomalhaut A"),
    (349.291, 8.680, 2.39, "Scheat"),
    (346.190, 15.205, 2.50, "Enif A"),
]
