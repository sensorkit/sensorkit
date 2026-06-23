"""sdasim rendering engine.

``SdasimEngine`` wraps a reusable sdasim ``Scene``: it is built once (parsing the
satellite catalog, if enabled) and reused across exposures, with pointing, mount
rate, and observation time passed as per-frame ``render()`` overrides. The Scene
is rebuilt only when the pointing drifts past a threshold or the exposure
changes (the star field and exposure are baked in at construction).

``sdasim`` and ``torch`` are imported lazily inside the methods that need them so
this module (and the rest of SensorKit) can be imported without the optional
``sdasim`` extra installed.
"""

from __future__ import annotations

import copy
import math
from pathlib import Path

import numpy as np
from loguru import logger


class SdasimEngine:
    """Reusable sdasim ``Scene`` wrapper for the camera device.

    The ``Scene`` (and its satellite catalog, when enabled) is built once and
    reused; pointing, mount rate, and time are per-frame ``render()`` overrides.
    The Scene is rebuilt only when the pointing center drifts past
    ``rebuild_threshold_deg`` or the exposure changes -- the star field and
    exposure are fixed at construction.
    """

    def __init__(
        self,
        sdasim_config_path: str,
        device: str = "cpu",
        rebuild_threshold_deg: float = 0.25,
    ):
        self._sdasim_config_path = sdasim_config_path
        self._device = device
        self._rebuild_threshold_deg = rebuild_threshold_deg
        self._base_config = None
        self._scene = None
        self._scene_ra: float | None = None
        self._scene_dec: float | None = None
        self._scene_exposure: float | None = None

    def initialize(self) -> None:
        """Load the sdasim scene config (the Scene itself is built lazily).

        Raises:
            FileNotFoundError: If the configured sdasim scene YAML does not exist.
        """
        path = Path(self._sdasim_config_path)
        if not path.exists():
            raise FileNotFoundError(f"sdasim config not found: {self._sdasim_config_path}")

        import sdasim

        self._base_config = sdasim.load_config(path)
        self._base_config.device = self._device
        catalog_on = bool(getattr(self._base_config.catalog, "enabled", False))
        logger.debug(
            f"Loaded sdasim config from {path}: "
            f"{self.sensor_width}x{self.sensor_height} "
            f"(device={self._device}, catalog={'on' if catalog_on else 'off'})"
        )

    @property
    def initialized(self) -> bool:
        return self._base_config is not None

    @property
    def catalog_enabled(self) -> bool:
        return bool(self._base_config and getattr(self._base_config.catalog, "enabled", False))

    @property
    def default_point(self) -> tuple[float, float]:
        """Scene-configured star-field center (RA, Dec in deg) -- used as a
        fallback pointing when no live mount telemetry is available."""
        if self._base_config is None:
            return (0.0, 0.0)
        return (self._base_config.stars.ra, self._base_config.stars.dec)

    @property
    def sensor_width(self) -> int:
        return self._base_config.sensor.width if self._base_config else 0

    @property
    def sensor_height(self) -> int:
        return self._base_config.sensor.height if self._base_config else 0

    @property
    def sensor_config(self):
        return self._base_config.sensor if self._base_config else None

    def render_frame(
        self,
        exposure_duration: float,
        point_ra: float,
        point_dec: float,
        mount_ra_rate: float = 0.0,
        mount_dec_rate: float = 0.0,
        obs_time: str | None = None,
        bin_factor: int = 1,
    ) -> tuple[np.ndarray, dict]:
        """Render one frame at the given pointing, mount rate, and time.

        Pointing, mount rate, and time are passed as ``render()`` overrides, so a
        sequence of exposures at (roughly) the same pointing reuses the same Scene
        and its already-parsed catalog. sdasim's ``apparent_rate = object_rate -
        mount_rate`` model handles sidereal vs rate track from the mount rate
        alone: (0, 0) == sidereal (sharp stars, streaking satellites); nonzero ==
        rate track (the tracked object becomes a point, stars streak).

        For CCD sensors (``is_cmos=False``) with binning > 1, read noise is
        deferred so charge is summed on-chip before one read-noise + A/D pass per
        superpixel; CMOS sensors apply read noise per pixel before binning.

        Args:
            exposure_duration: Exposure time in seconds.
            point_ra: Pointing-center right ascension (degrees).
            point_dec: Pointing-center declination (degrees).
            mount_ra_rate: Inertial RA rate of the pointing (deg/s); 0 == sidereal.
            mount_dec_rate: Inertial Dec rate of the pointing (deg/s).
            obs_time: ISO-8601 UTC observation time (defaults to the scene's).
            bin_factor: Symmetric NxN binning factor.

        Returns:
            ``(image, metadata)`` -- a 2D ``np.uint16`` array and sdasim's render
            metadata (``num_targets``, ``target_positions``,
            ``target_velocities``, ...), useful as truth labels.
        """
        if self._base_config is None:
            raise RuntimeError("sdasim engine not initialized")

        self._ensure_scene(point_ra, point_dec, exposure_duration)

        sensor = self._base_config.sensor
        defer = (not sensor.is_cmos) and bin_factor > 1

        overrides = dict(
            point_ra=point_ra,
            point_dec=point_dec,
            mount_ra_rate=mount_ra_rate,
            mount_dec_rate=mount_dec_rate,
            defer_read_noise=defer,
        )
        if obs_time is not None:
            overrides["obs_time"] = obs_time

        result, meta = self._scene.render(0, **overrides)
        image = result.detach().cpu()

        if defer:
            # CCD path: result is analog signal in PE (post shot noise). Bin the
            # charge first, then apply read noise + A/D once per superpixel.
            from sdasim.fpa import analog_to_digital
            from sdasim.noise import gaussian_noise

            image = self._bin_tensor(image, bin_factor)
            rn_sigma = math.sqrt(sensor.read_noise**2 + sensor.electronic_noise**2)
            if rn_sigma > 0:
                image = gaussian_noise(image, rn_sigma)
            image = analog_to_digital(
                image, sensor.gain, sensor.fwc, sensor.a2d_bias, sensor.a2d_dtype
            )
            return self._to_uint16(image.numpy()), meta

        # CMOS / unbinned path: render already produced a digital image.
        array = self._to_uint16(image.numpy())
        if bin_factor > 1:
            array = self.apply_binning(array, bin_factor)
        return array, meta

    def _ensure_scene(self, point_ra: float, point_dec: float, exposure: float) -> None:
        """(Re)build the Scene on first use, on an exposure change, or when the
        pointing drifts past the rebuild threshold.

        The star field and exposure are baked into the Scene at construction (and
        the satellite catalog is parsed there once), so those changes require a
        rebuild; pointing/rate/time within the threshold are cheap per-frame
        overrides.
        """
        if (
            self._scene is not None
            and self._scene_exposure == exposure
            and self._angular_sep_deg(point_ra, point_dec, self._scene_ra, self._scene_dec)
            <= self._rebuild_threshold_deg
        ):
            return

        import sdasim

        cfg = copy.deepcopy(self._base_config)
        cfg.sensor.exposure = exposure
        cfg.sensor.num_frames = 1
        cfg.stars.ra = point_ra
        cfg.stars.dec = point_dec
        cfg.seed = None

        self._scene = sdasim.Scene(cfg)
        self._scene_ra = point_ra
        self._scene_dec = point_dec
        self._scene_exposure = exposure
        logger.debug(
            f"(re)built sdasim Scene at RA={point_ra:.4f} Dec={point_dec:.4f} "
            f"exposure={exposure:.2f}s"
        )

    @staticmethod
    def _angular_sep_deg(ra1: float, dec1: float, ra2: float | None, dec2: float | None) -> float:
        """Great-circle separation (deg) between two RA/Dec points."""
        if ra2 is None or dec2 is None:
            return float("inf")
        r1, d1, r2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
        cos_sep = math.sin(d1) * math.sin(d2) + math.cos(d1) * math.cos(d2) * math.cos(r1 - r2)
        return math.degrees(math.acos(max(-1.0, min(1.0, cos_sep))))

    @staticmethod
    def _bin_tensor(image, bin_factor: int):
        """Bin a 2D torch tensor by summing NxN blocks."""
        h, w = image.shape
        h_trim = (h // bin_factor) * bin_factor
        w_trim = (w // bin_factor) * bin_factor
        trimmed = image[:h_trim, :w_trim]
        return trimmed.reshape(
            h_trim // bin_factor, bin_factor, w_trim // bin_factor, bin_factor
        ).sum(dim=(1, 3))

    @staticmethod
    def apply_binning(image: np.ndarray, bin_factor: int) -> np.ndarray:
        """Bin a rendered (digital) image by summing NxN pixel blocks (CMOS path)."""
        if bin_factor <= 1:
            return image

        h, w = image.shape
        h_trim = (h // bin_factor) * bin_factor
        w_trim = (w // bin_factor) * bin_factor
        trimmed = image[:h_trim, :w_trim].astype(np.uint32)

        binned = trimmed.reshape(
            h_trim // bin_factor, bin_factor, w_trim // bin_factor, bin_factor
        ).sum(axis=(1, 3))

        return np.clip(binned, 0, 65535).astype(np.uint16)

    @staticmethod
    def _to_uint16(array: np.ndarray) -> np.ndarray:
        return np.clip(array, 0, 65535).astype(np.uint16)
