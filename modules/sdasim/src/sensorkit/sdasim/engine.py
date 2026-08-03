# SPDX-License-Identifier: Apache-2.0
"""sdasim rendering engine.

`SdasimEngine` wraps an sdasim `Scene`. Every frame is rendered for the pointing,
mount rate and time it is given: satellites are propagated to that time, and the
stars are those the catalog holds at that pointing. The Scene caches the parsed
catalog between frames, and is rebuilt only when the exposure changes.

`sdasim` and `torch` are imported lazily inside the methods that need them so
this module (and the rest of SensorKit) can be imported without the optional
`sdasim` extra installed.
"""

from __future__ import annotations

import copy
import math
from pathlib import Path

import numpy as np
from loguru import logger


class SdasimEngine:
    """Reusable sdasim `Scene` wrapper for the camera device.

    Each render gets its own pointing, mount rate and time, so every frame shows
    the satellites where they are at that moment and the stars at that pointing.
    The Scene (and its satellite catalog, when enabled) is cached between frames
    and rebuilt only when the exposure changes.
    """

    def __init__(
        self,
        sdasim_config_path: str,
        device: str = "cpu",
    ):
        self._sdasim_config_path = sdasim_config_path
        self._device = device
        self._base_config = None
        self._scene = None
        self._scene_exposure: float | None = None
        self._warned_no_optics = False

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
    def optics_enabled(self) -> bool:
        """Whether the scene config enables sdasim's geometric defocus model.

        getattr-guarded so the bridge also runs against sdasim versions that
        predate the optics model (no `optics` field in SceneConfig).
        """
        optics = getattr(self._base_config, "optics", None) if self._base_config else None
        return bool(optics is not None and getattr(optics, "enabled", False))

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
        defocus_um: float | None = None,
    ) -> tuple[np.ndarray, dict]:
        """Render one frame at the given pointing, mount rate, and time.

        Satellites are propagated to `obs_time` and the stars are read from the
        catalog at `point_ra`/`point_dec`, so each frame reflects that moment and
        that pointing. sdasim's `apparent_rate = object_rate - mount_rate` model
        handles sidereal vs rate track from the mount rate alone: (0, 0) ==
        sidereal (sharp stars, streaking satellites); nonzero == rate track (the
        tracked object becomes a point, stars streak).

        For CCD sensors (`is_cmos=False`) with binning > 1, read noise is
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
            defocus_um: Commanded focal shift from best focus (microns), from
                the focuser telemetry. Forwarded to sdasim's optics model when
                the scene enables it; ignored (with a one-time warning) when
                the scene has no optics model to consume it.

        Returns:
            `(image, metadata)` -- a 2D `np.uint16` array and sdasim's render
            metadata (`num_targets`, `target_positions`,
            `target_velocities`, ...), useful as truth labels.
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
        if defocus_um is not None:
            if self.optics_enabled:
                overrides["defocus_um"] = defocus_um
            elif not self._warned_no_optics:
                self._warned_no_optics = True
                logger.warning(
                    "focuser telemetry provides defocus but the sdasim scene config "
                    "has no enabled `optics:` section (or sdasim predates it) -- "
                    "rendering stays in focus"
                )

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
        """(Re)build the Scene on first use or when the exposure changes.

        Pointing needs no rebuild: sdasim reads the stars from the catalog at the
        pointing it is given on each render. The exposure scales the signal the
        Scene is built with, so a change there does need one. `point_ra`/`point_dec`
        seed the build so the first catalog read lands near the working region.
        """
        if self._scene is not None and self._scene_exposure == exposure:
            return

        import sdasim

        cfg = copy.deepcopy(self._base_config)
        cfg.sensor.exposure = exposure
        cfg.sensor.num_frames = 1
        cfg.stars.ra = point_ra
        cfg.stars.dec = point_dec
        cfg.seed = None

        self._scene = sdasim.Scene(cfg)
        self._scene_exposure = exposure
        logger.debug(
            f"(re)built sdasim Scene at RA={point_ra:.4f} Dec={point_dec:.4f} "
            f"exposure={exposure:.2f}s (pointing tracks per-frame via render override)"
        )

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
