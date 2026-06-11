from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import numpy as np
from astropy.io import fits
from loguru import logger

import sensorkit.api as sk
from sensorkit.autofocus.models import (
    AutofocusConfig,
    AutofocusState,
    AutofocusStatus,
)
from sensorkit.autofocus.pipeline import (
    compute_defocus_sign,
    fit_vcurve,
)
from sensorkit.autofocus.server import start_vcurve_server
from sensorkit.senpai.models import SenpaiResult
from sensorkit.std.optics import ChangeFocusPosition

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit


@sk.declare_entity
class AutofocusAnalyzer:
    """Applies focus corrections based on SENPAI statistics."""

    def __init__(self, config: AutofocusConfig, sk_client: SensorKit):
        self.config = config
        self.sk_client = sk_client

        # Internal tracker for calibration frames processed by SENPAI
        self._vcurve_frames: dict[str, list[tuple[float, float]]] = {}
        self._senpai_queue: asyncio.Queue[SenpaiResult] = asyncio.Queue()

        self._entity = None
        self._tasks: list[asyncio.Task] = []
        self._program = None

        self.state = AutofocusState()

    @sk.on_attach
    async def entity_init(self):
        """Subscribe to `senpai`, start processor and server."""

        self._entity = sk.entity()
        logger.info(f"Starting AutofocusAnalyzer for {self.config.entity}")

        # Subscribe to SENPAI results and process them
        self._tasks.append(asyncio.create_task(self._subscribe_senpai()))
        self._tasks.append(asyncio.create_task(self._processor_loop()))

        # Start V-curve server
        self._tasks.append(
            asyncio.create_task(
                start_vcurve_server(
                    self.config.server.host,
                    self.config.server.port,
                    self,
                    self._program,
                )
            )
        )

    @sk.on_detach
    async def entity_deinit(self):
        """Stop subscriber, processor, and server."""

        logger.info(f"Stopping AutofocusAnalyzer for {self.config.entity}")
        for task in self._tasks:
            task.cancel()

    async def _subscribe_senpai(self):
        """Subscribe to SenpaiResult and enqueue for processing."""

        try:
            senpai = self.sk_client.entity(self.config.senpai_entity)
            async for _, result in await senpai.monitor(SenpaiResult):
                await self._senpai_queue.put(result)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Senpai subscription failed: {e}")
            raise

    async def _processor_loop(self):
        """Main loop to consume SenpaiResults and process them for focus."""

        try:
            while True:
                result = await self._senpai_queue.get()
                try:
                    await self._process_senpai(result)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Frame processing error")
        except asyncio.CancelledError:
            raise

    async def _process_senpai(self, result: SenpaiResult):
        """Process a single SenpaiResult: read FITS header, analyze stats, apply correction."""

        # Parse the FITS header
        try:
            header = await asyncio.to_thread(self._read_fits_header, result.file_path)
            focus_position = float(header.get("FOCUSPOS") or header.get("FOCUS_POS"))
            altitude = header.get("ALT_OBJ") or header.get("ALT")
            if altitude is not None:
                altitude = float(altitude)

            # Keywords for a calibration task (V-curve)
            calibration_id = header.get("CAL_ID")
            calibration_num = header.get("CAL_NUM")
            calibration_total = header.get("CAL_TOT")
            if calibration_num is not None:
                calibration_num = int(calibration_num)
            if calibration_total is not None:
                calibration_total = int(calibration_total)

        except Exception as e:
            logger.warning(f"Unable to parse FITS header from {result.file_path}: {e}")
            raise

        # Handle V-curve calibration frame
        if calibration_id is not None:
            await self._process_vcurve(
                calibration_id, calibration_num, calibration_total,
                focus_position, result.median_fwhm_pixels, result.pixel_scale_arcsec
            )

        # Compute defocus sign for passive correction
        # NOTE: this currently only works for sidereal frames, but we let rate-track frames pass through the pipeline
        # in preparation for a future rate-track support mode
        defocus_sign = 0
        if result.solved and result.detections:
            try:
                image_data, image_shape = await asyncio.to_thread(
                    self._load_fits_image, result.file_path,
                )
                defocus_sign = await asyncio.to_thread(
                    compute_defocus_sign, image_data, image_shape,
                    result.detections, result.median_fwhm_pixels,
                )
            except Exception as e:
                logger.debug(f"Unable to compute defocus sign from {result.file_path}: {e}")

        logger.debug(
            f"frame: FWHM={result.median_fwhm_pixels:.2f} pixels "
            f"({result.median_fwhm_arcsec or 0:.2f}\"), "
            f"n_sources={result.n_sources}, solved={result.solved}, "
            f"defocus_sign={defocus_sign}"
        )

        # Handle passive correction
        if calibration_id is None and self._should_correct(
            result.median_fwhm_pixels, result.pixel_scale_arcsec,
            result.solved, focus_position, defocus_sign, altitude,
        ):
            await self._apply_correction(
                result.median_fwhm_pixels, result.pixel_scale_arcsec,
                focus_position, defocus_sign,
            )

    @staticmethod
    def _read_fits_header(file_path: str) -> dict:
        with fits.open(file_path) as hdul:
            return dict(hdul[0].header)

    @staticmethod
    def _load_fits_image(file_path: str):
        with fits.open(file_path) as hdul:
            data = hdul[0].data.astype(np.float64)
            return data, data.shape

    async def _process_vcurve(
        self,
        cal_id: str,
        calibration_num: int,
        calibration_total: int,
        focus_position: float,
        median_fwhm_pixels: float,
        pixel_scale_arcsec: float,
    ):
        """Accumulate V-curve calibration frames and fit when complete."""

        if median_fwhm_pixels <= 0:
            logger.warning("V-curve frame missing FWHM stats")
            return

        # Accumulate
        if cal_id not in self._vcurve_frames:
            self._vcurve_frames[cal_id] = []
        self._vcurve_frames[cal_id].append((focus_position, median_fwhm_pixels))

        logger.info(
            f"V-curve frame {(calibration_num or 0) + 1}/{calibration_total}: "
            f"position={focus_position:.1f}, FWHM={median_fwhm_pixels:.2f} pixels"
        )

        # Check if the V-curve is complete
        if calibration_num + 1 == calibration_total:
            await self._fit_vcurve(cal_id, pixel_scale_arcsec)
            await self._apply_correction(cal_id)

    async def _fit_vcurve(self, cal_id: str, pixel_scale_arcsec: float):
        """Fit the V-curve and persist to state."""

        data = self._vcurve_frames.pop(cal_id, [])
        if len(data) < 3:
            logger.warning(f"Insufficient data points ({len(data)}) for {cal_id}")
            return

        logger.debug(f"Fitting {cal_id} with {len(data)} points")

        # Fit in thread pool
        vcurve_result = await asyncio.to_thread(fit_vcurve, data)

        logger.info(
            f"V-curve: slope={vcurve_result.slope:.6f}, R²={vcurve_result.r_squared:.4f}, "
            f"best_fwhm={vcurve_result.best_fwhm:.2f} pixels, best_position={vcurve_result.best_position:.1f}"
        )

        # Update state
        self.state.vcurve_slope = vcurve_result.slope
        self.state.vcurve_best_fwhm_pixels = vcurve_result.best_fwhm

        # Auto-calibrate defocus sign convention from V-curve shape
        # NOTE: this assumes an asymmetric V-curve, i.e. worse FWHMs on average on one side of best-focus
        positions = [d[0] for d in data]
        fwhms = [d[1] for d in data]
        above = [(p, f) for p, f in zip(positions, fwhms) if p > vcurve_result.best_position]
        below = [(p, f) for p, f in zip(positions, fwhms) if p < vcurve_result.best_position]
        if above and below:
            mean_above = sum(f for _, f in above) / len(above)
            mean_below = sum(f for _, f in below) / len(below)
            if abs(mean_above - mean_below) > 0.1:
                self.state.defocus_sign_convention = 1 if mean_above > mean_below else -1

        # Clamp to focuser limits
        best_position = max(
            self.config.focuser.min_position,
            min(vcurve_result.best_position, self.config.focuser.max_position),
        )

        # Publish V-curve results
        await self._entity.publish(
            AutofocusStatus(
                vcurve_timestamp=datetime.now(UTC),
                vcurve_best_position=best_position,
                vcurve_best_fwhm_arcsec=vcurve_result.best_fwhm * pixel_scale_arcsec,
                method="vcurve",
            )
        )

    def _should_correct(
        self,
        median_fwhm_pixels: float,
        pixel_scale_arcsec: float | None,
        solved: bool,
        focus_position: float | None,
        defocus_sign: int,
        altitude: float | None = None,
    ) -> bool:
        """Determine if a passive focus correction should be applied."""

        # Need a known focus position
        if focus_position is None:
            return False

        # Need V-curve calibration data
        if self.state.vcurve_slope is None:
            return False

        # Need a valid FWHM
        if median_fwhm_pixels <= 0 or not solved:
            return False

        # Reject frames with seeing worse than max_psf_arcsec
        if pixel_scale_arcsec and median_fwhm_pixels * pixel_scale_arcsec > self.config.max_psf_arcsec:
            logger.debug(
                f"skipping correction: FWHM {median_fwhm_pixels * pixel_scale_arcsec:.2f}\" "
                f"exceeds max_psf_arcsec ({self.config.max_psf_arcsec}\")"
            )
            return False

        # Need a defocus direction
        if defocus_sign == 0:
            return False

        # Do not correct low altitude frames
        if altitude is not None and altitude < self.config.min_altitude:
            logger.debug(f"skipping correction: altitude {altitude:.1f}° < min {self.config.min_altitude}°")
            return False

        # Check cooldown
        if self.state.last_correction_time is not None:
            elapsed = (datetime.now(UTC) - self.state.last_correction_time).total_seconds()
            if elapsed < self.config.correction.cooldown_seconds:
                return False

        # Select either the V-curve FWHM or the user-defined (intentionally worse) FWHM
        target_fwhm = self.state.vcurve_best_fwhm_pixels
        if self.config.correction.defocus_target_arcsec is not None and pixel_scale_arcsec:
            target_fwhm = self.config.correction.defocus_target_arcsec / pixel_scale_arcsec
        if target_fwhm is None or target_fwhm <= 0:
            return False

        # Only apply a correction if the measured FWHM is 10% worse than the FWHM we chose above
        # FIXME: determine if 10% is appropriate and/or needs configured, via testing
        if median_fwhm_pixels <= target_fwhm * 1.1:
            return False

        return True

    async def _apply_correction(
        self,
        median_fwhm_pixels: float | None = None,
        pixel_scale_arcsec: float | None = None,
        focus_position: float | None = None,
        defocus_sign: int | None = None,
        cal_id: str | None = None,
    ):
        """Compute a passive focus correction and apply either it or a V-curve result."""

        focuser = self.sk_client.device(self.config.focuser.entity)

        # Apply the resulting V-curve best focus position
        if cal_id is not None:
            new_position = AutofocusStatus.vcurve_best_position
            await focuser.command(ChangeFocusPosition(position=new_position))
            logger.info(f"Moved focuser to resulting V-curve best focus position: {new_position:.1f}")

        # Compute the passive correction

        delta = self._compute_correction(
            median_fwhm_pixels, pixel_scale_arcsec, defocus_sign,
        )
        if abs(delta) < self.config.correction.min_correction:
            return

        old_position = focus_position
        new_position = old_position + delta

        # Clamp to focuser limits
        new_position = max(
            self.config.focuser.min_position,
            min(new_position, self.config.focuser.max_position),
        )

        logger.info(
            f"Passive correction: {old_position:.1f} → {new_position:.1f} "
            f"(Δ={delta:.1f}, FWHM={median_fwhm_pixels:.2f}px)"
        )

        await focuser.command(ChangeFocusPosition(position=new_position))

        self.state.last_correction_time = datetime.now(UTC)

        # Publish correction
        await self._entity.publish(
            AutofocusStatus(
                old_position=old_position,
                new_position=new_position,
                method="passive",
            )
        )

    def _compute_correction(
        self,
        median_fwhm_px: float,
        pixel_scale_arcsec: float | None,
        defocus_sign: int,
    ) -> float:
        """Compute focus correction magnitude and direction.

        Uses: delta = sign * sqrt((FWHM^2 - FWHM_target^2) / a)
        where a is the V-curve slope.
        """

        a = self.state.vcurve_slope
        if a is None or a <= 0:
            return 0.0

        target_fwhm = self.state.vcurve_best_fwhm_pixels or 0.0
        if self.config.correction.defocus_target_arcsec is not None and pixel_scale_arcsec:
            target_fwhm = self.config.correction.defocus_target_arcsec / pixel_scale_arcsec

        current_fwhm = median_fwhm_px
        fwhm_sq_diff = current_fwhm ** 2 - target_fwhm ** 2

        if fwhm_sq_diff <= 0:
            return 0.0

        magnitude = math.sqrt(fwhm_sq_diff / a)

        # Apply defocus sign and sign convention
        sign = defocus_sign * self.state.defocus_sign_convention
        delta = sign * magnitude

        # Cap by max step
        delta = max(-self.config.correction.max_step, min(delta, self.config.correction.max_step))

        return delta
