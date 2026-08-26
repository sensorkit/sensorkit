from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import numpy as np
from astropy.io import fits
from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.autofocus.models import (
    AutofocusConfig,
    AutofocusState,
    AutofocusStatus,
    RunVCurve,
    SetAutofocusEnabled,
    VCurveResult,
    run_vcurve_request,
    set_enabled_request,
)
from sensorkit.autofocus.pipeline import (
    compute_defocus_sign,
    fit_vcurve,
)
from sensorkit.backend.base import KeyNotFound
from sensorkit.senpai.models import SenpaiResult
from sensorkit.std.optics import FocusCorrection

if TYPE_CHECKING:
    from sensorkit.autofocus.program import AutofocusProgram
    from sensorkit.core.client import SensorKit


async def _read_residual(focuser) -> FocusCorrection:
    """The focuser's standing FocusCorrection residual, or a zero one.

    An absent key is the ordinary case — nothing has corrected this focuser yet — and is
    silent. Any other failure is not: folding a new correction onto a zero we only *assumed*
    would discard the standing residual and move the focuser by that much, so it is logged.
    """
    try:
        return await focuser.kv_get_model(FocusCorrection)
    except KeyNotFound:
        return FocusCorrection()
    except Exception as e:
        logger.warning(f"Could not read the standing FocusCorrection; assuming zero ({e})")
        return FocusCorrection()


def _log_task_exception(task: asyncio.Task):
    """Surface a detached task's failure — without this it is swallowed silently."""
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error(f"Background task failed: {exc!r}")


def _is_sidereal(commanded, inferred: str | None) -> bool:
    """Whether a frame was sidereal-tracked, preferring the commanded mode over the inference.

    `commanded` is the FITS TRKMODE card the controller stamps; `inferred` is
    SenpaiResult.track_mode, measured from star elongation. Only fall back to the inference when
    the card is absent — the inference cannot distinguish a short streak from a round star and
    resolves ties toward SIDEREAL, which is the unsafe direction for us.
    """
    if commanded is not None:
        return str(commanded).strip().upper() == "SIDEREAL"
    return inferred == "SIDEREAL"


def _parse_frame_time(date_obs) -> datetime | None:
    """Parse a FITS DATE-OBS card to an aware UTC datetime; None if absent/unparseable."""
    if not date_obs:
        return None
    try:
        parsed = datetime.fromisoformat(str(date_obs))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass
class _Sweep:
    """The V-curve sweep currently being accumulated.

    Only one sweep accumulates at a time: sweep captures are serialized through the controller and
    SENPAI emits results in capture order, so two sweeps' frames never interleave — a frame from a
    new session means the previous sweep is finished.
    """

    session: str  # AFID
    expected_steps: int | None  # from expect_sweep(); None if the analyzer restarted mid-sweep
    seen_paths: set[str] = field(default_factory=set)  # dedups duplicate filesystem events
    points: list[tuple[float, float]] = field(default_factory=list)  # measurable (focus, FWHM px)
    unmeasured: list[float] = field(default_factory=list)  # positions SENPAI returned no FWHM for
    sign_votes: list[tuple[float, int]] = field(default_factory=list)  # (focus, quadrupole sign)
    pixel_scale_arcsec: float = 0.0  # from the sweep's frames, for reporting arcsec
    timer: asyncio.Task | None = None  # stall fallback: finalizes if a frame never arrives


@sk.declare_entity
class AutofocusAnalyzer:
    """Applies focus corrections based on SENPAI statistics."""

    def __init__(self, config: AutofocusConfig, sk_client: SensorKit):
        self.config = config
        self.sk_client = sk_client

        # Mutual reference, set by the service entrypoint (program.py) — the analyzer queues
        # recalibration sweeps through the program (the "recalibrate" branch in _process_senpai).
        self.program: AutofocusProgram | None = None

        self._sweep: _Sweep | None = None
        self._announced: tuple[str, int] | None = None  # (session, step count) of the next sweep
        # Set when a sweep is announced, cleared when it folds: while set, passive correction is
        # suppressed so a stray science frame can't race the fold's residual baseline.
        self._sweep_in_flight_since: datetime | None = None
        self._senpai_queue: asyncio.Queue[SenpaiResult] = asyncio.Queue()

        self._entity = None
        self._tasks: list[asyncio.Task] = []

        self.state = AutofocusState()

    @sk.on_attach
    async def entity_init(self):
        """Restore state, subscribe to `senpai`, start the processor."""

        self._entity = sk.entity()
        logger.info(f"Starting Autofocus analyzer for {self.config.entity}")

        # Restore persisted state (enable flag, calibration, cooldowns); boot lands where it left.
        try:
            self.state = await self._entity.kv_get_model(AutofocusState)
            logger.debug(f"restored state for {self.config.entity}")
        except Exception:
            logger.warning(f"No saved state for {self.config.entity}")
        logger.info(
            f"Autofocus analyzer starting {'ENABLED' if self.state.enabled else 'disabled'}"
        )

        # Control surface: entity-level Requests (see models.run_vcurve_request).
        await self._entity.handle_request(run_vcurve_request, self.run_vcurve)
        await self._entity.handle_request(set_enabled_request, self.set_autofocus_enabled)

        # Subscribe to SENPAI results and process them
        self._tasks.append(asyncio.create_task(self._subscribe_senpai()))
        self._tasks.append(asyncio.create_task(self._processor_loop()))

    async def run_vcurve(self, cmd: RunVCurve):
        """Queue a V-curve sweep. Omit ra/dec to auto-select a target.

        Hands off to a task: selecting a target and pushing the per-step tasks outlasts the
        deadline a request reply has to meet. Outcome is in the log and in VCurveResult — the
        recalibrate path calls queue_vcurve directly when it needs the answer.
        """
        target = None
        if cmd.ra is not None and cmd.dec is not None:
            target = ICRSTarget(coords=Equatorial(ra=cmd.ra, dec=cmd.dec))

        task = asyncio.create_task(self.program.queue_vcurve(target=target))
        task.add_done_callback(_log_task_exception)
        self._tasks.append(task)

    async def set_autofocus_enabled(self, cmd: SetAutofocusEnabled):
        """Enable/disable the analyzer's focus corrections (base + filter offset still drive)."""
        await self.set_enabled(cmd.enabled)

    @sk.on_detach
    async def entity_deinit(self):
        """Stop subscriber and processor."""

        logger.info(f"Stopping Autofocus analyzer for {self.config.entity}")
        for task in self._tasks:
            task.cancel()
        if self._sweep is not None and self._sweep.timer is not None:
            self._sweep.timer.cancel()

    async def _save_state(self):
        try:
            await self._entity.kv_put_model(self.state)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def set_enabled(self, enabled: bool):
        """Enable/disable the analyzer (passive corrections, recalibrate triggers, V-curve
        folds). The controller keeps driving base + filter offset + the last residual."""
        self.state.enabled = enabled
        await self._save_state()
        logger.info(f"Autofocus analyzer {'enabled' if enabled else 'disabled'}")

    async def _subscribe_senpai(self):
        """Subscribe to SenpaiResult and enqueue for processing.

        Re-subscribes if the stream errors or ends — losing this feed would silently disable
        both V-curve fitting and passive correction while the rest of the service looks healthy.
        """

        while True:
            try:
                senpai = self.sk_client.entity(self.config.senpai_entity)
                async for _, result in await senpai.monitor(SenpaiResult):
                    await self._senpai_queue.put(result)
                logger.warning("Senpai result stream ended; re-subscribing")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Senpai subscription failed; re-subscribing: {e}")
            await asyncio.sleep(5)

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

    async def _process_senpai(self, result: SenpaiResult):  # noqa: C901  (linear parse→dispatch)
        """Process a single SenpaiResult: read FITS header, analyze stats, apply correction."""

        # The analyzer sees EVERY SENPAI result, so skip anything unreadable or without a
        # FOCUSPOS (not an autofocus frame) rather than erroring.
        try:
            header = await asyncio.to_thread(self._read_fits_header, result.file_path)
        except Exception as e:
            logger.debug(f"Skipping {result.file_path}: unreadable FITS header ({e})")
            return

        focus_raw = header.get("FOCUSPOS")
        if focus_raw is None:
            return  # not an autofocus frame
        focus_position = float(focus_raw)

        altitude = header.get("ALT_OBJ") or header.get("ALT")
        if altitude is not None:
            altitude = float(altitude)

        # SENPAI's solved plate scale (already per binned pixel), falling back to the configured
        # one when the pipeline doesn't solve (detect mode). The config value is per NATIVE (1x1)
        # pixel, so scale it up by the sweep binning to match the binned FWHM we measure against.
        pixel_scale = result.pixel_scale_arcsec
        if pixel_scale is None and self.config.pixel_scale_arcsec is not None:
            pixel_scale = self.config.pixel_scale_arcsec * self.config.vcurve.binning

        # A V-curve sweep frame (AFID card) feeds the fit. Its sign is measured too: sweep frames
        # sit at known offsets from best, so they calibrate the convention (see _finalize_sweep).
        vcurve_session = header.get("AFID")
        if vcurve_session is not None:
            sign = 0
            if result.median_fwhm_pixels:
                sign = await self._measure_defocus_sign(result)
            await self._process_vcurve(
                vcurve_session,
                result.file_path,
                focus_position,
                result.median_fwhm_pixels,
                pixel_scale,
                sign,
            )
            return

        # Sweep-in-flight guard: from the moment a V-curve is queued until its fit folds, the sweep
        # owns focus. A passive write to FocusCorrection in that window races the fold's baseline
        # (the fold assumes the residual it reads is the one the sweep centered on — a concurrent
        # write breaks that, corrupting the fold). Drop science frames until the sweep folds; the
        # fold supersedes them anyway. Self-clear if a queued sweep never produced a frame.
        if self._sweep_in_flight_since is not None:
            budget = (self.config.vcurve.num_steps + 2) * (
                self.config.vcurve.exposure_time + self.config.focuser.timeout
            )
            elapsed = (datetime.now(UTC) - self._sweep_in_flight_since).total_seconds()
            if self._sweep is None and elapsed > budget:
                logger.warning(
                    f"V-curve announced but no sweep frame arrived in {budget:.0f}s — "
                    "clearing sweep-in-flight guard"
                )
                self._sweep_in_flight_since = None
            else:
                return

        # Passive correction only on a SIDEREAL frame with a measured FWHM — a rate frame's stars
        # are streaks, so its FWHM is trailing, not focus. Gate on the FITS TRKMODE (the COMMANDED
        # mode) not SenpaiResult.track_mode: the latter is inferred from star elongation and falls
        # back to SIDEREAL when inconclusive, so a short-streak rate frame arrives mislabelled
        # (observed live, queuing a spurious recalibration off 4.31" of pure trailing).
        if not _is_sidereal(header.get("TRKMODE"), result.track_mode):
            return
        if not result.median_fwhm_pixels:
            return

        # Stale-frame guard: SENPAI's serial queue can lag captures by minutes, so a frame shot
        # BEFORE the last residual change can arrive after the cooldown and get folded again — a
        # double-correction on a residual it never saw (observed live). It can't judge the current
        # residual, so drop it.
        if self.state.last_correction_time is not None:
            frame_time = _parse_frame_time(header.get("DATE-OBS"))
            if frame_time is not None and frame_time < self.state.last_correction_time:
                logger.debug(
                    f"Skipping stale frame {result.file_path}: captured {frame_time} before "
                    f"the last residual change {self.state.last_correction_time}"
                )
                return

        # Direction from the radial-ellipticity pattern, then decide whether to act.
        defocus_sign = await self._measure_defocus_sign(result)

        logger.debug(
            f"frame: FWHM={result.median_fwhm_pixels:.2f} pixels "
            f'({result.median_fwhm_arcsec or 0:.2f}"), '
            f"n_sources={result.n_sources}, solved={result.solved}, "
            f"defocus_sign={defocus_sign}"
        )

        decision = self._evaluate_correction(
            result.median_fwhm_pixels,
            pixel_scale,
            result.solved,
            focus_position,
            defocus_sign,
            altitude,
        )
        if decision == "correct":
            await self._apply_correction(
                result.median_fwhm_pixels,
                pixel_scale,
                focus_position,
                defocus_sign,
                result.file_path,
            )
        elif decision == "recalibrate":
            # Queue a V-curve through the program (which publishes offers; normal scheduling runs
            # it — no controller abort). Stamp the cooldown only when it actually queued, so a
            # failed attempt (no target, program not ready) is retried on the next frame.
            logger.info("Too defocused for a passive correction — queueing a V-curve")
            if await self.program.queue_vcurve():
                self.state.last_vcurve_trigger_time = datetime.now(UTC)
                await self._save_state()

    async def _measure_defocus_sign(self, result: SenpaiResult) -> int:
        """Measure the quadrupole defocus sign for one frame; 0 if unavailable.

        Field stars (any pipeline mode) are the principled input; fall back to the legacy
        full-mode detections list for older sensorkit-senpai results.
        """
        sources = result.stars or result.detections
        if not sources:
            return 0
        try:
            image_data, image_shape = await asyncio.to_thread(
                self._load_fits_image,
                result.file_path,
            )
            return await asyncio.to_thread(
                compute_defocus_sign,
                image_data,
                image_shape,
                sources,
                result.median_fwhm_pixels,
                self.config.correction.defocus_sign_threshold,
            )
        except Exception as e:
            # Visible on purpose: a sign-computation failure silently downgrades every
            # passive correction to a skip, which reads as "autofocus does nothing".
            logger.warning(f"Unable to compute defocus sign from {result.file_path}: {e}")
            return 0

    @staticmethod
    def _read_fits_header(file_path: str) -> dict:
        with fits.open(file_path) as hdul:
            return dict(hdul[0].header)

    @staticmethod
    def _load_fits_image(file_path: str):
        with fits.open(file_path) as hdul:
            data = hdul[0].data.astype(np.float64)
            return data, data.shape

    def expect_sweep(self, session: str, count: int):
        """The program announces a queued sweep's session and step count here, so the fit can run
        the instant the last frame arrives instead of waiting out the stall timer. Also arms the
        sweep-in-flight guard (see _process_senpai) so passive correction can't race the fold."""
        self._announced = (session, count)
        self._sweep_in_flight_since = datetime.now(UTC)

    async def _process_vcurve(
        self,
        session: str,
        file_path: str,
        focus_position: float,
        median_fwhm_pixels: float,
        pixel_scale_arcsec: float,
        defocus_sign: int = 0,
    ):
        """Accumulate one sweep frame; fit the instant the last expected frame arrives."""

        sweep = self._sweep
        if sweep is None or sweep.session != session:
            if sweep is not None:
                # Frames arrive in capture order, so a new session means the old sweep is done.
                await self._finalize_sweep()
            expected = None
            if self._announced is not None and self._announced[0] == session:
                expected = self._announced[1]
                self._announced = None
            sweep = self._sweep = _Sweep(session=session, expected_steps=expected)

        if file_path in sweep.seen_paths:
            return  # duplicate filesystem event for a frame we already counted
        sweep.seen_paths.add(file_path)

        if defocus_sign != 0:
            sweep.sign_votes.append((focus_position, defocus_sign))

        if median_fwhm_pixels and median_fwhm_pixels > 0:
            sweep.points.append((focus_position, median_fwhm_pixels))
            if pixel_scale_arcsec:
                sweep.pixel_scale_arcsec = pixel_scale_arcsec
            logger.info(
                f"V-curve {session}: position={focus_position:.1f}, FWHM={median_fwhm_pixels:.2f} "
                f"pixels ({len(sweep.points)} measurable / {len(sweep.seen_paths)} arrived)"
            )
        else:
            sweep.unmeasured.append(focus_position)
            logger.info(
                f"V-curve {session}: focus {focus_position:.0f} not measurable "
                f"({len(sweep.seen_paths)} arrived — no FWHM from SENPAI, likely too defocused)"
            )

        # Fit the moment every step has arrived. Until then, keep a stall timer running so a lost
        # frame (or an unknown step count after a restart) can't wedge the sweep forever.
        if sweep.expected_steps is not None and len(sweep.seen_paths) >= sweep.expected_steps:
            await self._finalize_sweep()
        else:
            if sweep.timer is not None:
                sweep.timer.cancel()
            sweep.timer = asyncio.create_task(self._sweep_timeout(sweep))

    async def _sweep_timeout(self, sweep: _Sweep):
        """Stall fallback: finalize the sweep if no new frame arrives in time (each new frame
        cancels and restarts this timer)."""

        try:
            await asyncio.sleep(self.config.vcurve.frame_timeout_seconds)
        except asyncio.CancelledError:
            return
        if self._sweep is not sweep:
            return  # already finalized
        logger.info(
            f"V-curve {sweep.session}: no frame for "
            f"{self.config.vcurve.frame_timeout_seconds:.0f}s — finalizing with what arrived"
        )
        sweep.timer = None  # this task IS the timer; don't cancel ourselves in finalize
        await self._finalize_sweep()

    async def _finalize_sweep(self):
        # Guarantee the sweep-in-flight guard clears however the fit/fold exits (early return or
        # error), so passive correction can never be locked out permanently.
        try:
            await self._fit_and_fold_sweep()
        finally:
            self._sweep_in_flight_since = None

    async def _fit_and_fold_sweep(self):
        """Fit the accumulated sweep, save the calibration, move to best focus, publish."""

        sweep, self._sweep = self._sweep, None
        if sweep is None:
            return
        if sweep.timer is not None:
            sweep.timer.cancel()

        data = sweep.points
        if len(data) < 3:
            logger.warning(
                f"V-curve {sweep.session}: only {len(data)} measurable frame(s); cannot fit"
            )
            return

        result = await asyncio.to_thread(fit_vcurve, data)
        logger.info(
            f"V-curve {sweep.session}: slope={result.slope:.6f}, R²={result.r_squared:.4f}, "
            f"best_fwhm={result.best_fwhm:.2f} pixels, best_position={result.best_position:.1f}"
        )

        self.state.vcurve_slope = result.slope
        self.state.vcurve_best_fwhm_pixels = result.best_fwhm

        # Calibrate the sign→direction convention from the sweep's quadrupole votes. A frame at
        # position p with measured sign s needs Δ = s·convention·magnitude to point toward best,
        # so convention = s·sign(best - p). Exclude frames within half a step of best (the fit's
        # own error can flip the offset's sign) and require net ≥2 so one noisy frame can't set it;
        # too few votes leaves the prior calibration (and passive correction off) in place.
        positions = sorted({p for p, _ in data} | set(sweep.unmeasured))
        spacing = min((b - a for a, b in zip(positions, positions[1:], strict=False)), default=0.0)
        votes = [
            s * (1 if result.best_position > p else -1)
            for p, s in sweep.sign_votes
            if abs(p - result.best_position) > 0.5 * spacing
        ]
        net = sum(votes)
        if abs(net) >= 2:
            self.state.defocus_sign_convention = 1 if net > 0 else -1
            logger.info(
                f"V-curve {sweep.session}: defocus sign convention calibrated to "
                f"{self.state.defocus_sign_convention:+d} ({len(votes)} votes, net {net:+d})"
            )
        elif self.state.defocus_sign_convention is None:
            # Loud on purpose: sweep looks fine, but with no direction every passive correction
            # skips and only V-curves refocus this sensor.
            logger.warning(
                f"V-curve {sweep.session}: defocus sign convention NOT calibrated "
                f"({len(votes)} usable votes, net {net:+d}) — passive correction stays disabled; "
                f"the optics may lack field-dependent astigmatism, or lower "
                f"correction.defocus_sign_threshold "
                f"(currently {self.config.correction.defocus_sign_threshold})"
            )

        await self._save_state()

        # Publish the result BEFORE moving the focuser: the fit exists regardless of whether the
        # move succeeds, and a device failure must not swallow the sweep from clients.
        best_position = max(
            self.config.focuser.min_position,
            min(result.best_position, self.config.focuser.max_position),
        )
        await self._entity.publish(
            VCurveResult(
                timestamp=datetime.now(UTC),
                session=sweep.session,
                best_position=best_position,
                best_fwhm_pixels=result.best_fwhm,
                slope=result.slope,
                r_squared=result.r_squared,
                pixel_scale_arcsec=sweep.pixel_scale_arcsec or None,
                samples=sorted(data),
                unmeasured_positions=sorted(sweep.unmeasured),
            )
        )

        # Fold the fit into the residual: best sits (best - sweep_center) from where the standing
        # target had us, so shift the residual by that. (See README Focus Model.)
        if not self.state.enabled:
            logger.info(
                f"V-curve {sweep.session}: analyzer disabled — fit published but not folded "
                f"into FocusCorrection"
            )
            return
        all_positions = [position for position, _ in data] + list(sweep.unmeasured)
        sweep_center = (min(all_positions) + max(all_positions)) / 2.0
        focuser = self.sk_client.device(self.config.focuser.entity)
        old = await _read_residual(focuser)
        residual = old.position + (best_position - sweep_center)
        await focuser.kv_put_model(FocusCorrection(position=residual))
        # The fold is a residual change: stamp it so the stale-frame guard (and the passive
        # cooldown) discard queued frames captured under the pre-fold residual.
        self.state.last_correction_time = datetime.now(UTC)
        await self._save_state()
        logger.info(
            f"V-curve {sweep.session}: residual correction {old.position:+.1f} → {residual:+.1f} "
            f"(best {best_position:.1f}, sweep center {sweep_center:.1f})"
        )

    def _evaluate_correction(  # noqa: C901
        self,
        median_fwhm_pixels: float,
        pixel_scale_arcsec: float | None,
        solved: bool,
        focus_position: float | None,
        defocus_sign: int,
        altitude: float | None = None,
    ) -> str:
        """Decide what to do with a non-V-curve frame: 'correct', 'recalibrate', or 'skip'.

        Limits are FWHM degradation from the target focus, in arcsec — the metric we act on.
        Below `min_arcsec` -> skip (deadband). Above `max_arcsec` -> recalibrate (a single-frame
        passive estimate, and defocus_sign, are unreliable that far out — a V-curve is the tool).
        """

        # Master switch: a disabled analyzer never touches the residual.
        if not self.state.enabled:
            return "skip"

        # Limits are in arcsec, so we need a plate scale — plus a known focus, calibration, solve.
        if not pixel_scale_arcsec or focus_position is None:
            return "skip"
        if self.state.vcurve_slope is None:
            return "skip"
        if median_fwhm_pixels <= 0 or not solved:
            return "skip"

        # Do not act on low-altitude frames.
        if altitude is not None and altitude < self.config.min_altitude:
            logger.debug(
                f"skipping correction: altitude {altitude:.1f}° < min {self.config.min_altitude}°"
            )
            return "skip"

        # Target FWHM: the V-curve best, or the user's intentional-defocus target.
        target_fwhm = self.state.vcurve_best_fwhm_pixels
        if self.config.correction.defocus_target_arcsec is not None:
            target_fwhm = self.config.correction.defocus_target_arcsec / pixel_scale_arcsec
        if target_fwhm is None or target_fwhm <= 0:
            return "skip"

        # FWHM degradation from target, in arcsec — what the limits are expressed in.
        error_arcsec = (median_fwhm_pixels - target_fwhm) * pixel_scale_arcsec

        # Deadband: focus is already good enough.
        if error_arcsec < self.config.correction.min_arcsec:
            return "skip"

        # Too far out for a reliable single-frame passive estimate -> recalibrate with a V-curve,
        # unless we already kicked one off recently.
        if error_arcsec > self.config.correction.max_arcsec:
            last = self.state.last_vcurve_trigger_time
            if last is not None:
                elapsed = (datetime.now(UTC) - last).total_seconds()
                if elapsed < self.config.correction.cooldown_seconds:
                    return "skip"
            logger.info(
                f'FWHM {error_arcsec:.2f}" worse than best exceeds max_arcsec '
                f'({self.config.correction.max_arcsec}")'
            )
            return "recalibrate"

        # Inside the band, need a real direction: both this frame's sign AND the learned
        # convention. Either missing = a guess that corrects INTO the error, so skip. This sits
        # BELOW recalibrate on purpose — the V-curve is what recovers and what teaches the convention.
        if defocus_sign == 0 or self.state.defocus_sign_convention is None:
            return "skip"
        if self.state.last_correction_time is not None:
            elapsed = (datetime.now(UTC) - self.state.last_correction_time).total_seconds()
            if elapsed < self.config.correction.cooldown_seconds:
                return "skip"

        return "correct"

    async def _apply_correction(
        self,
        median_fwhm_pixels: float,
        pixel_scale_arcsec: float | None,
        focus_position: float,
        defocus_sign: int,
        file_path: str | None = None,
    ):
        """Fold a passive correction into the standing FocusCorrection residual."""

        delta = self._compute_correction(
            median_fwhm_pixels,
            pixel_scale_arcsec,
            defocus_sign,
        )
        if not delta:
            return

        # Shift the standing residual; the controller drives base + filter offset + residual at
        # every capture, so this takes effect on the next one.
        focuser = self.sk_client.device(self.config.focuser.entity)
        old = await _read_residual(focuser)

        frame = file_path.rsplit("/", 1)[-1] if file_path else "?"
        logger.info(
            f"Passive correction: residual {old.position:+.1f} → {old.position + delta:+.1f} "
            f"(Δ={delta:.1f}, FWHM={median_fwhm_pixels:.2f}px, frame {frame}) — applied at the "
            f"next capture"
        )
        await focuser.kv_put_model(FocusCorrection(position=old.position + delta))

        self.state.last_correction_time = datetime.now(UTC)
        await self._save_state()

        await self._entity.publish(
            AutofocusStatus(
                timestamp=datetime.now(UTC),
                old_position=focus_position,
                new_position=focus_position + delta,
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

        fwhm_sq_diff = median_fwhm_px**2 - target_fwhm**2
        if fwhm_sq_diff <= 0:
            return 0.0

        # The magnitude is bounded upstream by max_arcsec (the excursion ceiling), and the final
        # position is clamped to the focuser's hard limits by the caller — no separate step cap.
        # Direction is this frame's measured sign mapped through the V-curve-learned convention;
        # _evaluate_correction has already refused the frame if either is missing.
        if not defocus_sign or self.state.defocus_sign_convention is None:
            return 0.0
        magnitude = math.sqrt(fwhm_sq_diff / a)
        return defocus_sign * self.state.defocus_sign_convention * magnitude
