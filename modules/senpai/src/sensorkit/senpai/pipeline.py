# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import math
import time
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from sensorkit.senpai.models import Detection, Photometry, SenpaiResult

# Importing the engine is noisy, and self-defeatingly so: `senpai` installs a
# console handler on the root logger partway through (see `sensorkit.senpai`), and
# whatever it imports afterwards logs onto it — matplotlib announcing a rebuilt
# font cache — before `analyzer.quiet_engine_logging()` can take the handler off
# again. Muting the `senpai` namespace alone doesn't cover that, since the records
# are somebody else's. Drop INFO and below for the duration of the import instead;
# warnings and errors still get through.
logging.disable(logging.INFO)
try:
    from senpai.core.config import initialize_config
    from senpai.engine.models.images import ProcessedFitsImage
    from senpai.engine.processing.collect import final_plots, process_senpai_collect
finally:
    logging.disable(logging.NOTSET)


@dataclass
class FrameInput:
    """One frame handed to the pipeline, with its collect identity from the DataGraph context."""

    data: bytes
    file_path: str
    task_id: str | None = None
    frame_num: int | None = None
    frame_count: int | None = None


def _iso_z(dt: datetime) -> str:
    """Format a UTC datetime as ISO-8601 with a trailing ``Z``."""
    return dt.isoformat().replace("+00:00", "Z")


def _obs_time(image) -> datetime | None:
    """A frame's DATE-OBS as a UTC datetime, or None when it's missing/unparseable."""
    try:
        return datetime.fromisoformat(image.header["DATE-OBS"]).replace(tzinfo=UTC)
    except (KeyError, TypeError, ValueError):
        return None


class SenpaiPipeline:
    """Uses the `astro-senpai` unified collect pipeline."""

    def __init__(self, senpai_config: str, senpai_output_dir: str):
        config = initialize_config(Path(senpai_config))
        if senpai_output_dir is not None:
            config.runtime.output_dir = Path(senpai_output_dir)
        self.config = config

    def process_frames(
        self, inputs: list[FrameInput], from_sequence: bool = False
    ) -> list[SenpaiResult]:
        """Analyze one or more FITS frames as a single SENPAI collect.

        Multi-frame batches let SENPAI anchor the WCS from sidereal frames,
        propagate it to rate frames, and confirm streaks across frames.
        `from_sequence` stamps the results as sequence-derived — the analyzer
        passes True for any batch it assembled from a collect, including a
        complete single-frame collect.

        Returns one SenpaiResult per input, in input order (a frame whose
        results can't be extracted is logged and skipped rather than failing
        the batch).
        """
        images = [
            ProcessedFitsImage.from_file_bytes(inp.data, file_path=inp.file_path)
            for inp in inputs
        ]

        # Announce the collect: frame count, the task id when the frames carry one,
        # and the observation span (earliest–latest DATE-OBS across the frames).
        obs_times = [t for t in (_obs_time(im) for im in images) if t is not None]
        task_id = inputs[0].task_id if inputs else None
        if obs_times:
            span = f"{_iso_z(min(obs_times))} - {_iso_z(max(obs_times))}"
            detail = f"{task_id}, {span}" if task_id else span
        else:
            detail = task_id or ""
        logger.info(f"Analyzing {len(inputs)} frame(s)" + (f" ({detail})" if detail else ""))

        # Run the unified collect pipeline. Time just the analysis so the completion
        # line can report a duration on every path — SENPAI only fills in its own
        # compute_seconds on success, leaving nothing to report when a run fails.
        t0 = time.perf_counter()
        senpai_run = process_senpai_collect(images)
        elapsed = time.perf_counter() - t0

        # Generate annotated plots (WCS overlay, detections, etc.)
        final_plots(senpai_run, Path(self.config.runtime.output_dir))

        # Map the run's frames (split by track mode) back to their source files.
        frames_by_path: dict[str, tuple] = {}
        for frame in senpai_run.sidereal_frames:
            frames_by_path[str(frame.frame.file_path)] = (frame, "SIDEREAL")
        for frame in senpai_run.rate_track_frames:
            frames_by_path[str(frame.frame.file_path)] = (frame, "RATE")

        # Collect-level outcome at INFO/WARNING: a WCS line, then a photometry line
        # (only when a solve landed — photometry is gated on it), then a terminal
        # completion line. Per-frame numbers stay at DEBUG (see _process/_extract).
        n_frames = len(inputs)
        n_solved = sum(
            1 for frame, _ in frames_by_path.values() if frame.starfield and frame.starfield.fit
        )
        if n_solved == 0:
            logger.warning(f"No WCS solution found for any of {n_frames} frame(s)")
        else:
            logger.info(f"WCS solution found for {n_solved}/{n_frames} frame(s)")
            n_photom = sum(
                1
                for frame, _ in frames_by_path.values()
                if frame.photometry_summary
                and frame.photometry_summary.get("zero_point") is not None
            )
            if n_photom:
                logger.info(f"Photometry calibrated for {n_photom}/{n_frames} frame(s)")
            else:
                logger.warning(f"No photometry calibration for any of {n_frames} frame(s)")

        if senpai_run.completed:
            logger.info(f"SENPAI analysis complete in {elapsed:.1f}s")
        else:
            logger.warning(f"SENPAI analysis failed after {elapsed:.1f}s")

        results: list[SenpaiResult] = []
        for inp, image in zip(inputs, images, strict=True):
            try:
                results.append(
                    self._extract(inp, image, frames_by_path, from_sequence)
                )
            except Exception:
                # A bad frame (e.g. a corrupt header) must not discard the
                # rest of the batch's results.
                logger.exception(f"Failed to extract results for {inp.file_path}")

        return results

    def _extract(  # noqa: C901
        self,
        inp: FrameInput,
        image: ProcessedFitsImage,
        frames_by_path: dict[str, tuple],
        from_sequence: bool,
    ) -> SenpaiResult:
        """Build the SenpaiResult for one frame of the collect run."""
        timestamp = datetime.fromisoformat(image.header["DATE-OBS"]).replace(tzinfo=UTC)

        frame, track_mode = frames_by_path.get(inp.file_path, (None, "UNKNOWN"))
        if frame is None:
            logger.debug(f"No frame returned from collect pipeline for {inp.file_path}")
            return SenpaiResult(
                file_path=inp.file_path,
                timestamp=timestamp,
                track_mode="UNKNOWN",
                task_id=inp.task_id,
                frame_num=inp.frame_num,
                frame_count=inp.frame_count,
                from_sequence=from_sequence,
                exposure_time_seconds=image.header.get("EXPTIME"),
                n_sources=0,
                median_fwhm_pixels=None,
                std_fwhm_pixels=None,
                pixel_scale_arcsec=None,
                median_fwhm_arcsec=None,
                std_fwhm_arcsec=None,
                solved=False,
                detections=[],
            )

        # Extract WCS stats
        solved = False
        pixel_scale_arcsec = None
        if frame.starfield:
            solved = frame.starfield.fit
            if frame.starfield.wcs_metadata is not None:
                pixel_scale_arcsec = frame.starfield.wcs_metadata.x_ifov_arcsec

            # Diagnostic: compare WCS-solved center to FITS header RA/Dec
            if solved and frame.starfield.wcs is not None:
                wcs_astropy = frame.starfield.wcs.to_astropy_wcs()
                img_w = int(image.header.get("NAXIS1", 0))
                img_h = int(image.header.get("NAXIS2", 0))
                center_x, center_y = img_w / 2.0, img_h / 2.0
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    sky = wcs_astropy.pixel_to_world(center_x, center_y)
                wcs_ra, wcs_dec = sky.ra.deg, sky.dec.deg
                hdr_ra = image.header.get("RA")
                hdr_dec = image.header.get("DEC")
                if hdr_ra is not None:
                    dra = (wcs_ra - hdr_ra) * 3600.0  # arcsec
                    ddec = (wcs_dec - hdr_dec) * 3600.0

                    cos_dec = math.cos(math.radians(hdr_dec))
                    sep = math.sqrt((dra * cos_dec) ** 2 + ddec**2)
                    # Also log the CRPIX values
                    crpix1 = wcs_astropy.wcs.crpix[0]
                    crpix2 = wcs_astropy.wcs.crpix[1]
                    logger.debug(
                        f"WCS diagnostic ({track_mode}): "
                        f"FITS RA={hdr_ra:.6f} Dec={hdr_dec:.6f}, "
                        f"WCS center RA={wcs_ra:.6f} Dec={wcs_dec:.6f}, "
                        f'dRA={dra:.1f}" dDec={ddec:.1f}" sep={sep:.1f}", '
                        f"CRPIX=({crpix1:.1f}, {crpix2:.1f}), "
                        f"img_center=({center_x:.1f}, {center_y:.1f})"
                    )

        # Extract FWHM stats
        median_fwhm_pixels = None
        std_fwhm_pixels = None
        if track_mode == "SIDEREAL":
            # Plenty of point-sources, use those stats
            if frame.starfield and frame.starfield.fwhm_stats is not None:
                median_fwhm_pixels = frame.starfield.fwhm_stats.median_fwhm
                std_fwhm_pixels = frame.starfield.fwhm_stats.std_fwhm
        else:
            # More streaks, so use those; if unavailable, fall back to satellites (point-sources)
            if frame.streak:
                median_fwhm_pixels = frame.streak.fwhm
            if median_fwhm_pixels is None and frame.seeing:
                median_fwhm_pixels = frame.seeing.pixel_fwhm
            if frame.seeing:
                std_fwhm_pixels = frame.seeing.pixel_fwhm_stdev

        # Extract photometry
        photometry = None
        if frame.photometry_summary:
            ps = frame.photometry_summary
            photometry = Photometry(
                zero_point=ps.get("zero_point"),
                zero_point_err=ps.get("zero_point_err"),
                limiting_mag=ps.get("limiting_magnitude"),
                limiting_mag_50=ps.get("limiting_magnitude_50"),
                limiting_mag_90=ps.get("limiting_magnitude_90"),
                median_snr=ps.get("median_snr"),
                median_background=ps.get("median_background"),
                n_stars=ps.get("n_stars", 0),
                n_quality=ps.get("n_quality", 0),
            )

        # Extract detections, both point sources and confirmed streaks
        detections: list[Detection] = []
        if frame.detections and frame.detections.detections:
            for det in frame.detections.detections:
                detections.append(
                    Detection(
                        kind=det.detection_type,
                        x=det.x,
                        y=det.y,
                        snr=det.snr,
                        fwhm_pixels=det.pixel_fwhm,
                        ra=det.ra,
                        dec=det.dec,
                        angle_deg=det.angle_deg,
                        length_pixels=det.length_pixels,
                        rate_pixels_per_sec=det.rate_pixels_per_sec,
                        rate_arcsec_per_sec=det.rate_arcsec_per_sec,
                        flux=det.flux,
                        flux_err=det.flux_err,
                        instrumental_mag=det.instrumental_magnitude,
                        calibrated_magnitudes=det.calibrated_magnitudes,
                        magnitude_errs=det.magnitude_errs,
                    )
                )

        # Extract streak candidates (unconfirmed)
        if frame.streak_candidates:
            for c in frame.streak_candidates:
                detections.append(
                    Detection(
                        kind="streak_candidate",
                        x=c.x,
                        y=c.y,
                        snr=c.peak_snr,
                        angle_deg=c.angle_deg,
                        length_pixels=c.length_pixels,
                        width_pixels=c.width_pixels,
                        ra=c.ra,
                        dec=c.dec,
                        rate_pixels_per_sec=c.rate_pixels_per_sec,
                        rate_arcsec_per_sec=c.rate_arcsec_per_sec,
                        flux=c.flux,
                        flux_err=c.flux_err,
                        instrumental_mag=c.instrumental_magnitude,
                        calibrated_magnitudes=c.calibrated_magnitudes,
                        magnitude_errs=c.magnitude_errs,
                    )
                )

        # Convert FWHM to arcsec if plate solve succeeded
        median_fwhm_arcsec = None
        std_fwhm_arcsec = None
        if pixel_scale_arcsec is not None and median_fwhm_pixels is not None:
            median_fwhm_arcsec = median_fwhm_pixels * pixel_scale_arcsec
        if pixel_scale_arcsec is not None and std_fwhm_pixels is not None:
            std_fwhm_arcsec = std_fwhm_pixels * pixel_scale_arcsec

        exposure = (
            frame.frame_metadata.exposure_time_seconds if frame.frame_metadata else None
        )
        if exposure is None:
            exposure = image.header.get("EXPTIME")

        return SenpaiResult(
            file_path=inp.file_path,
            timestamp=timestamp,
            track_mode=track_mode,
            task_id=inp.task_id,
            frame_num=inp.frame_num,
            frame_count=inp.frame_count,
            from_sequence=from_sequence,
            exposure_time_seconds=exposure,
            n_sources=len(detections),
            median_fwhm_pixels=median_fwhm_pixels,
            std_fwhm_pixels=std_fwhm_pixels,
            pixel_scale_arcsec=pixel_scale_arcsec,
            median_fwhm_arcsec=median_fwhm_arcsec,
            std_fwhm_arcsec=std_fwhm_arcsec,
            solved=solved,
            detections=detections,
            photometry=photometry,
        )
