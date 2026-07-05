# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from senpai.core.config import initialize_config
from senpai.engine.models.images import ProcessedFitsImage
from senpai.engine.processing.collect import final_plots, process_senpai_collect
from sensorkit.senpai.models import Detection, Photometry, SenpaiResult


class SenpaiPipeline:
    """Uses the `astro-senpai` unified collect pipeline."""

    def __init__(self, senpai_config: str, senpai_output_dir: str):
        config = initialize_config(Path(senpai_config))
        if senpai_output_dir is not None:
            config.runtime.output_dir = Path(senpai_output_dir)
        self.config = config

    def process_frame(self, fits_data: bytes, file_path: str | Path) -> SenpaiResult:  # noqa: C901
        """Analyze a FITS frame via SENPAI's unified collect pipeline.

        Parameters:
            fits_data: Raw FITS file bytes.
            file_path: Path to the FITS file on disk.

        Returns:
            SenpaiResult
        """

        file_path = str(file_path)

        # Load FITS image
        fits_image = ProcessedFitsImage.from_file_bytes(fits_data, file_path=file_path)

        # Run the unified collect pipeline
        senpai_run = process_senpai_collect([fits_image])

        # Generate annotated plots (WCS overlay, detections, etc.)
        final_plots(senpai_run, Path(self.config.runtime.output_dir))

        # Determine which track mode produced the frame
        if senpai_run.sidereal_frames:
            frame = senpai_run.sidereal_frames[0]
            track_mode = "SIDEREAL"
        elif senpai_run.rate_track_frames:
            frame = senpai_run.rate_track_frames[0]
            track_mode = "RATE"
        else:
            logger.warning("No frames returned from collect pipeline")
            return SenpaiResult(
                file_path=file_path,
                timestamp=datetime.fromisoformat(fits_image.header["DATE-OBS"]).replace(
                    tzinfo=UTC
                ),
                track_mode="UNKNOWN",
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
                import warnings

                wcs_astropy = frame.starfield.wcs.to_astropy_wcs()
                img_w = int(fits_image.header.get("NAXIS1", 0))
                img_h = int(fits_image.header.get("NAXIS2", 0))
                center_x, center_y = img_w / 2.0, img_h / 2.0
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    sky = wcs_astropy.pixel_to_world(center_x, center_y)
                wcs_ra, wcs_dec = sky.ra.deg, sky.dec.deg
                hdr_ra = fits_image.header.get("RA")
                hdr_dec = fits_image.header.get("DEC")
                if hdr_ra is not None:
                    dra = (wcs_ra - hdr_ra) * 3600.0  # arcsec
                    ddec = (wcs_dec - hdr_dec) * 3600.0
                    import math

                    cos_dec = math.cos(math.radians(hdr_dec))
                    sep = math.sqrt((dra * cos_dec) ** 2 + ddec**2)
                    # Also log the CRPIX values
                    crpix1 = wcs_astropy.wcs.crpix[0]
                    crpix2 = wcs_astropy.wcs.crpix[1]
                    logger.info(
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

        # Extract streak candidates (unconfirmed in single-frame mode)
        if frame.streak_candidates:
            for c in frame.streak_candidates:
                detections.append(
                    Detection(
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

        return SenpaiResult(
            file_path=file_path,
            timestamp=datetime.fromisoformat(fits_image.header["DATE-OBS"]).replace(tzinfo=UTC),
            track_mode=track_mode,
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
