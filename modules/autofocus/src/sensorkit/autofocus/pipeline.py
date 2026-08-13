from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from loguru import logger
from scipy.optimize import curve_fit


@dataclass
class SourceMeasurement:
    x: float
    y: float
    fwhm_px: float
    e1: float
    e2: float
    snr: float


@dataclass
class VCurveFitResult:
    best_position: float
    best_fwhm: float
    slope: float
    r_squared: float


def compute_quadrupole_moments(
    image_data: np.ndarray,
    detections: list,
    fwhm_px: float,
) -> list[SourceMeasurement]:
    """Compute quadrupole moments for each detected source.

    For each star, extracts a cutout, computes weighted second moments,
    and derives ellipticity components (e1, e2).

    Args:
        image_data: 2D image array.
        detections: List of detected sources with x, y attributes.
        fwhm_px: Estimated FWHM in pixels for Gaussian weighting.

    Returns:
        List of SourceMeasurement with quadrupole ellipticities.
    """
    sources: list[SourceMeasurement] = []
    half_box = max(int(4 * fwhm_px), 8)
    sigma = max(fwhm_px, 1.0)
    ny, nx = image_data.shape

    for det in detections:
        xc = det.x
        yc = det.y
        # SENPAI's detection-stage stars carry snr=None (only the catalog-matched list has
        # real SNRs) — coerce to 0 so downstream weighting floors it rather than crashing.
        snr = float(getattr(det, "snr", 0) or 0.0)

        # Bounds check
        x0 = max(int(xc - half_box), 0)
        x1 = min(int(xc + half_box), nx)
        y0 = max(int(yc - half_box), 0)
        y1 = min(int(yc + half_box), ny)

        if x1 - x0 < 3 or y1 - y0 < 3:
            continue

        cutout = image_data[y0:y1, x0:x1].astype(np.float64)

        # Subtract local background (median of edge pixels)
        edge = np.concatenate([cutout[0, :], cutout[-1, :], cutout[:, 0], cutout[:, -1]])
        bg = np.median(edge)
        cutout = cutout - bg
        cutout = np.maximum(cutout, 0.0)

        # Coordinate grids relative to star center
        ys, xs = np.mgrid[y0:y1, x0:x1]
        dx = xs - xc
        dy = ys - yc
        r2 = dx * dx + dy * dy

        # Adaptive Gaussian weight: w = I(x,y) * exp(-r^2 / 2*sigma^2)
        gauss = np.exp(-r2 / (2.0 * sigma * sigma))
        w = cutout * gauss

        w_sum = w.sum()
        if w_sum <= 0:
            continue

        # Weighted second moments
        qxx = np.sum(w * dx * dx) / w_sum
        qyy = np.sum(w * dy * dy) / w_sum
        qxy = np.sum(w * dx * dy) / w_sum

        trace = qxx + qyy
        if trace <= 0:
            continue

        # Ellipticity components
        e1 = (qxx - qyy) / trace
        e2 = 2.0 * qxy / trace

        # Estimate FWHM from moments
        source_fwhm = 2.355 * math.sqrt(trace / 2.0)

        sources.append(
            SourceMeasurement(
                x=xc,
                y=yc,
                fwhm_px=source_fwhm,
                e1=e1,
                e2=e2,
                snr=snr,
            )
        )

    return sources


def determine_defocus_sign(
    sources: list[SourceMeasurement],
    image_center: tuple[float, float],
    threshold: float = 0.02,
) -> int:
    """Determine defocus direction from radial ellipticity pattern.

    Intra-focal defocus produces positive radial ellipticity (stars
    elongated radially), extra-focal produces negative (tangential).

    The signal comes from FIELD-DEPENDENT astigmatism and grows as the SQUARE of field
    radius — a frame-center star carries ~zero direction information but full measurement
    noise (per-star ellipticity scatter is ~0.1 even for bright stars). So the inner field
    is excluded outright, and the rest is weighted by (r/r_max)^2 — matched to the signal's
    field law — times an SNR term CAPPED so a single bright star (typically the on-axis
    target, exactly where the signal vanishes) cannot dominate the mean.

    Args:
        sources: Source measurements with quadrupole ellipticities.
        image_center: (x, y) center of the image.
        threshold: Minimum |leverage-weighted mean radial ellipticity| to call a direction.
            Depends on how much field-dependent aberration the optics have (a
            constant-orientation astigmatism averages toward zero over a full field), so it
            is tunable per sensor.

    Returns:
        +1 for intra-focal, -1 for extra-focal, 0 for indeterminate.
    """
    if len(sources) < 3:
        return 0

    cx, cy = image_center
    r_max = math.hypot(cx, cy)  # center to corner
    if r_max <= 0:
        return 0

    weighted_er_sum = 0.0
    weight_sum = 0.0

    for src in sources:
        dx = src.x - cx
        dy = src.y - cy
        r = math.hypot(dx, dy)

        # Inner field: no signal (it grows as r^2), pure noise — and the position angle is
        # ill-defined near the center anyway.
        if r < 0.15 * r_max:
            continue

        # Position angle from center to star
        theta = math.atan2(dy, dx)

        # Radial ellipticity: project (e1, e2) onto the radial direction
        e_r = src.e1 * math.cos(2.0 * theta) + src.e2 * math.sin(2.0 * theta)

        # Leverage weighting matched to the r^2 field law, with capped SNR
        w = (r / r_max) ** 2 * min(max(src.snr, 1.0), 20.0)
        weighted_er_sum += e_r * w
        weight_sum += w

    if weight_sum <= 0:
        return 0

    mean_er = weighted_er_sum / weight_sum

    # Threshold to declare a direction
    if mean_er > threshold:
        return 1  # intra-focal
    elif mean_er < -threshold:
        return -1  # extra-focal
    else:
        return 0  # indeterminate


def compute_defocus_sign(
    image_data: np.ndarray,
    image_shape: tuple[int, ...],
    detections: list,
    median_fwhm_px: float,
    threshold: float = 0.02,
) -> int:
    """Compute defocus sign from image data and point-source detections.

    Args:
        image_data: 2D image array.
        image_shape: Shape of the image (height, width).
        detections: Objects with x, y, snr attributes.
        median_fwhm_px: Median FWHM in pixels for Gaussian weighting.
        threshold: Minimum |mean radial ellipticity| to call a direction (see
            determine_defocus_sign).

    Returns:
        +1 for intra-focal, -1 for extra-focal, 0 for indeterminate.
    """
    fwhm = median_fwhm_px if median_fwhm_px > 0 else 3.0
    sources = compute_quadrupole_moments(image_data, detections, fwhm)
    if not sources:
        return 0
    image_center = (image_shape[1] / 2.0, image_shape[0] / 2.0)
    return determine_defocus_sign(sources, image_center, threshold)


def fit_vcurve(data: list[tuple[float, float]]) -> VCurveFitResult:
    """Fit a V-curve (parabola in FWHM^2 vs focus position).

    Model: FWHM^2(p) = a * (p - p_opt)^2 + b
    where b = FWHM_best^2

    Args:
        data: List of (focus_position, median_fwhm) tuples.

    Returns:
        VCurveFitResult with optimal position, best FWHM, slope, and R^2.
    """
    positions = np.array([d[0] for d in data])
    fwhm_values = np.array([d[1] for d in data])
    fwhm_sq = fwhm_values**2

    def parabola(p, a, p_opt, b):
        return a * (p - p_opt) ** 2 + b

    # Initial guesses
    idx_min = np.argmin(fwhm_sq)
    p0_opt = positions[idx_min]
    b0 = fwhm_sq[idx_min]
    # Rough slope from spread
    pos_range = positions.max() - positions.min()
    fwhm_sq_range = fwhm_sq.max() - fwhm_sq.min()
    a0 = fwhm_sq_range / (pos_range**2 / 4.0) if pos_range > 0 else 1e-6

    try:
        popt, _ = curve_fit(
            parabola,
            positions,
            fwhm_sq,
            p0=[a0, p0_opt, b0],
            bounds=([0, positions.min(), 0], [np.inf, positions.max(), np.inf]),
        )
    except RuntimeError:
        logger.warning("V-curve fit did not converge, using minimum FWHM point")
        return VCurveFitResult(
            best_position=p0_opt,
            best_fwhm=math.sqrt(b0),
            slope=a0,
            r_squared=0.0,
        )

    a_fit, p_opt_fit, b_fit = popt

    # Compute R^2
    ss_res = np.sum((fwhm_sq - parabola(positions, *popt)) ** 2)
    ss_tot = np.sum((fwhm_sq - np.mean(fwhm_sq)) ** 2)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # The fitted vertex depth (b) is poorly constrained with steep arms + few bottom points and
    # can pin to the b>=0 fit bound; report the best MEASURED FWHM as the achievable best focus.
    fwhm_best = max(math.sqrt(max(b_fit, 0.0)), float(fwhm_values.min()))

    return VCurveFitResult(
        best_position=p_opt_fit,
        best_fwhm=fwhm_best,
        slope=a_fit,
        r_squared=r_squared,
    )
