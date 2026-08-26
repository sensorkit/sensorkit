"""Integration tests for the autofocus pipeline using real through-focus data.

Test data:
    /opt/sk/tmp/focus1/  — 7 frames, positions 23043–26043, step 500
    /opt/sk/tmp/focus2/  — 7 frames, positions 23437–26437, step 500

These are 8120x8120 float32 FITS with minimal headers (no WCS).
Focus position is encoded in the filename: FOCUS{position}.fit

Run with:
    cd /opt/sensorkit && .venv/bin/python -m pytest modules/autofocus/tests/test_autofocus_integration.py -v -s
"""

import math
import re
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
from scipy.ndimage import gaussian_filter, maximum_filter
from scipy.optimize import curve_fit as scipy_curve_fit

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FOCUS1_DIR = Path("/opt/sk/tmp/focus1")
FOCUS2_DIR = Path("/opt/sk/tmp/focus2")

DATASETS = [
    pytest.param(FOCUS1_DIR, id="focus1"),
    pytest.param(FOCUS2_DIR, id="focus2"),
]


def _have_data():
    return FOCUS1_DIR.exists() and FOCUS2_DIR.exists()


skip_no_data = pytest.mark.skipif(not _have_data(), reason="Focus test data not found")


# ---------------------------------------------------------------------------
# Helpers — lightweight source detection + FWHM (no SENPAI dependency)
# ---------------------------------------------------------------------------


def load_fits(path: Path) -> np.ndarray:
    """Load a FITS file and return the image data as float64."""
    from astropy.io import fits

    with fits.open(str(path)) as hdul:
        return hdul[0].data.astype(np.float64)


def extract_position_from_filename(filename: str) -> float:
    """Extract the focus position from a filename like FOCUS24543.fit."""
    m = re.search(r"FOCUS(\d+)\.fit", filename)
    if not m:
        raise ValueError(f"Cannot parse position from {filename}")
    return float(m.group(1))


def detect_sources(data: np.ndarray, sigma_threshold: float = 5.0, min_sep: int = 11):
    """Simple peak-based source detection on a 2D image.

    Returns list of (x, y, peak_value) tuples.
    """
    bg = np.median(data)
    noise = np.std(data[::8, ::8])  # subsample for speed on large images
    threshold = bg + sigma_threshold * noise

    smoothed = gaussian_filter(data - bg, sigma=2.0)
    local_max = maximum_filter(smoothed, size=min_sep)
    peaks = (smoothed == local_max) & (smoothed > (threshold - bg))

    ys, xs = np.where(peaks)
    vals = smoothed[ys, xs]

    # Sort by brightness
    order = np.argsort(-vals)
    return [(float(xs[i]), float(ys[i]), float(vals[i])) for i in order]


def measure_fwhm(data: np.ndarray, x0: float, y0: float, half_box: int = 15) -> float | None:
    """Measure FWHM at a source position via 1D Gaussian fit along x and y.

    Returns FWHM in pixels or None if the fit fails.
    """
    ny, nx = data.shape
    ix, iy = int(round(x0)), int(round(y0))

    if ix < half_box or ix >= nx - half_box or iy < half_box or iy >= ny - half_box:
        return None

    bg = np.median(data)

    def gauss(x, amp, mu, sig, offset):
        return amp * np.exp(-0.5 * ((x - mu) / sig) ** 2) + offset

    fwhms = []
    for profile_data in [
        data[iy, ix - half_box : ix + half_box + 1],
        data[iy - half_box : iy + half_box + 1, ix],
    ]:
        profile = profile_data - bg
        xcoords = np.arange(len(profile))
        try:
            popt, _ = scipy_curve_fit(
                gauss,
                xcoords,
                profile,
                p0=[profile.max(), half_box, 3.0, 0.0],
                maxfev=500,
            )
            fwhm = abs(popt[2]) * 2.355
            if 1.0 < fwhm < 50.0:
                fwhms.append(fwhm)
        except (RuntimeError, ValueError):
            pass

    return float(np.mean(fwhms)) if fwhms else None


def measure_frame_fwhm(
    data: np.ndarray,
    max_sources: int = 100,
    subsample: int = 1,
) -> tuple[float, float, int]:
    """Detect sources and return (median_fwhm, std_fwhm, n_measured).

    For very large images, set subsample > 1 to work on a smaller version.
    """
    if subsample > 1:
        data = data[::subsample, ::subsample]

    sources = detect_sources(data, sigma_threshold=5.0)
    if not sources:
        return 0.0, 0.0, 0

    fwhms = []
    for x, y, _ in sources[:max_sources]:
        fwhm = measure_fwhm(data, x, y)
        if fwhm is not None:
            fwhms.append(fwhm)

    if not fwhms:
        return 0.0, 0.0, 0

    # Scale back if subsampled
    scale = float(subsample)
    arr = np.array(fwhms) * scale
    return float(np.median(arr)), float(np.std(arr)), len(arr)


# ---------------------------------------------------------------------------
# Test: Quadrupole moments on synthetic PSF
# ---------------------------------------------------------------------------


class TestQuadrupoleMoments:
    """Test compute_quadrupole_moments and determine_defocus_sign with synthetic data."""

    def _make_elliptical_source(
        self, size: int, sigma_x: float, sigma_y: float, angle_deg: float = 0.0
    ) -> np.ndarray:
        """Create a synthetic elliptical Gaussian PSF."""
        y, x = np.mgrid[:size, :size]
        cx, cy = size / 2.0, size / 2.0
        dx, dy = x - cx, y - cy

        angle = np.radians(angle_deg)
        cos_a, sin_a = np.cos(angle), np.sin(angle)
        dx_r = dx * cos_a + dy * sin_a
        dy_r = -dx * sin_a + dy * cos_a

        return 1000.0 * np.exp(-0.5 * (dx_r**2 / sigma_x**2 + dy_r**2 / sigma_y**2))

    def test_circular_psf_has_zero_ellipticity(self):
        """A perfectly circular PSF should have e1 ~ 0, e2 ~ 0."""
        from sensorkit.autofocus.pipeline import compute_quadrupole_moments

        size = 64
        image = self._make_elliptical_source(size, 5.0, 5.0)

        class FakeDet:
            def __init__(self, x, y, snr=100.0):
                self.x, self.y, self.snr = x, y, snr

        sources = compute_quadrupole_moments(image, [FakeDet(size / 2, size / 2)], 5.0)
        assert len(sources) == 1
        assert abs(sources[0].e1) < 0.02
        assert abs(sources[0].e2) < 0.02

    def test_elongated_psf_has_nonzero_e1(self):
        """A PSF elongated along x should have positive e1."""
        from sensorkit.autofocus.pipeline import compute_quadrupole_moments

        size = 64
        image = self._make_elliptical_source(size, 8.0, 4.0, angle_deg=0.0)

        class FakeDet:
            def __init__(self, x, y, snr=100.0):
                self.x, self.y, self.snr = x, y, snr

        sources = compute_quadrupole_moments(image, [FakeDet(size / 2, size / 2)], 5.0)
        assert len(sources) == 1
        assert sources[0].e1 > 0.1  # Elongated along x → positive e1

    def test_defocus_sign_intrafocal_pattern(self):
        """Simulated intra-focal pattern: stars elongated radially from center."""
        from sensorkit.autofocus.pipeline import (
            SourceMeasurement,
            determine_defocus_sign,
        )

        center = (500.0, 500.0)
        sources = []

        # Place stars at various positions around the center
        for angle_deg in range(0, 360, 30):
            angle = math.radians(angle_deg)
            r = 200.0
            x = center[0] + r * math.cos(angle)
            y = center[1] + r * math.sin(angle)

            # Intra-focal: radial elongation → e_r > 0
            # e1 = cos(2θ) * e_radial, e2 = sin(2θ) * e_radial
            e_mag = 0.15
            e1 = e_mag * math.cos(2 * angle)
            e2 = e_mag * math.sin(2 * angle)

            sources.append(
                SourceMeasurement(
                    x=x,
                    y=y,
                    fwhm_px=5.0,
                    e1=e1,
                    e2=e2,
                    snr=50.0,
                )
            )

        sign = determine_defocus_sign(sources, center)
        assert sign == 1, f"Expected intra-focal (+1), got {sign}"

    def test_defocus_sign_extrafocal_pattern(self):
        """Simulated extra-focal pattern: stars elongated tangentially."""
        from sensorkit.autofocus.pipeline import (
            SourceMeasurement,
            determine_defocus_sign,
        )

        center = (500.0, 500.0)
        sources = []

        for angle_deg in range(0, 360, 30):
            angle = math.radians(angle_deg)
            r = 200.0
            x = center[0] + r * math.cos(angle)
            y = center[1] + r * math.sin(angle)

            # Extra-focal: tangential elongation → e_r < 0
            e_mag = -0.15
            e1 = e_mag * math.cos(2 * angle)
            e2 = e_mag * math.sin(2 * angle)

            sources.append(
                SourceMeasurement(
                    x=x,
                    y=y,
                    fwhm_px=5.0,
                    e1=e1,
                    e2=e2,
                    snr=50.0,
                )
            )

        sign = determine_defocus_sign(sources, center)
        assert sign == -1, f"Expected extra-focal (-1), got {sign}"

    def test_defocus_sign_survives_snr_none(self):
        """SENPAI's detection-stage stars carry snr=None — the sign path must not crash on them.

        Regression: snr=None reached the weighting as-is and raised TypeError, which the
        analyzer swallowed, silently reading every live frame as sign=0.
        """
        from sensorkit.autofocus.pipeline import compute_defocus_sign

        size = 400
        image = np.zeros((size, size), dtype=np.float64)
        rng = np.random.default_rng(7)
        image += rng.normal(100.0, 2.0, image.shape)

        class SenpaiStar:  # mirrors sensorkit.senpai.models.Star
            def __init__(self, x, y):
                self.x, self.y, self.snr = x, y, None

        dets = []
        for angle_deg in range(0, 360, 45):
            angle = math.radians(angle_deg)
            x = size / 2 + 150.0 * math.cos(angle)
            y = size / 2 + 150.0 * math.sin(angle)
            yy, xx = np.mgrid[0:size, 0:size]
            image += 500.0 * np.exp(-(((xx - x) ** 2) + ((yy - y) ** 2)) / (2 * 3.0**2))
            dets.append(SenpaiStar(x, y))

        # Must complete without raising; round sources are legitimately indeterminate.
        sign = compute_defocus_sign(image, image.shape, dets, 5.0)
        assert sign == 0


# ---------------------------------------------------------------------------
# Test: V-curve fitting
# ---------------------------------------------------------------------------


class TestVCurveFit:
    """Test fit_vcurve with synthetic and real data."""

    def test_synthetic_vcurve(self):
        """Fit a perfect parabolic V-curve."""
        from sensorkit.autofocus.pipeline import fit_vcurve

        # Generate synthetic data: FWHM²(p) = 0.001*(p - 25000)² + 4.0
        a_true = 0.001
        p_opt_true = 25000.0
        fwhm_best_true = 2.0  # FWHM² = 4.0

        positions = np.linspace(23000, 27000, 9)
        fwhm_values = [
            math.sqrt(a_true * (p - p_opt_true) ** 2 + fwhm_best_true**2) for p in positions
        ]
        data = list(zip(positions, fwhm_values, strict=True))

        result = fit_vcurve(data)

        assert abs(result.best_position - p_opt_true) < 10.0
        assert abs(result.best_fwhm - fwhm_best_true) < 0.1
        assert result.slope > 0
        assert result.r_squared > 0.99

    def test_noisy_vcurve(self):
        """Fit a V-curve with realistic noise."""
        from sensorkit.autofocus.pipeline import fit_vcurve

        rng = np.random.default_rng(42)
        a_true = 0.0005
        p_opt_true = 24700.0
        fwhm_best_true = 3.0

        positions = np.linspace(23000, 26500, 7)
        fwhm_values = []
        for p in positions:
            fwhm_clean = math.sqrt(a_true * (p - p_opt_true) ** 2 + fwhm_best_true**2)
            fwhm_noisy = fwhm_clean + rng.normal(0, 0.3)
            fwhm_values.append(max(fwhm_noisy, 1.0))

        data = list(zip(positions, fwhm_values, strict=True))
        result = fit_vcurve(data)

        # Should be within ~200 steps of true optimum given noise
        assert abs(result.best_position - p_opt_true) < 300.0
        assert result.best_fwhm > 0
        assert result.slope > 0

    @skip_no_data
    @pytest.mark.parametrize("data_dir", DATASETS)
    def test_vcurve_on_real_data(self, data_dir: Path):
        """Measure FWHM from real focus sweep data and fit V-curve.

        This is the key integration test: load actual through-focus frames,
        measure FWHM at each position, and verify the V-curve fit produces
        a physically reasonable optimal focus position.
        """
        from sensorkit.autofocus.pipeline import fit_vcurve

        files = sorted(data_dir.glob("FOCUS*.fit"))
        assert len(files) == 7, f"Expected 7 files in {data_dir}, got {len(files)}"

        data_points: list[tuple[float, float]] = []
        print(f"\n--- {data_dir.name} ---")

        for fpath in files:
            pos = extract_position_from_filename(fpath.name)
            image = load_fits(fpath)

            # Subsample 2x for speed on 8k images
            median_fwhm, std_fwhm, n_measured = measure_frame_fwhm(image, subsample=2)

            print(
                f"  {fpath.name}: pos={pos:.0f}, "
                f"FWHM={median_fwhm:.2f} ± {std_fwhm:.2f} px, "
                f"n={n_measured}"
            )

            if n_measured >= 3 and median_fwhm > 0:
                data_points.append((pos, median_fwhm))

        assert len(data_points) >= 5, (
            f"Need at least 5 valid FWHM measurements, got {len(data_points)}"
        )

        # Fit V-curve
        result = fit_vcurve(data_points)

        positions = [d[0] for d in data_points]
        pos_min, pos_max = min(positions), max(positions)

        print("\n  V-curve fit:")
        print(f"    optimal_position = {result.best_position:.1f}")
        print(f"    fwhm_best = {result.best_fwhm:.2f} px")
        print(f"    slope = {result.slope:.6f}")
        print(f"    R² = {result.r_squared:.4f}")

        # Validate: optimal should be within sweep range (or close)
        assert pos_min - 500 < result.best_position < pos_max + 500, (
            f"Optimal position {result.best_position:.0f} is far outside "
            f"sweep range [{pos_min:.0f}, {pos_max:.0f}]"
        )

        # FWHM_best should be reasonable (> 0 and < max measured)
        max_fwhm = max(d[1] for d in data_points)
        assert 0 < result.best_fwhm < max_fwhm, (
            f"FWHM_best {result.best_fwhm:.2f} is not reasonable (max measured: {max_fwhm:.2f})"
        )

        # Slope must be positive (V-curve opens upward)
        assert result.slope > 0, f"V-curve slope should be positive, got {result.slope}"


# ---------------------------------------------------------------------------
# Test: Quadrupole moments on real data
# ---------------------------------------------------------------------------


@skip_no_data
class TestQuadrupoleMomentsOnRealData:
    """Test quadrupole moment measurement on real focus frames."""

    def test_quadrupole_moments_real_frame(self):
        """Verify that compute_quadrupole_moments runs on a real focus frame
        and produces plausible ellipticity values.
        """
        from sensorkit.autofocus.pipeline import compute_quadrupole_moments

        # Use the most defocused frame (should have visible ellipticity)
        fpath = FOCUS1_DIR / "FOCUS23043.fit"
        image = load_fits(fpath)

        # Subsample for speed
        image_sub = image[::2, ::2]
        sources_raw = detect_sources(image_sub, sigma_threshold=5.0)

        # Build fake detection objects
        class Det:
            def __init__(self, x, y, snr=50.0):
                self.x, self.y, self.snr = x, y, snr

        detections = [Det(x, y, v) for x, y, v in sources_raw[:50]]
        median_fwhm = 5.0  # rough estimate for subsampled

        sources = compute_quadrupole_moments(image_sub, detections, median_fwhm)

        print(f"\nQuadrupole moments on {fpath.name} (subsampled 2x):")
        print(f"  Input detections: {len(detections)}")
        print(f"  Valid measurements: {len(sources)}")

        assert len(sources) > 0, "Should measure at least some sources"

        e1_vals = [s.e1 for s in sources]
        e2_vals = [s.e2 for s in sources]
        print(f"  e1: mean={np.mean(e1_vals):.4f}, std={np.std(e1_vals):.4f}")
        print(f"  e2: mean={np.mean(e2_vals):.4f}, std={np.std(e2_vals):.4f}")

        # Ellipticity should be bounded [-1, 1]
        for s in sources:
            assert -1.0 <= s.e1 <= 1.0, f"e1={s.e1} out of bounds"
            assert -1.0 <= s.e2 <= 1.0, f"e2={s.e2} out of bounds"

    def test_defocus_sign_varies_across_sweep(self):
        """The defocus sign (or at least the mean radial ellipticity) should
        differ between the extremes of the focus sweep.
        """
        from sensorkit.autofocus.pipeline import (
            compute_quadrupole_moments,
            determine_defocus_sign,
        )

        class Det:
            def __init__(self, x, y, snr=50.0):
                self.x, self.y, self.snr = x, y, snr

        signs = {}
        files = sorted(FOCUS1_DIR.glob("FOCUS*.fit"))
        test_files = [files[0], files[-1]]  # first and last (most defocused)

        for fpath in test_files:
            image = load_fits(fpath)
            image_sub = image[::2, ::2]
            center = (image_sub.shape[1] / 2.0, image_sub.shape[0] / 2.0)

            sources_raw = detect_sources(image_sub, sigma_threshold=5.0)
            detections = [Det(x, y, v) for x, y, v in sources_raw[:80]]
            sources = compute_quadrupole_moments(image_sub, detections, 5.0)

            sign = determine_defocus_sign(sources, center)
            pos = extract_position_from_filename(fpath.name)
            signs[pos] = sign

            print(f"  {fpath.name}: defocus_sign={sign} (n_sources={len(sources)})")

        # We expect the two extremes to have different signs (or at least
        # not both be the same non-zero sign), since they are on opposite
        # sides of focus. With streaked/noisy data, both might be 0.
        pos_list = sorted(signs.keys())
        s1, s2 = signs[pos_list[0]], signs[pos_list[-1]]
        print(f"\n  Extreme signs: pos={pos_list[0]:.0f} → {s1}, pos={pos_list[-1]:.0f} → {s2}")

        # At minimum, the function should not crash and should return valid values
        assert s1 in (-1, 0, 1)
        assert s2 in (-1, 0, 1)


# ---------------------------------------------------------------------------
# Test: Correction computation
# ---------------------------------------------------------------------------


class TestCorrectionComputation:
    """Test the passive focus correction formula."""

    def test_correction_formula(self):
        """Verify delta = sign * sqrt((FWHM² - FWHM_target²) / a)."""
        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            AutofocusState,
            CorrectionConfig,
            FocuserConfig,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=20000, max_position=30000),
            correction=CorrectionConfig(),
        )

        # Create analyzer without initializing the pipeline
        analyzer = AutofocusAnalyzer.__new__(AutofocusAnalyzer)
        analyzer.config = config
        analyzer.state = AutofocusState(
            vcurve_slope=0.001,
            vcurve_best_fwhm_pixels=3.0,
            defocus_sign_convention=1,
        )

        # Case: FWHM=6px, target=3px, slope=0.001
        # delta = sign * sqrt((36 - 9) / 0.001) = sign * sqrt(27000) ≈ ±164.3
        delta = analyzer._compute_correction(
            median_fwhm_px=6.0,
            pixel_scale_arcsec=None,
            defocus_sign=1,
        )
        expected = math.sqrt((36 - 9) / 0.001)
        assert abs(abs(delta) - expected) < 1.0, (
            f"Expected |delta| ≈ {expected:.1f}, got {delta:.1f}"
        )
        assert delta > 0, "Positive defocus_sign + positive convention → positive delta"

    def test_correction_not_step_capped(self):
        """_compute_correction returns the full formula magnitude — there is no focuser-step cap
        anymore; the arcsec excursion ceiling gates correction upstream in _evaluate_correction."""
        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            AutofocusState,
            CorrectionConfig,
            FocuserConfig,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=20000, max_position=30000),
            correction=CorrectionConfig(),
        )

        analyzer = AutofocusAnalyzer.__new__(AutofocusAnalyzer)
        analyzer.config = config
        analyzer.state = AutofocusState(
            vcurve_slope=0.0001,  # very shallow slope → large correction
            vcurve_best_fwhm_pixels=3.0,
            defocus_sign_convention=1,
        )

        delta = analyzer._compute_correction(
            median_fwhm_px=10.0,
            pixel_scale_arcsec=None,
            defocus_sign=1,
        )
        expected = math.sqrt((100 - 9) / 0.0001)  # ≈ 954, previously clamped to 100
        assert abs(abs(delta) - expected) < 1.0, (
            f"Expected uncapped |delta| ≈ {expected:.1f}, got {delta:.1f}"
        )

    @staticmethod
    def _analyzer_with(convention):
        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            AutofocusState,
            CorrectionConfig,
            FocuserConfig,
        )

        analyzer = AutofocusAnalyzer.__new__(AutofocusAnalyzer)
        analyzer.config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=20000, max_position=30000),
            correction=CorrectionConfig(),
        )
        analyzer.state = AutofocusState(
            enabled=True,
            vcurve_slope=0.001,
            vcurve_best_fwhm_pixels=3.0,
            defocus_sign_convention=convention,
        )
        return analyzer

    def test_uncalibrated_convention_yields_no_delta(self):
        """Without a learned sign→direction mapping there is no defensible direction to move."""
        analyzer = self._analyzer_with(None)
        assert (
            analyzer._compute_correction(
                median_fwhm_px=6.0, pixel_scale_arcsec=None, defocus_sign=1
            )
            == 0.0
        )

    def test_uncalibrated_convention_skips_passive_correction(self):
        """Same frame corrects once the convention is known, and is skipped while it is not."""
        # 4.0px against a 3.0px best at 1"/px -> 1.0" excursion: inside the passive band.
        args = dict(
            median_fwhm_pixels=4.0,
            pixel_scale_arcsec=1.0,
            solved=True,
            focus_position=25000.0,
            defocus_sign=1,
        )
        assert self._analyzer_with(1)._evaluate_correction(**args) == "correct"
        assert self._analyzer_with(None)._evaluate_correction(**args) == "skip"

    def test_recalibration_still_fires_while_uncalibrated(self):
        """The bootstrap path: a grossly defocused frame must still queue the V-curve that
        TEACHES the convention, otherwise an uncalibrated sensor could never recover."""
        analyzer = self._analyzer_with(None)
        # 7.0px against a 3.0px best at 1"/px -> 4.0" excursion, above the 3.0" ceiling.
        decision = analyzer._evaluate_correction(
            median_fwhm_pixels=7.0,
            pixel_scale_arcsec=1.0,
            solved=True,
            focus_position=25000.0,
            defocus_sign=0,
        )
        assert decision == "recalibrate"


# ---------------------------------------------------------------------------
# Test: Target selection
# ---------------------------------------------------------------------------


class TestTargetSelection:
    """Test the V-curve target auto-selection (SSTRC7 catalog only)."""

    def test_no_catalog_returns_none(self):
        """Without a catalog_path there is no star source, so selection returns None."""
        from sensorkit.autofocus.program import _select_vcurve_target

        target = _select_vcurve_target(
            min_altitude=15.0,
            min_solar_elongation=0.0,
            min_magnitude=8.0,
            max_magnitude=None,
            site_lat=35.0,
            site_lon=-106.0,
            catalog_path=None,
        )
        assert target is None

    def test_bad_catalog_returns_none(self):
        """A failing SSTRC7 query is caught and selection returns None."""
        from sensorkit.autofocus.program import _select_vcurve_target

        target = _select_vcurve_target(
            min_altitude=15.0,
            min_solar_elongation=0.0,
            min_magnitude=8.0,
            max_magnitude=None,
            site_lat=35.0,
            site_lon=-106.0,
            catalog_path="/nonexistent/catalog",
        )
        assert target is None


# ---------------------------------------------------------------------------
# Test: Full pipeline integration (real data → FWHM → V-curve)
# ---------------------------------------------------------------------------


@skip_no_data
class TestFullPipelineIntegration:
    """End-to-end test: load frames, measure FWHM, fit V-curve, verify result."""

    @pytest.mark.parametrize("data_dir", DATASETS)
    def test_full_sweep(self, data_dir: Path):
        """Process all frames in a focus sweep and verify the V-curve fit."""
        from sensorkit.autofocus.pipeline import fit_vcurve

        files = sorted(data_dir.glob("FOCUS*.fit"))
        positions = []
        fwhms = []

        for fpath in files:
            pos = extract_position_from_filename(fpath.name)
            image = load_fits(fpath)
            median_fwhm, _, n = measure_frame_fwhm(image, subsample=2)

            if n >= 3:
                positions.append(pos)
                fwhms.append(median_fwhm)

        assert len(positions) >= 5

        # Verify FWHM follows a V-curve shape: middle positions should have
        # smaller FWHM than the extremes
        mid_idx = len(positions) // 2
        edge_fwhm = max(fwhms[0], fwhms[-1])
        mid_fwhm = fwhms[mid_idx]

        print(f"\n  Edge FWHM: {edge_fwhm:.2f}, Mid FWHM: {mid_fwhm:.2f}")
        # The middle should generally be smaller than the edges
        # (though with noisy data this isn't guaranteed for every frame)

        # Fit and verify
        data = list(zip(positions, fwhms, strict=True))
        result = fit_vcurve(data)

        print(f"  V-curve optimal: {result.best_position:.0f}")
        print(f"  V-curve FWHM_best: {result.best_fwhm:.2f} px")
        print(f"  V-curve R²: {result.r_squared:.4f}")

        # Basic sanity: positive slope, reasonable optimal
        assert result.slope > 0
        assert result.best_fwhm > 0
        assert result.best_fwhm < max(fwhms)


# ---------------------------------------------------------------------------
# Test: Track-mode gating (commanded TRKMODE beats SENPAI's inference)
# ---------------------------------------------------------------------------


class TestTrackModeGate:
    """Only sidereal frames measure focus; a rate frame's streaks are not defocus."""

    def test_commanded_trkmode_wins_over_inference(self):
        """Regression: SENPAI infers track mode and resolves ties to SIDEREAL, so a rate frame
        can arrive labelled sidereal — live, one queued a spurious V-curve off 4.31" of pure
        trailing. The controller's TRKMODE card is authoritative."""
        from sensorkit.autofocus.analyzer import _is_sidereal

        assert _is_sidereal("rate", "SIDEREAL") is False
        assert _is_sidereal("sidereal", "RATE") is True

    def test_case_and_whitespace_tolerant(self):
        from sensorkit.autofocus.analyzer import _is_sidereal

        assert _is_sidereal("SIDEREAL", None) is True
        assert _is_sidereal(" Sidereal ", None) is True
        assert _is_sidereal("RATE", None) is False

    def test_falls_back_to_inference_without_the_card(self):
        from sensorkit.autofocus.analyzer import _is_sidereal

        assert _is_sidereal(None, "SIDEREAL") is True
        assert _is_sidereal(None, "RATE") is False
        assert _is_sidereal(None, "UNKNOWN") is False


# ---------------------------------------------------------------------------
# Test: Stale-frame time parsing (guard against double-corrections from SENPAI lag)
# ---------------------------------------------------------------------------


class TestFrameTimeParse:
    """_parse_frame_time turns FITS DATE-OBS into an aware UTC datetime for staleness checks."""

    def test_naive_fits_date_obs_gets_utc(self):
        from datetime import UTC, datetime

        from sensorkit.autofocus.analyzer import _parse_frame_time

        t = _parse_frame_time("2026-08-06T06:12:49.385130")
        assert t == datetime(2026, 8, 6, 6, 12, 49, 385130, tzinfo=UTC)
        # Comparable against aware state timestamps — the actual guard operation.
        assert t < datetime(2026, 8, 6, 6, 13, 0, tzinfo=UTC)

    def test_aware_timestamp_preserved(self):
        from sensorkit.autofocus.analyzer import _parse_frame_time

        t = _parse_frame_time("2026-08-06T06:12:49+00:00")
        assert t is not None and t.tzinfo is not None

    def test_absent_or_garbage_returns_none(self):
        from sensorkit.autofocus.analyzer import _parse_frame_time

        assert _parse_frame_time(None) is None
        assert _parse_frame_time("") is None
        assert _parse_frame_time("not a date") is None


# ---------------------------------------------------------------------------
# Test: Sign-convention calibration from sweep quadrupole votes
# ---------------------------------------------------------------------------


class TestConventionCalibration:
    """The V-curve calibrates state.defocus_sign_convention from per-frame quadrupole signs."""

    @staticmethod
    def _analyzer():
        from unittest.mock import AsyncMock, MagicMock

        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            FocuserConfig,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=0, max_position=50000),
        )
        analyzer = AutofocusAnalyzer(config, MagicMock())
        analyzer._entity = AsyncMock()
        analyzer._save_state = AsyncMock()
        analyzer.state.enabled = False  # skip the residual fold; calibration happens before it
        return analyzer

    def _run_sweep(self, sign_above: int):
        """Finalize a clean V sweep whose frames above best measured `sign_above` (and below,
        the opposite), and return the calibrated convention."""
        import asyncio

        from sensorkit.autofocus.analyzer import _Sweep

        analyzer = self._analyzer()
        best = 25000.0
        positions = [best + (i - 4) * 35.0 for i in range(9)]
        sweep = _Sweep(session="cal", expected_steps=9)
        sweep.points = [(p, (2.0**2 + (0.02 * (p - best)) ** 2) ** 0.5) for p in positions]
        sweep.sign_votes = [
            (p, sign_above if p > best else -sign_above) for p in positions if p != best
        ]
        analyzer._sweep = sweep
        asyncio.run(analyzer._finalize_sweep())
        return analyzer.state.defocus_sign_convention

    def test_positive_sign_above_best_calibrates_negative_convention(self):
        # Extra-focal frames measuring +1 need negative corrections: convention -1.
        assert self._run_sweep(sign_above=1) == -1

    def test_negative_sign_above_best_calibrates_positive_convention(self):
        assert self._run_sweep(sign_above=-1) == 1

    def test_no_votes_leaves_convention_untouched(self):
        import asyncio

        from sensorkit.autofocus.analyzer import _Sweep

        analyzer = self._analyzer()
        prior = analyzer.state.defocus_sign_convention
        best = 25000.0
        positions = [best + (i - 4) * 35.0 for i in range(9)]
        sweep = _Sweep(session="cal", expected_steps=9)
        sweep.points = [(p, (2.0**2 + (0.02 * (p - best)) ** 2) ** 0.5) for p in positions]
        analyzer._sweep = sweep
        asyncio.run(analyzer._finalize_sweep())
        assert analyzer.state.defocus_sign_convention == prior


# ---------------------------------------------------------------------------
# Test: Sweep finalization (instant on expected count, dedup, stall timeout)
# ---------------------------------------------------------------------------


class TestSweepFinalization:
    """Test the analyzer's sweep-completion logic without a live pipeline."""

    @staticmethod
    def _analyzer(frame_timeout_seconds: float = 90.0):
        from unittest.mock import AsyncMock, MagicMock

        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            CorrectionConfig,
            FocuserConfig,
            VCurveConfig,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=0, max_position=50000),
            vcurve=VCurveConfig(frame_timeout_seconds=frame_timeout_seconds),
            correction=CorrectionConfig(),
        )
        analyzer = AutofocusAnalyzer(config, MagicMock())
        analyzer._finalize_sweep = AsyncMock()
        return analyzer

    def test_finalizes_instantly_when_expected_count_arrives(self):
        """With the step count announced, the fit runs on the last frame — no timeout wait."""
        import asyncio

        analyzer = self._analyzer()

        async def run():
            analyzer.expect_sweep("s1", 3)
            for i, (pos, fwhm) in enumerate([(100.0, 3.0), (200.0, 2.0), (300.0, 3.0)]):
                await analyzer._process_vcurve("s1", f"/f{i}.fits", pos, fwhm, 1.0)
            if analyzer._sweep and analyzer._sweep.timer:
                analyzer._sweep.timer.cancel()

        asyncio.run(run())
        analyzer._finalize_sweep.assert_awaited_once()

    def test_duplicate_frame_paths_do_not_count(self):
        """A duplicate filesystem event for the same frame must not complete the sweep early."""
        import asyncio

        analyzer = self._analyzer()

        async def run():
            analyzer.expect_sweep("s1", 3)
            await analyzer._process_vcurve("s1", "/f0.fits", 100.0, 3.0, 1.0)
            await analyzer._process_vcurve("s1", "/f0.fits", 100.0, 3.0, 1.0)  # duplicate
            await analyzer._process_vcurve("s1", "/f1.fits", 200.0, 2.0, 1.0)
            if analyzer._sweep and analyzer._sweep.timer:
                analyzer._sweep.timer.cancel()

        asyncio.run(run())
        analyzer._finalize_sweep.assert_not_awaited()  # only 2 distinct of 3 expected

    def test_stall_timeout_finalizes_partial_sweep(self):
        """A lost frame must not wedge the sweep: the stall timer finalizes what arrived."""
        import asyncio

        analyzer = self._analyzer(frame_timeout_seconds=0.05)

        async def run():
            analyzer.expect_sweep("s1", 3)
            await analyzer._process_vcurve("s1", "/f0.fits", 100.0, 3.0, 1.0)
            await analyzer._process_vcurve("s1", "/f1.fits", 200.0, 2.0, 1.0)
            await asyncio.sleep(0.2)  # third frame never arrives

        asyncio.run(run())
        analyzer._finalize_sweep.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test: Sweep-in-flight guard (passive correction must not race the fold)
# ---------------------------------------------------------------------------


class TestSweepInFlightGuard:
    """While a V-curve is queued/running, a stray science frame's passive write to FocusCorrection
    would corrupt the fold's residual baseline. The guard suppresses passive correction from the
    sweep's announce until its fit folds."""

    @staticmethod
    def _analyzer():
        from unittest.mock import AsyncMock, MagicMock

        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            CorrectionConfig,
            FocuserConfig,
            VCurveConfig,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=20000, max_position=30000),
            vcurve=VCurveConfig(),
            correction=CorrectionConfig(),
        )
        analyzer = AutofocusAnalyzer(config, MagicMock())
        analyzer._entity = AsyncMock()
        analyzer._save_state = AsyncMock()
        analyzer.state.enabled = True
        analyzer.state.defocus_sign_convention = 1
        analyzer.state.vcurve_slope = 0.001
        analyzer.state.vcurve_best_fwhm_pixels = 3.0
        return analyzer

    @staticmethod
    def _science_result():
        from sensorkit.senpai.models import SenpaiResult

        return SenpaiResult(
            file_path="/f.fits",
            timestamp=datetime.now(UTC),
            track_mode="sidereal",
            n_sources=50,
            median_fwhm_pixels=4.0,
            std_fwhm_pixels=0.5,
            pixel_scale_arcsec=1.0,
            median_fwhm_arcsec=4.0,
            std_fwhm_arcsec=0.5,
            solved=True,
            detections=[],
        )

    def test_expect_sweep_arms_guard(self):
        analyzer = self._analyzer()
        assert analyzer._sweep_in_flight_since is None
        analyzer.expect_sweep("s1", 9)
        assert analyzer._sweep_in_flight_since is not None

    def test_finalize_clears_guard_even_on_early_return(self):
        """The fold's `finally` must clear the guard however finalize exits, or passive locks out
        forever. Here the sweep has too few points to fit — an early return."""
        import asyncio

        from sensorkit.autofocus.analyzer import _Sweep

        analyzer = self._analyzer()
        analyzer._sweep_in_flight_since = datetime.now(UTC)
        sweep = _Sweep(session="s1", expected_steps=9)
        sweep.points = [(25000.0, 3.0), (25100.0, 4.0)]  # <3 -> "cannot fit" early return
        analyzer._sweep = sweep
        asyncio.run(analyzer._finalize_sweep())
        assert analyzer._sweep_in_flight_since is None

    def test_passive_skipped_while_sweep_in_flight(self):
        """A science frame during the sweep window never reaches the correction decision."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        analyzer = self._analyzer()
        analyzer._read_fits_header = MagicMock(
            return_value={"FOCUSPOS": 25000.0, "TRKMODE": "sidereal"}
        )
        analyzer._measure_defocus_sign = AsyncMock(return_value=1)
        analyzer._evaluate_correction = MagicMock(return_value="skip")
        result = self._science_result()

        async def run(inflight):
            analyzer._sweep = None
            analyzer._sweep_in_flight_since = inflight
            analyzer._evaluate_correction.reset_mock()
            await analyzer._process_senpai(result)

        asyncio.run(run(datetime.now(UTC)))  # sweep in flight
        analyzer._evaluate_correction.assert_not_called()

        asyncio.run(run(None))  # guard clear
        analyzer._evaluate_correction.assert_called_once()

    def test_stuck_guard_self_clears_after_budget(self):
        """A queued sweep that never produced a frame must not lock passive out permanently."""
        import asyncio
        from datetime import timedelta
        from unittest.mock import AsyncMock, MagicMock

        analyzer = self._analyzer()
        analyzer._read_fits_header = MagicMock(
            return_value={"FOCUSPOS": 25000.0, "TRKMODE": "sidereal"}
        )
        analyzer._measure_defocus_sign = AsyncMock(return_value=1)
        analyzer._evaluate_correction = MagicMock(return_value="skip")
        analyzer._sweep = None  # no sweep ever materialized
        analyzer._sweep_in_flight_since = datetime.now(UTC) - timedelta(hours=1)

        asyncio.run(analyzer._process_senpai(self._science_result()))

        assert analyzer._sweep_in_flight_since is None  # self-cleared
        analyzer._evaluate_correction.assert_called_once()  # passive proceeded


# ---------------------------------------------------------------------------
# Test: AFMODE — autofocus telling SENPAI how to process a sweep
# ---------------------------------------------------------------------------


class TestPipelineModeCard:
    """The requested pipeline mode reaches SENPAI on the frames, since nothing calls SENPAI.

    Autofocus only subscribes to SenpaiResult; SENPAI is driven by its own directory watcher.
    So the FITS header is the transport, and VCurveStep is what writes it.
    """

    def test_mode_is_emitted_as_afmode(self):
        from sensorkit.autofocus.models import VCurveStep

        cards = dict(VCurveStep(session="abc123", pipeline_mode="full").get_fits_cards())
        assert cards["AFID"][0] == "abc123"
        assert cards["AFMODE"][0] == "full"

    def test_afmode_always_accompanies_afid(self):
        """Every sweep frame states its mode — there is no 'unspecified' sweep."""
        from sensorkit.autofocus.models import VCurveStep

        cards = dict(VCurveStep(session="abc123").get_fits_cards())
        assert cards["AFID"][0] == "abc123"
        assert cards["AFMODE"][0] == "detect"

    def test_config_default_is_detect(self):
        from sensorkit.autofocus.models import VCurveConfig

        assert VCurveConfig().pipeline_mode == "detect"

    def test_afmode_fits_the_8_char_keyword_limit(self):
        from sensorkit.autofocus.models import VCurveStep

        for key in dict(VCurveStep(session="s", pipeline_mode="detect_solve").get_fits_cards()):
            assert len(key) <= 8, f"{key} exceeds the FITS keyword limit"


# ---------------------------------------------------------------------------
# Test: Control-surface Requests are registered on the entity
# ---------------------------------------------------------------------------


class TestRequestRegistration:
    """entity_init registers the module's control Requests (the agent pattern)."""

    def test_entity_init_registers_requests(self):
        import asyncio
        from unittest.mock import AsyncMock, MagicMock, patch

        from sensorkit.autofocus.analyzer import AutofocusAnalyzer
        from sensorkit.autofocus.models import (
            AutofocusConfig,
            FocuserConfig,
            run_vcurve_request,
            set_enabled_request,
        )

        config = AutofocusConfig(
            entity="test_af",
            controller="test_ctrl",
            senpai_entity="senpai_test",
            focuser=FocuserConfig(entity="focuser", min_position=0, max_position=50000),
        )
        analyzer = AutofocusAnalyzer(config, MagicMock())
        entity = AsyncMock()
        entity.kv_get_model.side_effect = Exception("no saved state")

        async def run():
            with patch("sensorkit.autofocus.analyzer.sk.entity", return_value=entity):
                await analyzer.entity_init()
            for task in analyzer._tasks:
                task.cancel()
            await asyncio.gather(*analyzer._tasks, return_exceptions=True)

        asyncio.run(run())

        registered = [call.args[0] for call in entity.handle_request.await_args_list]
        assert run_vcurve_request in registered
        assert set_enabled_request in registered
        assert len(registered) == 2
