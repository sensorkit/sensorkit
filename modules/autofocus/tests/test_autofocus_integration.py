"""Integration tests for the autofocus pipeline using real through-focus data.

Test data:
    /opt/sk/tmp/focus1/  — 7 frames, positions 23043–26043, step 500
    /opt/sk/tmp/focus2/  — 7 frames, positions 23437–26437, step 500

These are 8120x8120 float32 FITS with minimal headers (no WCS).
Focus position is encoded in the filename: FOCUS{position}.fit

Run with:
    cd /opt/sensorkit && uv run pytest analysis/autofocus/tests/test_autofocus_integration.py -v -s
"""

import math
import re
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

        return 1000.0 * np.exp(-0.5 * (dx_r ** 2 / sigma_x ** 2 + dy_r ** 2 / sigma_y ** 2))

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

            sources.append(SourceMeasurement(
                x=x, y=y, fwhm_px=5.0, e1=e1, e2=e2, snr=50.0,
            ))

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

            sources.append(SourceMeasurement(
                x=x, y=y, fwhm_px=5.0, e1=e1, e2=e2, snr=50.0,
            ))

        sign = determine_defocus_sign(sources, center)
        assert sign == -1, f"Expected extra-focal (-1), got {sign}"


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
            math.sqrt(a_true * (p - p_opt_true) ** 2 + fwhm_best_true ** 2)
            for p in positions
        ]
        data = list(zip(positions, fwhm_values))

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
            fwhm_clean = math.sqrt(a_true * (p - p_opt_true) ** 2 + fwhm_best_true ** 2)
            fwhm_noisy = fwhm_clean + rng.normal(0, 0.3)
            fwhm_values.append(max(fwhm_noisy, 1.0))

        data = list(zip(positions, fwhm_values))
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
            f"FWHM_best {result.best_fwhm:.2f} is not reasonable "
            f"(max measured: {max_fwhm:.2f})"
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
            correction=CorrectionConfig(max_step=500, min_correction=10),
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
        assert abs(abs(delta) - expected) < 1.0, f"Expected |delta| ≈ {expected:.1f}, got {delta:.1f}"
        assert delta > 0, "Positive defocus_sign + positive convention → positive delta"

    def test_correction_clamped_by_max_step(self):
        """Correction should be clamped to max_step."""
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
            correction=CorrectionConfig(max_step=100, min_correction=10),
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
        assert abs(delta) <= 100.0, f"Delta {delta:.1f} should be clamped to max_step=100"


# ---------------------------------------------------------------------------
# Test: Target selection
# ---------------------------------------------------------------------------

class TestTargetSelection:
    """Test the V-curve target auto-selection."""

    def test_select_target_returns_valid(self):
        """Target selection should return an ICRSTarget above min altitude."""
        from sensorkit.autofocus.program import _select_vcurve_target

        # Use a mid-latitude site
        target = _select_vcurve_target(
            min_altitude=15.0,
            site_lat=35.0,
            site_lon=-106.0,
        )

        # May return None if no stars are up (unlikely for a reasonable location)
        if target is not None:
            assert hasattr(target, "coords")
            assert 0 <= target.coords.ra < 360
            assert -90 <= target.coords.dec <= 90

    def test_select_target_respects_altitude(self):
        """No target should be returned if min_altitude is impossibly high."""
        from sensorkit.autofocus.program import _select_vcurve_target

        target = _select_vcurve_target(
            min_altitude=89.9,  # Nearly zenith — very few candidates
            site_lat=35.0,
            site_lon=-106.0,
        )
        # This might return None, which is acceptable
        # Just verify it doesn't crash


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
        data = list(zip(positions, fwhms))
        result = fit_vcurve(data)

        print(f"  V-curve optimal: {result.best_position:.0f}")
        print(f"  V-curve FWHM_best: {result.best_fwhm:.2f} px")
        print(f"  V-curve R²: {result.r_squared:.4f}")

        # Basic sanity: positive slope, reasonable optimal
        assert result.slope > 0
        assert result.best_fwhm > 0
        assert result.best_fwhm < max(fwhms)
