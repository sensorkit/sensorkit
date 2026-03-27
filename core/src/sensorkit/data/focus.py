import asyncio
import io
from typing import Literal

import numpy as np
from astropy.io import fits
from pydantic import BaseModel, Field

from sensorkit.common.keyword import declare_keyword
from sensorkit.data.fits import ArrayInfo
from sensorkit.data.graph import DataFlow, DataOp


@declare_keyword
class FocusInfo(BaseModel):
    """Focus quality metrics for an image."""
    hfr: float | None = Field(None, description="Half-Flux Radius in pixels")
    fwhm: float | None = Field(None, description="Full Width Half Maximum in pixels")
    star_count: int = Field(0, description="Number of detected stars")
    focus_score: float | None = Field(None, description="Overall focus quality score (0-1, higher is better)")


@declare_keyword
class FocusFFTInfo(BaseModel):
    """Focus quality metrics using Fourier Transform analysis."""
    high_freq_ratio: float = Field(description="Ratio of high-frequency power to total power")
    focus_score: float = Field(description="Focus quality score (0-1, higher is better)")
    high_freq_threshold: float = Field(description="Threshold used to define high frequency region")
    scale_factor: float = Field(description="Scale factor applied to normalize score")


@declare_keyword
class FocusConvolutionInfo(BaseModel):
    """Focus quality metrics using convolution-based edge detection."""
    method: str = Field(description="Edge detection method used (laplacian, sobel, variance_laplacian)")
    raw_metric: float = Field(description="Raw focus metric value before normalization")
    focus_score: float = Field(description="Focus quality score (0-1, higher is better)")
    scale_factor: float = Field(description="Scale factor applied to normalize score")


@declare_keyword
class FocusConvolutionResults(BaseModel):
    """Container for multiple convolution-based focus analysis results."""
    results: dict[str, FocusConvolutionInfo] = Field(
        default_factory=dict,
        description="Focus analysis results keyed by method name"
    )

    def add_result(self, info: FocusConvolutionInfo):
        """Add a focus analysis result."""
        self.results[info.method] = info


class AnalyzeFocusStars(DataOp):
    """Analyze focus quality of an image and attach FocusInfo to context."""
    op: Literal["analyze_focus_stars"] = "analyze_focus_stars"
    detection_sigma: float = Field(5.0, description="Detection threshold in standard deviations above background")
    min_star_flux: float = Field(100.0, description="Minimum integrated flux for star detection")

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        if buffer[:6] == b'SIMPLE':
            image = await asyncio.to_thread(self._extract_fits_image, buffer)
        else:
            array_info = context.get(ArrayInfo)
            if not array_info:
                raise RuntimeError("analyze_focus requires ArrayInfo in context for raw array data")
            image = await asyncio.to_thread(array_info.ndarray_from_buffer, buffer)

        if image.ndim != 2:
            raise RuntimeError("analyze_focus only supports 2D arrays")

        focus_info = await asyncio.to_thread(self._analyze_focus, image)

        context.set(focus_info)

        await outgoing[0].send(context, buffer)

    def _extract_fits_image(self, buffer: bytes) -> np.ndarray:
        """Extract image data from a FITS buffer."""
        with io.BytesIO(buffer) as bio:
            with fits.open(bio) as hdul:
                return hdul[0].data.astype(np.float64)

    def _analyze_focus(self, image: np.ndarray) -> FocusInfo:
        """Compute focus metrics from image data."""
        try:
            from scipy import ndimage
        except ImportError:
            return FocusInfo()

        img = image.astype(np.float64)

        background = np.median(img)
        noise = np.std(img[img < np.percentile(img, 50)])

        threshold = background + self.detection_sigma * noise
        binary = img > threshold
        labeled, num_features = ndimage.label(binary)

        if num_features == 0:
            return FocusInfo(star_count=0)

        hfr_values = []
        fwhm_values = []
        star_count = 0

        for label_id in range(1, num_features + 1):
            star_metrics = self._analyze_star(img, labeled, label_id, background)
            if star_metrics:
                hfr_values.append(star_metrics['hfr'])
                if star_metrics['fwhm'] is not None:
                    fwhm_values.append(star_metrics['fwhm'])
                star_count += 1

        return self._compute_focus_info(hfr_values, fwhm_values, star_count)

    def _analyze_star(self, img: np.ndarray, labeled: np.ndarray, label_id: int, background: float) -> dict | None:
        """Analyze a single star and return its metrics."""
        mask = labeled == label_id
        if not mask.any():
            return None

        positions = np.where(mask)
        fluxes = img[mask]
        total_flux = fluxes.sum()

        if total_flux < self.min_star_flux:
            return None

        centroid_y = np.average(positions[0], weights=fluxes)
        centroid_x = np.average(positions[1], weights=fluxes)

        hfr = self._calculate_hfr(positions, fluxes, centroid_y, centroid_x)

        fwhm = self._calculate_fwhm(img, centroid_y, centroid_x, background)

        return {'hfr': hfr, 'fwhm': fwhm}

    def _calculate_hfr(self, positions: tuple, fluxes: np.ndarray, centroid_y: float, centroid_x: float) -> float:
        """Calculate Half-Flux Radius for a star."""
        distances = np.sqrt((positions[0] - centroid_y)**2 + (positions[1] - centroid_x)**2)
        sorted_indices = np.argsort(distances)
        sorted_fluxes = fluxes[sorted_indices]
        cumulative_flux = np.cumsum(sorted_fluxes)
        half_flux = fluxes.sum() / 2

        hfr_idx = np.searchsorted(cumulative_flux, half_flux)
        if hfr_idx < len(distances):
            return float(distances[sorted_indices[hfr_idx]])
        return 0.0

    def _calculate_fwhm(self, img: np.ndarray, centroid_y: float, centroid_x: float, background: float) -> float | None:
        """Calculate Full Width Half Maximum for a star."""
        try:
            cy, cx = int(centroid_y), int(centroid_x)
            size = 15
            y_min = max(0, cy - size)
            y_max = min(img.shape[0], cy + size + 1)
            x_min = max(0, cx - size)
            x_max = min(img.shape[1], cx + size + 1)

            star_region = img[y_min:y_max, x_min:x_max]
            if star_region.size > 0:
                peak = star_region.max()
                half_max = (peak + background) / 2
                half_max_pixels = star_region > half_max
                if half_max_pixels.any():
                    area = half_max_pixels.sum()
                    return float(2 * np.sqrt(area / np.pi))
        except (IndexError, ValueError):
            pass
        return None

    def _compute_focus_info(self, hfr_values: list[float], fwhm_values: list[float], star_count: int) -> FocusInfo:
        """Compute final focus metrics from collected star data."""
        median_hfr = float(np.median(hfr_values)) if hfr_values else None
        median_fwhm = float(np.median(fwhm_values)) if fwhm_values else None

        focus_score = None
        if median_hfr is not None:
            focus_score = float(1.0 / (1.0 + median_hfr))

        return FocusInfo(
            hfr=median_hfr,
            fwhm=median_fwhm,
            star_count=star_count,
            focus_score=focus_score
        )


class AnalyzeFocusFFT(DataOp):
    """Analyze focus quality using Fourier Transform (frequency domain analysis).

    Sharper images have more high-frequency content, so this method computes the
    ratio of high-frequency power to total power in the image spectrum.
    """
    op: Literal["analyze_focus_fft"] = "analyze_focus_fft"
    high_freq_threshold: float = Field(
        0.3,
        description="Fraction of frequency range considered 'high' (0-1)"
    )
    scale_factor: float = Field(
        50.0,
        description="Scale factor to normalize score to 0-1 range. Higher values for images with less high-freq content (astronomy: 50, satellites: 30)"
    )

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        if buffer[:6] == b'SIMPLE':
            image = await asyncio.to_thread(self._extract_fits_image, buffer)
        else:
            array_info = context.get(ArrayInfo)
            if not array_info:
                raise RuntimeError("analyze_focus_fft requires ArrayInfo in context for raw array data")
            image = await asyncio.to_thread(array_info.ndarray_from_buffer, buffer)

        if image.ndim != 2:
            raise RuntimeError("analyze_focus_fft only supports 2D arrays")

        focus_info = await asyncio.to_thread(self._analyze_focus_fft, image)

        context.set(focus_info)

        await outgoing[0].send(context, buffer)

    def _extract_fits_image(self, buffer: bytes) -> np.ndarray:
        """Extract image data from a FITS buffer."""
        with io.BytesIO(buffer) as bio:
            with fits.open(bio) as hdul:
                return hdul[0].data.astype(np.float64)

    def _analyze_focus_fft(self, image: np.ndarray) -> FocusFFTInfo:
        """Compute focus metrics using FFT analysis."""
        img = image.astype(np.float64)

        # Normalize the image
        img = (img - img.mean()) / (img.std() + 1e-8)

        # Compute 2D FFT
        fft = np.fft.fft2(img)
        fft_shift = np.fft.fftshift(fft)
        magnitude_spectrum = np.abs(fft_shift)

        # Create frequency coordinates
        rows, cols = img.shape
        crow, ccol = rows // 2, cols // 2

        # Create radial frequency map
        y, x = np.ogrid[:rows, :cols]
        r = np.sqrt((x - ccol)**2 + (y - crow)**2)
        max_r = np.sqrt(crow**2 + ccol**2)

        # Define high frequency region
        high_freq_radius = self.high_freq_threshold * max_r
        high_freq_mask = r > high_freq_radius

        # Compute power in high frequencies vs total power
        total_power = np.sum(magnitude_spectrum**2)
        high_freq_power = np.sum((magnitude_spectrum * high_freq_mask)**2)

        # Focus score based on high frequency content
        # Normalized to 0-1 range using configurable scale factor
        if total_power > 0:
            high_freq_ratio = high_freq_power / total_power
            focus_score = float(min(1.0, high_freq_ratio * self.scale_factor))
        else:
            high_freq_ratio = 0.0
            focus_score = 0.0

        return FocusFFTInfo(
            high_freq_ratio=float(high_freq_ratio),
            focus_score=focus_score,
            high_freq_threshold=self.high_freq_threshold,
            scale_factor=self.scale_factor
        )


class AnalyzeFocusConvolution(DataOp):
    """Analyze focus quality using convolution-based edge detection.

    Uses Laplacian or other edge detection kernels to measure sharpness.
    Sharper images have stronger edge responses.
    """
    op: Literal["analyze_focus_convolution"] = "analyze_focus_convolution"
    method: Literal["laplacian", "sobel", "variance_laplacian"] = Field(
        "variance_laplacian",
        description="Edge detection method to use"
    )
    scale_factor: float = Field(
        1.0,
        description="Divisor to normalize score to 0-1 range. Recommended: variance_laplacian=1.0, laplacian=0.5, sobel=1.0"
    )

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        if buffer[:6] == b'SIMPLE':
            image = await asyncio.to_thread(self._extract_fits_image, buffer)
        else:
            array_info = context.get(ArrayInfo)
            if not array_info:
                raise RuntimeError("analyze_focus_convolution requires ArrayInfo in context for raw array data")
            image = await asyncio.to_thread(array_info.ndarray_from_buffer, buffer)

        if image.ndim != 2:
            raise RuntimeError("analyze_focus_convolution only supports 2D arrays")

        focus_info = await asyncio.to_thread(self._analyze_focus_convolution, image)

        # Get or create the results container
        results = context.get(FocusConvolutionResults)
        if results is None:
            results = FocusConvolutionResults()

        # Add this result to the container
        results.add_result(focus_info)
        context.set(results)

        await outgoing[0].send(context, buffer)

    def _extract_fits_image(self, buffer: bytes) -> np.ndarray:
        """Extract image data from a FITS buffer."""
        with io.BytesIO(buffer) as bio:
            with fits.open(bio) as hdul:
                return hdul[0].data.astype(np.float64)

    def _analyze_focus_convolution(self, image: np.ndarray) -> FocusConvolutionInfo:
        """Compute focus metrics using convolution-based edge detection."""
        try:
            from scipy import ndimage
        except ImportError:
            # Return a default result if scipy is not available
            return FocusConvolutionInfo(
                method=self.method,
                raw_metric=0.0,
                focus_score=0.0,
                scale_factor=self.scale_factor
            )

        img = image.astype(np.float64)

        # Normalize the image
        img_norm = (img - img.mean()) / (img.std() + 1e-8)

        if self.method == "laplacian":
            # Laplacian kernel
            laplacian = ndimage.laplace(img_norm)
            focus_metric = np.abs(laplacian).mean()

        elif self.method == "sobel":
            # Sobel edge detection in both directions
            sobel_x = ndimage.sobel(img_norm, axis=0)
            sobel_y = ndimage.sobel(img_norm, axis=1)
            edge_magnitude = np.sqrt(sobel_x**2 + sobel_y**2)
            focus_metric = edge_magnitude.mean()

        elif self.method == "variance_laplacian":
            # Variance of Laplacian - excellent for focus detection
            # This is essentially the Tenengrad operator
            laplacian = ndimage.laplace(img_norm)
            focus_metric = laplacian.var()

        # Normalize focus metric to 0-1 range using configurable scale factor
        focus_score = float(min(1.0, focus_metric / self.scale_factor))

        return FocusConvolutionInfo(
            method=self.method,
            raw_metric=float(focus_metric),
            focus_score=focus_score,
            scale_factor=self.scale_factor
        )


class FocusInfoToFITS(DataOp):
    """Add focus analysis keyword data to FITS headers.

    Handles FocusInfo (star-based), FocusFFTInfo, and FocusConvolutionResults.
    """
    op: Literal["focus_info_to_fits"] = "focus_info_to_fits"

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        if buffer[:6] != b'SIMPLE':
            raise RuntimeError("focus_info_to_fits requires FITS format buffer")

        focus_info = context.get(FocusInfo)
        fft_info = context.get(FocusFFTInfo)
        conv_results = context.get(FocusConvolutionResults)

        # If no focus data at all, pass through
        if not any([focus_info, fft_info, conv_results]):
            await outgoing[0].send(context, buffer)
            return

        def update_fits_headers(
            buffer: bytes,
            focus: FocusInfo | None,
            fft: FocusFFTInfo | None,
            conv: FocusConvolutionResults | None
        ) -> bytes:
            bio_in = io.BytesIO(buffer)
            bio_out = io.BytesIO()

            with fits.open(bio_in) as hdul:
                header = hdul[0].header

                # Star-based focus metrics
                if focus:
                    if focus.hfr is not None:
                        header['FOCHFR'] = (focus.hfr, 'Half-Flux Radius in pixels')
                    if focus.fwhm is not None:
                        header['FOCFWHM'] = (focus.fwhm, 'Full Width Half Maximum in pixels')
                    if focus.star_count is not None:
                        header['FOCSTR'] = (focus.star_count, 'Number of detected stars')
                    if focus.focus_score is not None:
                        header['FOCSCR'] = (focus.focus_score, 'Focus score (star-based)')

                # FFT-based focus metrics
                if fft:
                    header['FOCFFT'] = (fft.focus_score, 'Focus score (FFT)')
                    header['FOCFFTR'] = (fft.high_freq_ratio, 'High-frequency ratio')
                    header['FOCFFTT'] = (fft.high_freq_threshold, 'FFT high-freq threshold')

                # Convolution-based focus metrics
                if conv and conv.results:
                    for method_name, info in conv.results.items():
                        # Use method abbreviations for FITS keywords (8 char limit)
                        prefix = {
                            'variance_laplacian': 'FOCVLAP',
                            'laplacian': 'FOCLAP',
                            'sobel': 'FOCSOB'
                        }.get(method_name, f'FOC{method_name[:3].upper()}')

                        header[prefix] = (info.focus_score, f'Focus score ({method_name})')
                        header[f'{prefix}R'] = (info.raw_metric, f'{method_name} raw metric')

                hdul.writeto(bio_out)

            return bio_out.getvalue()

        updated_buffer = await asyncio.to_thread(
            update_fits_headers, buffer, focus_info, fft_info, conv_results
        )
        await outgoing[0].send(context, updated_buffer)
