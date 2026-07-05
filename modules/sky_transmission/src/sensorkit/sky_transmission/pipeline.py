# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
import io
import pathlib
import warnings

import time

from astropy.utils.exceptions import AstropyDeprecationWarning

# Silence deprecation noise emitted by photutils when called from allclear.
# Scoped to photutils so we don't hide warnings raised by astropy itself or by
# SensorKit code elsewhere.
warnings.filterwarnings(
    "ignore",
    category=AstropyDeprecationWarning,
    module=r"photutils\..*",
)

from allclear.api import SkyTransmissionResult, get_sky_transmission
from allclear.instrument import InstrumentModel
from allclear.plotting import plot_frame
from allclear.utils import load_image
from loguru import logger
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

from sensorkit.sky_transmission.models import AllClearConfig


@dataclass
class FrameResult:
    """Result of processing a single all-sky frame."""

    clear_fraction: float
    n_matched: int
    n_expected: int
    status: str
    annotated_jpeg: bytes
    annotated_path: str | None = None
    sky_result: SkyTransmissionResult | None = None


class AllClearPipeline:
    """Uses the `allclear` API for a given instrument model."""

    def __init__(self, config: AllClearConfig):
        self.instrument_model = InstrumentModel.load(config.instrument_model_path)
        self.camera = self.instrument_model.to_camera_model()
        self.clear_threshold = config.clear_threshold

    def process_frame(
        self,
        image_path: str,
        *,
        pointings: dict[str, tuple[float, float]] | None = None,
        output_dir: str | None = None,
        site_location: tuple[float, float] | None = None,
    ) -> FrameResult:
        """Process a single image file through the allclear pipeline.

        Parameters:
            image_path : str
                Path to a FITS/JPG/PNG/TIFF image.
            pointings : dict of mount_name -> (az_deg, alt_deg), optional
                Telescope pointings to overlay as crosshairs.
            output_dir : str, optional
                Directory to save annotated PNG. If None, no file is saved.
            site_location : tuple of (lat_deg, lon_deg), optional
                Fallback site coordinates from the SensorKit controller.

        Returns:
            FrameResult
        """
        try:
            return self._do_process(image_path, pointings, output_dir, site_location)
        except Exception:
            logger.exception(f"allclear pipeline failed for {image_path}")
            return FrameResult(
                clear_fraction=0.0,
                n_matched=0,
                n_expected=0,
                status="failed",
                annotated_jpeg=b"",
            )

    def _do_process(
        self,
        image_path: str,
        pointings: dict[str, tuple[float, float]] | None,
        output_dir: str | None,
        site_location: tuple[float, float] | None,
    ) -> FrameResult:
        """Process a frame through allclear and render an annotated image."""

        # Run the allclear pipeline
        sky_result = get_sky_transmission(
            image_path, self.instrument_model, threshold=self.clear_threshold,
        )

        # Reconstruct per-star transmission overlay from the result
        transmission_data = None
        if sky_result.per_star:
            stars = sky_result.per_star
            transmission_data = (
                np.array([s["az_deg"] for s in stars]),
                np.array([s["alt_deg"] for s in stars]),
                np.array([s["transmission"] for s in stars]),
            )

        # Load image data for rendering the annotated frame
        data, _ = load_image(image_path)

        fig, ax = plot_frame(
            data, self.camera,
            show_grid=True,
            transmission_data=transmission_data,
            obs_time=sky_result.obs_time,
            lat_deg=sky_result.site_lat,
            lon_deg=sky_result.site_lon,
        )

        # Annotate the frame. Note plot_frame uses origin="lower", so y=0 is the
        # bottom edge. Clear-sky/status go top-right; the collection timestamp is
        # pinned to the bottom-right corner.
        ny, nx = data.shape
        margin = 0.005 * np.array(data.shape)
        fs = max(6, min(nx, ny) * 0.01) * 1.2

        info_lines = [
            f"Clear sky: {sky_result.clear_fraction:.0%}",
            f"Status: {sky_result.status}",
        ]
        for i, line in enumerate(info_lines):
            ax.text(
                nx - margin[1], (ny - 1) - margin[0] - fs * 2.0 * i, line,
                color="white", ha="right", va="top", size=fs, alpha=0.8,
            )

        ax.text(
            nx - margin[1], margin[0],
            sky_result.obs_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            color="white", ha="right", va="bottom", size=fs, alpha=0.8,
        )

        # Add telescope pointing crosshairs
        if pointings and fig is not None:
            self._draw_pointings(ax, self.camera, pointings, data.shape)

        # Save annotated PNG and serialize to JPEG
        annotated_path = None
        if output_dir:
            out_dir = pathlib.Path(output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            stem = pathlib.Path(image_path).stem
            annotated_path = str(out_dir / f"{stem}_transmission.png")

        annotated_jpeg = b""
        if fig is not None:
            if annotated_path:
                fig.savefig(annotated_path, dpi=100, bbox_inches="tight")
            buf = io.BytesIO()
            fig.savefig(buf, format="jpg", dpi=100, bbox_inches="tight")
            annotated_jpeg = buf.getvalue()
            plt.close(fig)

        return FrameResult(
            clear_fraction=sky_result.clear_fraction,
            n_matched=sky_result.n_matched,
            n_expected=sky_result.n_expected,
            status=sky_result.status,
            annotated_jpeg=annotated_jpeg,
            annotated_path=annotated_path,
            sky_result=sky_result if sky_result.n_matched >= 3 else None,
        )

    @staticmethod
    def _draw_pointings(
        ax,
        camera_model,
        pointings: dict[str, tuple[float, float]],
        image_shape: tuple[int, int],
    ):
        """Draw crosshair markers at telescope pointing positions."""
        height, width = image_shape
        crosshair_width = 30

        for name, (az_deg, alt_deg) in pointings.items():
            px, py = camera_model.sky_to_pixel(np.radians(az_deg), np.radians(alt_deg))

            if not (np.isfinite(px) and np.isfinite(py)):
                continue
            if not (0 <= px < width and 0 <= py < height):
                continue

            style = dict(color="cyan", linewidth=2, alpha=0.9)
            ax.plot([px - crosshair_width, px + crosshair_width], [py, py], **style)
            ax.plot([px, px], [py - crosshair_width, py + crosshair_width], **style)

            ax.annotate(
                f"{name} ({alt_deg:.0f}\u00b0, {az_deg:.0f}\u00b0)",
                (px, py + crosshair_width + 10),
                color="cyan", fontsize=16, ha="center",
            )


class MovieBuilder:
    """Scans output_dir for recent annotated frames and builds an MP4."""

    def __init__(self, max_width: int = 800):
        self.max_width = max_width

    def build_movie(
        self, output_dir: str, output_path: str, lookback_hours: float
    ) -> bool:
        """Build an H.264 MP4 from frames within the lookback window."""
        import imageio.v3 as iio

        cutoff = time.time() - lookback_hours * 3600
        out_dir = pathlib.Path(output_dir)
        files = sorted(
            (
                p
                for p in out_dir.glob("*_transmission.png")
                if p.stat().st_mtime >= cutoff
            ),
            key=lambda p: p.stat().st_mtime,
        )

        if len(files) < 2:
            return False

        tmp_path = output_path + ".tmp.mp4"
        try:
            pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            with iio.imopen(tmp_path, "w", plugin="pyav") as out:
                for file in files:
                    if not file.exists():
                        continue
                    img = Image.open(file).convert("RGB")

                    w, h = img.size
                    if w > self.max_width:
                        scale = self.max_width / w
                        img = img.resize(
                            (self.max_width, int(h * scale)), Image.LANCZOS
                        )
                    # H.264 requires even dimensions
                    nw, nh = img.size
                    if nw % 2:
                        nw -= 1
                    if nh % 2:
                        nh -= 1
                    if (nw, nh) != img.size:
                        img = img.crop((0, 0, nw, nh))
                    frame = np.asarray(img)

                    out.write(
                        frame,
                        is_batch=False,
                        codec="libx264",
                        fps=24,
                        in_pixel_format="rgb24",
                        out_pixel_format="yuv420p",
                    )

            pathlib.Path(tmp_path).replace(output_path)
            return True
        except Exception as e:
            logger.exception(f"movie build failed: {e}")
            pathlib.Path(tmp_path).unlink(missing_ok=True)
            return False
