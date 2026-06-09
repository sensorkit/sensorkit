import asyncio
import io
import logging
from collections.abc import Buffer
from typing import Any, Literal

import numpy as np
from astropy.io import fits
from pydantic import BaseModel, Field

from sensorkit.common.keyword import declare_keyword
from sensorkit.data.context import Context
from sensorkit.data.graph import DataFlow, DataOp

logger = logging.getLogger(__name__)


@declare_keyword
class ImageSize(BaseModel):
    """Pixel dimensions of a 2-D image."""
    width: int
    height: int


@declare_keyword
class ArrayInfo(BaseModel):
    """Metadata describing the shape, dtype, and memory order of a raw array buffer."""
    shape: tuple[int, ...]
    dtype: str
    order: Literal["C", "F"] = "C"

    @property
    def bit_length(self):
        """Return the number of bits per element in the array."""
        return np.dtype(self.dtype).itemsize * 8

    def ndarray_from_buffer(self, buffer: Buffer, allow_copy: bool = False) -> np.ndarray:
        """Interpret *buffer* as an ndarray with this object's shape and dtype."""
        arr = np.frombuffer(buffer, dtype=np.dtype(self.dtype))

        if arr.size != np.prod(self.shape):
            raise ValueError(f"Array size {arr.size} does not match shape {self.shape}")

        return arr.reshape(self.shape, order=self.order, copy=None if allow_copy else False)


class ReshapeArray(DataOp):
    """Convert the input buffer into an ndarray."""
    op: Literal["reshape_array"] = "reshape_array"
    array: ArrayInfo

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        if _info := context.get(ArrayInfo):
            # TODO: handle pre-existing ArrayInfo
            pass

        arr = await asyncio.to_thread(self.array.ndarray_from_buffer, buffer)
        context.set(self.array)
        await outgoing[0].send(context, arr)


class ArrayToFITS(DataOp):
    """Converts an input array to FITS format."""
    op: Literal["array_to_fits"] = "array_to_fits"
    header: dict = Field(default_factory=dict)

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        # This op requires the ArrayInfo keyword to be present.
        array = context[ArrayInfo]

        # Reshape the input buffer. This should always be zero-copy in cases where ReshapeArray
        # preceded this op, because if memory reallocation was required, it will have already been
        # done. Note in that case the buffer is actually already an ndarray, but we cannot assume
        # that.
        image_ndarray: np.ndarray = await asyncio.to_thread(array.ndarray_from_buffer, buffer)

        # Here we stipulate that this is a 2D image. FITS data cube support could be added here.
        if image_ndarray.ndim != 2:
            raise RuntimeError("array_to_fits only supports 2D arrays")

        # TODO: change this to ImageInfo and add bits-per-pixel, scale, color encoding, etc.
        context.set(ImageSize(width=image_ndarray.shape[1], height=image_ndarray.shape[0]))

        # Build primary HDU. astropy populates SIMPLE, NAXIS, and NAXIS{n} from the
        # array shape (NAXIS1 = fastest-varying axis = number of columns).
        primary_hdu = fits.PrimaryHDU(image_ndarray)
        header = primary_hdu.header

        # Generate the desired FITS keywords by evaluating the input patterns against the context.
        for kw, obj in self.header.items():
            value = self._eval_header_value(obj, context)

            if value is not None:
                header[kw] = value

        # Build the output FITS bytes and send it along the graph.
        hdul = fits.HDUList([primary_hdu])
        bio = io.BytesIO()
        hdul.writeto(bio)

        await outgoing[0].send(context, bio.getvalue())

    @staticmethod
    def _eval_header_value(obj: Any, context: Context) -> tuple[Any, str | None] | None:
        match obj:
            case {"value": expr, "comment": comment} | [expr, comment] | (expr, comment):
                pass
            case {"value": expr} | str(expr):
                comment = None
            case _:
                raise ValueError(f"Invalid header value: {obj}")

        value = context.eval(expr)
        return None if value is None else (value, comment)


class CompressFITS(DataOp):
    """Tile-compress a FITS image buffer using the FITS tiled-image convention.

    Wraps the primary image HDU in a CompImageHDU. If the image data exceeds
    32 bits per pixel (e.g. int64, float64), compression falls back to GZIP_1
    (lossless for all dtypes) and logs a warning, since RICE_1 only supports
    ≤32-bit values.
    """
    op: Literal["compress_fits"] = "compress_fits"
    algorithm: Literal["RICE_1", "GZIP_1", "GZIP_2", "HCOMPRESS_1"] = Field(
        "RICE_1",
        description="Tile compression algorithm. RICE_1 is lossless for ≤32-bit integer data.",
    )
    quantize_level: float = Field(
        0.0,
        description=(
            "Floating-point quantization level. 0 disables quantization (lossless). "
            "Values > 0 enable lossy quantization for floating-point data."
        ),
    )

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")
        compressed = await asyncio.to_thread(self._compress, buffer)
        await outgoing[0].send(context, compressed)

    def _compress(self, buffer: bytes) -> bytes:
        with fits.open(io.BytesIO(buffer)) as hdul:
            data = hdul[0].data
            header = hdul[0].header

            algorithm = self.algorithm

            # Safety check: RICE_1 and HCOMPRESS_1 only support ≤32-bit data.
            if data.dtype.itemsize > 4 and algorithm in ("RICE_1", "HCOMPRESS_1"):
                logger.warning(
                    "compress_fits: image dtype %s exceeds 32 bits; "
                    "falling back to GZIP_1 for lossless compression.",
                    data.dtype,
                )
                algorithm = "GZIP_1"

            compressed_hdu = fits.CompImageHDU(
                data=data,
                header=header,
                compression_type=algorithm,
                quantize_level=self.quantize_level,
            )
            new_hdul = fits.HDUList([fits.PrimaryHDU(), compressed_hdu])

            bio = io.BytesIO()
            new_hdul.writeto(bio)
            return bio.getvalue()


class ContextFromFITS(DataOp):
    """DataGraph node that populates the context with values from a FITS header."""
    op: Literal["context_from_fits"] = "context_from_fits"
    keyword_map: dict = Field(default_factory=dict)

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        from astropy.io import fits

        # Receive the FITS data as a buffer
        context, buffer = await incoming[0].receive("buffer")

        # Read FITS header from buffer
        with io.BytesIO(buffer) as bio:
            hdul = fits.open(bio)
            header = hdul[0].header

            # Map FITS keywords to context based on keyword map
            for meta_key, fits_key in self.keyword_map.items():
                if fits_key in header:
                    context[meta_key] = header[fits_key]

        # Pass through the original buffer
        await outgoing[0].send(context, buffer)




@declare_keyword
class DarkInfo(BaseModel):
    """Information about dark frame subtraction."""
    applied: bool = False
    dark_path: str | None = None
    dark_exposure: float | None = None
    image_exposure: float | None = None


class ApplyDark(DataOp):
    """Subtract a dark frame from the input image, matching by exposure time.

    Scans a directory for master dark FITS files and selects the one with the
    closest exposure time to the input image. Dark frames should have EXPTIME
    or EXPOSURE in their headers.
    """
    op: Literal["apply_dark"] = "apply_dark"
    dark_directory: str = Field(..., description="Directory containing master dark FITS files")
    pattern: str = Field("*.fits", description="Glob pattern for dark frame files")

    _dark_library: dict[float, tuple[str, np.ndarray]] | None = None

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, buffer = await incoming[0].receive("buffer")

        # Load dark library on first use
        if self._dark_library is None:
            self._dark_library = await asyncio.to_thread(self._load_dark_library)

        # Apply dark subtraction
        result_buffer, dark_info = await asyncio.to_thread(
            self._apply_dark, buffer
        )

        context.set(dark_info)
        await outgoing[0].send(context, result_buffer)

    def _load_dark_library(self) -> dict[float, tuple[str, np.ndarray]]:
        """Load all dark frames from directory, indexed by exposure time."""
        import glob
        import os

        library: dict[float, tuple[str, np.ndarray]] = {}
        pattern_path = os.path.join(self.dark_directory, self.pattern)

        for filepath in glob.glob(pattern_path):
            try:
                with fits.open(filepath) as hdul:
                    header = hdul[0].header
                    exposure = header.get('EXPTIME') or header.get('EXPOSURE')
                    if exposure is not None:
                        data = hdul[0].data.astype(np.float64)
                        library[float(exposure)] = (filepath, data)
            except Exception:
                continue

        return library

    def _find_closest_dark(self, target_exposure: float) -> tuple[float, str, np.ndarray] | None:
        """Find the dark frame with closest exposure time."""
        if not self._dark_library:
            return None

        exposures = list(self._dark_library.keys())
        closest_exp = min(exposures, key=lambda x: abs(x - target_exposure))
        filepath, data = self._dark_library[closest_exp]
        return closest_exp, filepath, data

    def _apply_dark(self, buffer: bytes) -> tuple[bytes, DarkInfo]:
        """Subtract matching dark frame from image data."""
        with io.BytesIO(buffer) as bio_in:
            with fits.open(bio_in) as hdul:
                image_data = hdul[0].data.astype(np.float64)
                header = hdul[0].header.copy()

                # Get image exposure time
                img_exposure = header.get('EXPTIME') or header.get('EXPOSURE')

                if img_exposure is None:
                    # No exposure info, pass through unchanged
                    return buffer, DarkInfo(applied=False, image_exposure=None)

                # Find closest matching dark
                match = self._find_closest_dark(float(img_exposure))
                if match is None:
                    # No darks available, pass through unchanged
                    return buffer, DarkInfo(applied=False, image_exposure=float(img_exposure))

                dark_exposure, dark_path, dark_data = match

                # Subtract dark frame
                calibrated = image_data - dark_data

                # Update header
                header['DARKFILE'] = (dark_path, 'Dark frame applied')
                header['DARKEXP'] = (dark_exposure, 'Dark frame exposure time')

                # Create output FITS
                primary_hdu = fits.PrimaryHDU(calibrated.astype(image_data.dtype), header=header)
                hdul_out = fits.HDUList([primary_hdu])

                bio_out = io.BytesIO()
                hdul_out.writeto(bio_out)

                dark_info = DarkInfo(
                    applied=True,
                    dark_path=dark_path,
                    dark_exposure=dark_exposure,
                    image_exposure=float(img_exposure),
                )

                return bio_out.getvalue(), dark_info
