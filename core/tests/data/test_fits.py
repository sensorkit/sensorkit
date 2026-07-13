# SPDX-License-Identifier: Apache-2.0
import asyncio
import io

import numpy as np
import pytest
from astropy.io import fits
from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword
from sensorkit.data.fits import (
    ApplyDark,
    ArrayInfo,
    ArrayToFITS,
    BuildFITSHeader,
    CompressFITS,
    ContextFromFITS,
    DarkInfo,
    FITSCardValueWithComment,
    FITSHeader,
    ReshapeArray,
)
from sensorkit.data.graph import Context, DataFlow


def _make_fits_buffer(data: np.ndarray, header_dict: dict | None = None) -> bytes:
    """Build FITS bytes from a numpy array and optional header keywords."""
    hdu = fits.PrimaryHDU(data)
    if header_dict:
        for k, v in header_dict.items():
            hdu.header[k] = v
    bio = io.BytesIO()
    fits.HDUList([hdu]).writeto(bio)
    return bio.getvalue()


async def _run_dataop(op, ctx: Context, buffer: bytes) -> tuple[Context, bytes]:
    """Drive a single DataOp through one process() run and return (out_context, out_buffer)."""
    incoming, outgoing = DataFlow(), DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))
    holder: dict = {}

    async def receiver():
        holder["ctx"], holder["buf"] = await outgoing.receive("buffer")

    recv = asyncio.create_task(receiver())
    await incoming.send(ctx, buffer)
    await recv
    await task
    return holder["ctx"], holder["buf"]


@pytest.mark.asyncio
async def test_reshape_array():
    """Test the ReshapeArray class for reshaping raw image data."""
    width, height = 10, 12
    reshape = ReshapeArray(array=ArrayInfo(dtype="uint16", shape=(height, width)))
    incoming_edge = DataFlow()
    outgoing_edge = DataFlow()
    process_task = asyncio.create_task(reshape.process([incoming_edge], [outgoing_edge]))

    test_data = np.arange(width * height, dtype=np.uint16)
    context = Context()

    async def receiver():
        out_context, reshaped_array = await outgoing_edge.receive("buffer")
        assert isinstance(reshaped_array, np.ndarray)
        assert reshaped_array.shape == (height, width)
        assert np.array_equal(reshaped_array.flatten(), test_data)

        info = out_context.get(ArrayInfo)
        assert info.shape == (height, width)

    receiver_task = asyncio.create_task(receiver())
    await incoming_edge.send(context, test_data.tobytes())
    await receiver_task
    await process_task


@pytest.mark.asyncio
async def test_array_to_fits():
    array_to_fits = ArrayToFITS(
        header={
            "MYHDR": 'f"{mycontextval}"',
            "BYTEORD": 'f"{ArrayInfo.order}"',
            "INSTRUME": "Test Camera",
        },
    )

    incoming_edge = DataFlow()
    outgoing_edge = DataFlow()
    process_task = asyncio.create_task(
        array_to_fits.process([incoming_edge], [outgoing_edge])
    )
    width, height = 12, 8
    test_data = np.arange(width * height, dtype=np.uint16)

    context = Context()
    context.set(ArrayInfo(shape=(height, width), dtype="uint16", order="C"))
    context["mycontextval"] = 42

    async def receiver():
        out_context, fits_buffer = await outgoing_edge.receive("buffer")
        assert out_context["ImageSize"].width == width
        assert out_context["ImageSize"].height == height

        bio = io.BytesIO(fits_buffer)

        with fits.open(bio) as hdul:
            image_data = hdul[0].data
            assert image_data.shape == (height, width)
            assert np.array_equal(image_data.flatten(), test_data)

            header = hdul[0].header
            assert header["NAXIS1"] == width
            assert header["NAXIS2"] == height
            assert header["MYHDR"] == str(context["mycontextval"])
            assert header["BYTEORD"] == "C"
            assert header["INSTRUME"] == "Test Camera"

    receiver_task = asyncio.create_task(receiver())
    await incoming_edge.send(context, test_data.tobytes())
    await receiver_task
    await process_task


def test_array_info_bit_length():
    assert ArrayInfo(shape=(1,), dtype="uint16").bit_length == 16
    assert ArrayInfo(shape=(1,), dtype="float64").bit_length == 64
    assert ArrayInfo(shape=(1,), dtype="uint8").bit_length == 8


def test_array_info_ndarray_from_buffer():
    for dtype in ("uint16", "float32"):
        arr = np.arange(12, dtype=dtype)
        info = ArrayInfo(shape=(3, 4), dtype=dtype)
        result = info.ndarray_from_buffer(arr.tobytes())
        assert result.shape == (3, 4)
        assert result.dtype == np.dtype(dtype)
        assert np.array_equal(result.flatten(), arr)


def test_array_info_ndarray_size_mismatch():
    info = ArrayInfo(shape=(3, 4), dtype="uint16")
    wrong_buf = np.arange(10, dtype=np.uint16).tobytes()  # 10 != 3*4
    with pytest.raises(ValueError, match="does not match shape"):
        info.ndarray_from_buffer(wrong_buf)


def test_array_info_fortran_order():
    arr = np.arange(6, dtype=np.float32)
    info = ArrayInfo(shape=(2, 3), dtype="float32", order="F")
    result = info.ndarray_from_buffer(arr.tobytes(), allow_copy=True)
    assert result.shape == (2, 3)
    # Fortran order fills columns first.
    assert result[0, 0] == 0.0
    assert result[1, 0] == 1.0
    assert result[0, 1] == 2.0


@pytest.mark.asyncio
async def test_array_to_fits_non_2d_raises():
    """1D array should raise RuntimeError."""
    op = ArrayToFITS()
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    ctx = Context()
    ctx.set(ArrayInfo(shape=(10,), dtype="uint16"))
    data = np.arange(10, dtype=np.uint16)

    await incoming.send(ctx, data.tobytes())
    with pytest.raises(RuntimeError, match="only supports 2D"):
        await task


@pytest.mark.asyncio
async def test_array_to_fits_dict_header():
    """Header with dict syntax: {'value': ..., 'comment': ...}."""
    op = ArrayToFITS(
        header={"CAMERA": {"value": "MyCam", "comment": "Camera name"}},
    )
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    width, height = 4, 4
    ctx = Context()
    ctx.set(ArrayInfo(shape=(height, width), dtype="uint16"))
    data = np.zeros(width * height, dtype=np.uint16)

    async def receiver():
        _, buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(buf)) as hdul:
            assert hdul[0].header["CAMERA"] == "MyCam"
            assert "Camera name" in hdul[0].header.comments["CAMERA"]

    recv = asyncio.create_task(receiver())
    await incoming.send(ctx, data.tobytes())
    await recv
    await task


@pytest.mark.asyncio
async def test_array_to_fits_tuple_header():
    """Header with tuple/list syntax: [value, comment]."""
    op = ArrayToFITS(
        header={"INST": ["Scope", "Instrument"]},
    )
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    width, height = 4, 4
    ctx = Context()
    ctx.set(ArrayInfo(shape=(height, width), dtype="uint16"))
    data = np.zeros(width * height, dtype=np.uint16)

    async def receiver():
        _, buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(buf)) as hdul:
            assert hdul[0].header["INST"] == "Scope"
            assert "Instrument" in hdul[0].header.comments["INST"]

    recv = asyncio.create_task(receiver())
    await incoming.send(ctx, data.tobytes())
    await recv
    await task


@pytest.mark.asyncio
async def test_array_to_fits_none_value_skipped():
    """Header value resolving to None should be omitted."""
    op = ArrayToFITS(
        header={"MISSING": "=None", "PRESENT": "yes"},
    )
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    width, height = 4, 4
    ctx = Context()
    ctx.set(ArrayInfo(shape=(height, width), dtype="uint16"))
    data = np.zeros(width * height, dtype=np.uint16)

    async def receiver():
        _, buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(buf)) as hdul:
            assert "MISSING" not in hdul[0].header
            assert hdul[0].header["PRESENT"] == "yes"

    recv = asyncio.create_task(receiver())
    await incoming.send(ctx, data.tobytes())
    await recv
    await task


@pytest.mark.asyncio
async def test_array_to_fits_non_string_scalar_literals():
    """Non-string YAML scalars (int/float/bool) pass through as literal card values."""
    op = ArrayToFITS(
        header={
            "EXPTIME": 30.0,
            "NCOADD": 4,
            "SHUTTER": True,
            "GAIN": [1.5, "detector gain"],
        },
    )
    ctx = Context()
    ctx.set(ArrayInfo(shape=(4, 4), dtype="uint16"))
    data = np.zeros(16, dtype=np.uint16)

    _, buf = await _run_dataop(op, ctx, data.tobytes())

    with fits.open(io.BytesIO(buf)) as hdul:
        header = hdul[0].header
        assert header["EXPTIME"] == 30.0
        assert header["NCOADD"] == 4
        assert header["SHUTTER"] is True
        assert header["GAIN"] == 1.5  # numeric value inside the [value, comment] form
        assert "detector gain" in header.comments["GAIN"]


@pytest.mark.asyncio
async def test_context_from_fits():
    """Extract FITS header keywords into context via keyword_map."""
    image = np.zeros((4, 4), dtype=np.uint16)
    buf = _make_fits_buffer(image, {"INSTRUME": "TestCam", "EXPTIME": 5.0})

    op = ContextFromFITS(
        keyword_map={"instrument": "INSTRUME", "exposure": "EXPTIME", "missing_key": "NOEXIST"},
    )
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        ctx, out_buf = await outgoing.receive("buffer")
        assert ctx["instrument"] == "TestCam"
        assert ctx["exposure"] == 5.0
        assert "missing_key" not in ctx
        # Original buffer passed through.
        assert out_buf == buf

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_apply_dark_happy_path(tmp_path):
    """Dark subtraction applied when exposures match."""
    dark_data = np.full((4, 4), 10.0, dtype=np.float64)
    dark_buf = _make_fits_buffer(dark_data, {"EXPTIME": 5.0})
    (tmp_path / "dark_5s.fits").write_bytes(dark_buf)

    image_data = np.full((4, 4), 100.0, dtype=np.float64)
    image_buf = _make_fits_buffer(image_data, {"EXPTIME": 5.0})

    op = ApplyDark(dark_directory=str(tmp_path))
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        ctx, out_buf = await outgoing.receive("buffer")
        info = ctx.get(DarkInfo)
        assert info is not None
        assert info.applied is True
        assert info.image_exposure == 5.0

        with fits.open(io.BytesIO(out_buf)) as hdul:
            result = hdul[0].data.astype(np.float64)
            np.testing.assert_allclose(result, 90.0)
            assert "DARKFILE" in hdul[0].header

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), image_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_apply_dark_no_exposure(tmp_path):
    """Image without EXPTIME passes through unchanged."""
    image_data = np.full((4, 4), 100.0, dtype=np.float64)
    image_buf = _make_fits_buffer(image_data)

    op = ApplyDark(dark_directory=str(tmp_path))
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        ctx, out_buf = await outgoing.receive("buffer")
        info = ctx.get(DarkInfo)
        assert info is not None
        assert info.applied is False
        assert out_buf == image_buf

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), image_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_apply_dark_no_darks(tmp_path):
    """Empty dark directory passes through unchanged."""
    image_data = np.full((4, 4), 100.0, dtype=np.float64)
    image_buf = _make_fits_buffer(image_data, {"EXPTIME": 5.0})

    op = ApplyDark(dark_directory=str(tmp_path))
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        ctx, out_buf = await outgoing.receive("buffer")
        info = ctx.get(DarkInfo)
        assert info is not None
        assert info.applied is False

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), image_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_apply_dark_closest_exposure(tmp_path):
    """Multiple darks: closest exposure time is selected."""
    for exp in (1.0, 5.0, 10.0):
        dark = np.full((4, 4), exp, dtype=np.float64)
        buf = _make_fits_buffer(dark, {"EXPTIME": exp})
        (tmp_path / f"dark_{exp}s.fits").write_bytes(buf)

    # Image with EXPTIME=6.0 — closest dark is 5.0s.
    image_data = np.full((4, 4), 100.0, dtype=np.float64)
    image_buf = _make_fits_buffer(image_data, {"EXPTIME": 6.0})

    op = ApplyDark(dark_directory=str(tmp_path))
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        ctx, out_buf = await outgoing.receive("buffer")
        info = ctx.get(DarkInfo)
        assert info.applied is True
        assert info.dark_exposure == 5.0

        with fits.open(io.BytesIO(out_buf)) as hdul:
            # 100 - 5.0 (dark data value for 5s exposure) = 95
            np.testing.assert_allclose(hdul[0].data.astype(np.float64), 95.0)

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), image_buf)
    await recv
    await task


# --- CompressFITS tests ---


@pytest.mark.asyncio
async def test_compress_fits_rice():
    """RICE_1 compression round-trips 16-bit data correctly."""
    image_data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    fits_buf = _make_fits_buffer(image_data)

    op = CompressFITS()  # defaults: algorithm=RICE_1, quantize_level=0.0
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        _, compressed_buf = await outgoing.receive("buffer")
        # Compressed buffer should be smaller (or at least valid FITS)
        with fits.open(io.BytesIO(compressed_buf)) as hdul:
            # CompImageHDU is stored as extension 1
            assert len(hdul) == 2
            assert isinstance(hdul[1], fits.CompImageHDU)
            np.testing.assert_array_equal(hdul[1].data, image_data)

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), fits_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_compress_fits_gzip():
    """Explicit GZIP_1 algorithm works for 16-bit data."""
    image_data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    fits_buf = _make_fits_buffer(image_data)

    op = CompressFITS(algorithm="GZIP_1")
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        _, compressed_buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(compressed_buf)) as hdul:
            assert isinstance(hdul[1], fits.CompImageHDU)
            np.testing.assert_array_equal(hdul[1].data, image_data)

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), fits_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_compress_fits_fallback_on_64bit():
    """64-bit data with RICE_1 falls back to GZIP_1, which is lossless for all dtypes."""
    image_data = np.arange(64, dtype=np.int64).reshape(8, 8)
    fits_buf = _make_fits_buffer(image_data)

    op = CompressFITS(algorithm="RICE_1")
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        _, compressed_buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(compressed_buf)) as hdul:
            assert isinstance(hdul[1], fits.CompImageHDU)
            assert hdul[1].compression_type == "GZIP_1"
            np.testing.assert_array_equal(hdul[1].data, image_data)

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), fits_buf)
    await recv
    await task


@pytest.mark.asyncio
async def test_compress_fits_passthrough_header():
    """FITS header keywords survive compression."""
    image_data = np.zeros((8, 8), dtype=np.uint16)
    fits_buf = _make_fits_buffer(image_data, {"INSTRUME": "TestCam", "EXPTIME": 30.0})

    op = CompressFITS()
    incoming = DataFlow()
    outgoing = DataFlow()
    task = asyncio.create_task(op.process([incoming], [outgoing]))

    async def receiver():
        _, compressed_buf = await outgoing.receive("buffer")
        with fits.open(io.BytesIO(compressed_buf)) as hdul:
            header = hdul[1].header
            assert header["INSTRUME"] == "TestCam"
            assert header["EXPTIME"] == 30.0

    recv = asyncio.create_task(receiver())
    await incoming.send(Context(), fits_buf)
    await recv
    await task


# --- BuildFITSHeader tests ---


@declare_keyword
class _StubCamera(BaseModel):
    """Test FITSCardProvider yielding a plain card and a card with a comment."""

    def get_fits_cards(self):
        yield "INSTRUME", "StubCam"
        yield "GAIN", FITSCardValueWithComment("1.5", "detector gain")


@declare_keyword
class _StubTelescope(BaseModel):
    """Test FITSCardProvider that is declared but never placed in the context."""

    def get_fits_cards(self):
        yield "TELESCOP", "StubScope"


@declare_keyword
class _StubNotAProvider(BaseModel):
    """Test keyword that is declared but does not satisfy `FITSCardProvider`."""

    value: int = 42


@pytest.mark.asyncio
async def test_build_fits_header_define_and_option():
    """define resolves values; option only adds non-None and suppresses missing names."""
    op = BuildFITSHeader(
        define={"TELESCOP": "Scope-1", "EXPTIME": "=exptime"},
        option={"CCDTEMP": "=ccdtemp", "FILTER": "=missing"},
    )
    ctx = Context()
    ctx["exptime"] = 30.0
    ctx["ccdtemp"] = -10.0

    out_ctx, out_buf = await _run_dataop(op, ctx, b"raw-passthrough")

    # The op builds a header but passes the buffer through unchanged.
    assert out_buf == b"raw-passthrough"

    header = out_ctx.get(FITSHeader)
    assert header["TELESCOP"] == "Scope-1"  # bare string is a literal
    assert header["EXPTIME"] == 30.0  # =expr evaluates against the context
    assert header["CCDTEMP"] == -10.0
    assert "FILTER" not in header  # option with an absent name is suppressed


@pytest.mark.asyncio
async def test_build_fits_header_respects_existing_and_define_skips_present():
    """A pre-existing FITSHeader is reused; define does not overwrite present keywords."""
    op = BuildFITSHeader(define={"OBSERVER": "Bob", "TELESCOP": "Scope-1"})
    ctx = Context()
    ctx.set(FITSHeader({"OBSERVER": "Alice"}))

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    header = out_ctx.get(FITSHeader)
    assert header["OBSERVER"] == "Alice"  # define skips an already-set keyword
    assert header["TELESCOP"] == "Scope-1"


@pytest.mark.asyncio
async def test_build_fits_header_mutate_rename_remove():
    """mutate replaces existing values only; rename moves cards; remove drops them."""
    op = BuildFITSHeader(
        mutate={"GAIN": "=gain", "ABSENT": "ignored"},
        rename={"OLD": "NEW"},
        remove={"JUNK"},
    )
    ctx = Context()
    ctx.set(FITSHeader({"GAIN": "1.0", "OLD": "moved", "JUNK": "x"}))
    ctx["gain"] = 2.5

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    header = out_ctx.get(FITSHeader)
    assert header["GAIN"] == 2.5  # mutated to the resolved value
    assert "ABSENT" not in header  # mutate skips keywords not already present
    assert header["NEW"] == "moved" and "OLD" not in header  # renamed
    assert "JUNK" not in header  # removed


@pytest.mark.asyncio
async def test_build_fits_header_include():
    """A declared keyword holding a provider contributes its cards, stored verbatim."""
    op = BuildFITSHeader(include={"_StubCamera"})
    ctx = Context()
    ctx.set(_StubCamera())

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    assert out_ctx.get(FITSHeader) == {
        "INSTRUME": "StubCam",
        "GAIN": FITSCardValueWithComment("1.5", "detector gain"),
    }


@pytest.mark.asyncio
async def test_build_fits_header_include_absent_keyword_is_optional():
    """A declared keyword absent from the context is skipped, so one op serves many pathways."""
    op = BuildFITSHeader(include={"_StubCamera", "_StubTelescope"})
    ctx = Context()
    ctx.set(_StubCamera())  # _StubTelescope never reaches this pathway.

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    header = out_ctx.get(FITSHeader)
    assert header["INSTRUME"] == "StubCam"
    assert "TELESCOP" not in header


@pytest.mark.asyncio
async def test_build_fits_header_include_undeclared_key_is_skipped():
    """An include key that is not a declared keyword (e.g. a typo) contributes nothing."""
    op = BuildFITSHeader(include={"_StubCamera", "NotAKeyword"})
    ctx = Context()
    ctx.set(_StubCamera())

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    assert out_ctx.get(FITSHeader) == {
        "INSTRUME": "StubCam",
        "GAIN": FITSCardValueWithComment("1.5", "detector gain"),
    }


@pytest.mark.asyncio
async def test_build_fits_header_include_non_provider_is_skipped():
    """A declared keyword whose value is not a FITSCardProvider contributes nothing."""
    op = BuildFITSHeader(include={"_StubNotAProvider"})
    ctx = Context()
    ctx.set(_StubNotAProvider())

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    assert out_ctx.get(FITSHeader) == {}


@pytest.mark.asyncio
async def test_build_fits_header_comment_preserved():
    """A [value, comment] card keeps its comment through resolution."""
    op = BuildFITSHeader(define={"CCDTEMP": ["=ccdtemp", "sensor temperature, C"]})
    ctx = Context()
    ctx["ccdtemp"] = -12.3

    out_ctx, _ = await _run_dataop(op, ctx, b"")

    header = out_ctx.get(FITSHeader)
    assert header["CCDTEMP"] == FITSCardValueWithComment(-12.3, "sensor temperature, C")


@pytest.mark.asyncio
async def test_build_fits_header_feeds_array_to_fits():
    """BuildFITSHeader populates the context header that ArrayToFITS then writes out."""
    width, height = 6, 4
    data = np.arange(width * height, dtype=np.uint16)

    ctx = Context()
    ctx.set(ArrayInfo(shape=(height, width), dtype="uint16", order="C"))
    ctx["exptime"] = 30.0

    build = BuildFITSHeader(
        include={"_StubCamera"},
        define={"EXPTIME": ["=exptime", "exposure seconds"]},
    )
    ctx.set(_StubCamera())

    # The raw array bytes pass through BuildFITSHeader unchanged into ArrayToFITS.
    ctx, buffer = await _run_dataop(build, ctx, data.tobytes())

    array_to_fits = ArrayToFITS(header={"OBSERVER": "Erik"})
    out_ctx, fits_buf = await _run_dataop(array_to_fits, ctx, buffer)

    with fits.open(io.BytesIO(fits_buf)) as hdul:
        header = hdul[0].header
        assert np.array_equal(hdul[0].data.flatten(), data)
        assert header["INSTRUME"] == "StubCam"  # from the provider
        assert header["GAIN"] == "1.5"  # provider card stored verbatim
        assert "detector gain" in header.comments["GAIN"]  # provider comment carried through
        assert header["EXPTIME"] == 30.0  # define, resolved against the context
        assert "exposure seconds" in header.comments["EXPTIME"]
        assert header["OBSERVER"] == "Erik"  # added by ArrayToFITS
