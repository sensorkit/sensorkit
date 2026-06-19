from __future__ import annotations

import asyncio
import contextlib
import json
import pathlib

import httpx
import numpy as np
import pytest
import pytest_asyncio
from astropy.io import fits

from sensorkit.webapi import fastapi as webapi_mod
from sensorkit.webapi.fastapi import WebAPI, WebAPIConfig
from sensorkit.webapi.forwarder import ProductForwarder, SKRecord
from sensorkit.webapi.preview import PreviewJPEG
from sensorkit.webapi.serve import (
    ServeLocalFITSConfig,
    ServeLocalFITSHandler,
    _header_to_dict,
)


def make_fits(path: pathlib.Path, data=None, *, comments=(), history=(), **header) -> pathlib.Path:
    """Write a minimal FITS file at *path* with the given header cards."""
    if data is None:
        data = np.arange(64, dtype=np.float32).reshape(8, 8)

    hdu = fits.PrimaryHDU(data=data)
    for key, value in header.items():
        hdu.header[key] = value
    for comment in comments:
        hdu.header.add_comment(comment)
    for entry in history:
        hdu.header.add_history(entry)

    path.parent.mkdir(parents=True, exist_ok=True)
    hdu.writeto(path, overwrite=True)
    return path


@contextlib.asynccontextmanager
async def running_handler(config: ServeLocalFITSConfig):
    """Start a ServeLocalFITSHandler and wait until its initial listing is ready."""
    handler = ServeLocalFITSHandler(config)

    async with asyncio.TaskGroup() as tg:
        handler.start_monitor(task_group=tg)

        try:
            async with asyncio.timeout(5.0):
                await handler.get_listing()

            yield handler
        finally:
            await handler.stop_monitor()


# ---------------------------------------------------------------------------
# Header conversion
# ---------------------------------------------------------------------------

def test_header_to_dict_is_json_serializable():
    hdu = fits.PrimaryHDU(data=np.zeros((4, 4), dtype=np.float32))
    hdu.header["SKCTRL"] = "ctrlA"
    hdu.header["EXPTIME"] = 1.5
    hdu.header["TRACKING"] = True
    hdu.header.add_comment("a comment")
    hdu.header.add_history("made by test")

    result = _header_to_dict(hdu.header)

    assert result["SKCTRL"] == "ctrlA"
    assert result["EXPTIME"] == 1.5
    assert result["TRACKING"] is True
    assert "a comment" in result["COMMENT"]
    assert "made by test" in result["HISTORY"]

    # The whole thing must round-trip through JSON without error.
    json.dumps(result)


# ---------------------------------------------------------------------------
# Preview rendering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_preview_renders_jpeg(tmp_path):
    raw = make_fits(tmp_path / "a.fits").read_bytes()

    preview = await PreviewJPEG.from_fits(raw)

    assert preview.jpeg_bytes[:3] == b"\xff\xd8\xff"  # JPEG SOI marker


@pytest.mark.asyncio
async def test_preview_caches_by_checksum(tmp_path):
    raw = make_fits(tmp_path / "a.fits").read_bytes()

    first = await PreviewJPEG.from_fits(raw)
    second = await PreviewJPEG.from_fits(raw)

    # Identical content is rendered once and shared.
    assert first is second


@pytest.mark.asyncio
async def test_preview_invalid_fits_raises():
    with pytest.raises(Exception):
        await PreviewJPEG.from_fits(b"definitely not a FITS file")


# ---------------------------------------------------------------------------
# ServeLocalFITSHandler
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handler_lists_products_from_path(tmp_path):
    make_fits(tmp_path / "ctrlA" / "frame1.fits")
    make_fits(tmp_path / "ctrlB" / "frame2.fits")

    config = ServeLocalFITSConfig(root_directory=str(tmp_path), controller_id="from_path")

    async with running_handler(config) as handler:
        listing = await handler.get_listing()

        assert ("ctrlA", "frame1.fits") in listing
        assert ("ctrlB", "frame2.fits") in listing

        assert handler.has_product("ctrlA", "frame1.fits")
        assert not handler.has_product("ctrlA", "missing.fits")
        assert not handler.has_product("unknown", "frame1.fits")


@pytest.mark.asyncio
async def test_handler_controller_from_metadata(tmp_path):
    make_fits(tmp_path / "x.fits", SKCTRL="metactrl")

    config = ServeLocalFITSConfig(root_directory=str(tmp_path), controller_id="from_metadata")

    async with running_handler(config) as handler:
        listing = await handler.get_listing()

        assert ("metactrl", "x.fits") in listing
        assert handler.get_metadata("metactrl", "x.fits")["SKCTRL"] == "metactrl"


@pytest.mark.asyncio
async def test_handler_get_data_returns_file_bytes(tmp_path):
    path = make_fits(tmp_path / "ctrlA" / "frame1.fits")

    config = ServeLocalFITSConfig(root_directory=str(tmp_path), controller_id="from_path")

    async with running_handler(config) as handler:
        assert await handler.get_data("ctrlA", "frame1.fits") == path.read_bytes()


# ---------------------------------------------------------------------------
# ProductForwarder
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_product_forwarder_emits_initial_and_live(tmp_path):
    make_fits(tmp_path / "ctrlA" / "frame1.fits")

    config = ServeLocalFITSConfig(root_directory=str(tmp_path), controller_id="from_path")
    queue: asyncio.Queue[SKRecord | None] = asyncio.Queue()

    async with asyncio.TaskGroup() as tg:
        handler = ServeLocalFITSHandler(config)
        handler.start_monitor(task_group=tg)

        forwarder = ProductForwarder(handler, targets={queue})
        await forwarder.start(task_group=tg)

        try:
            # The already-present product is forwarded as a "product" record.
            async with asyncio.timeout(5.0):
                record = await queue.get()

            assert record is not None
            assert record.kind == "product"
            assert str(record.subject.entity()) == "ctrlA"
            assert record.subject.prop == "frame1.fits"
            assert record.payload is not None

            # A newly discovered product is forwarded too.
            await handler._found_file(make_fits(tmp_path / "ctrlA" / "frame2.fits"))

            async with asyncio.timeout(5.0):
                while (record := await queue.get()) is not None:
                    if record.subject.prop == "frame2.fits":
                        break

            assert record is not None
            assert record.kind == "product"
        finally:
            await forwarder.stop()
            await handler.stop_monitor()


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def product_api(kit, tmp_path):
    """WebAPI serving a single FITS product under controller ``ctrlA``."""
    # Include COMMENT/HISTORY cards: a raw fits.Header carrying these stalls JSON
    # serialization, so this guards the metadata/SSE routes against that regression.
    make_fits(
        tmp_path / "ctrlA" / "frame1.fits",
        EXPTIME=2.0,
        comments=["a comment"],
        history=["created by test"],
    )

    config = WebAPIConfig(
        serve_data_products=[
            ServeLocalFITSConfig(root_directory=str(tmp_path), controller_id="from_path"),
        ]
    )
    webapi = WebAPI(kit, config)

    async with asyncio.TaskGroup() as tg:
        await webapi._start_forwarders(task_group=tg)

        # Wait until each handler's initial listing is ready so route assertions are
        # deterministic (the scan runs as a background task).
        async with asyncio.timeout(5.0):
            for handler in webapi._serve_handlers:
                await handler.get_listing()

        transport = httpx.ASGITransport(app=webapi.app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")

        try:
            yield webapi, client, tmp_path
        finally:
            await client.aclose()
            await webapi.shutdown()


@pytest.mark.asyncio
async def test_route_list_products(product_api):
    _, client, _ = product_api

    resp = await client.get("/controller/ctrlA/products")
    assert resp.status_code == 200
    assert resp.json() == ["frame1.fits"]


@pytest.mark.asyncio
async def test_route_list_products_unknown_controller(product_api):
    _, client, _ = product_api

    resp = await client.get("/controller/unknown/products")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_route_get_data(product_api):
    _, client, tmp_path = product_api

    resp = await client.get("/controller/ctrlA/product/frame1.fits/data")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/fits"
    assert resp.content == (tmp_path / "ctrlA" / "frame1.fits").read_bytes()


@pytest.mark.asyncio
async def test_route_get_metadata(product_api):
    _, client, _ = product_api

    resp = await client.get("/controller/ctrlA/product/frame1.fits/metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert body["EXPTIME"] == 2.0
    # Commentary cards must survive as JSON-safe lists (not raw header objects).
    assert "a comment" in body["COMMENT"]
    assert "created by test" in body["HISTORY"]


@pytest.mark.asyncio
async def test_route_get_preview(product_api):
    _, client, _ = product_api

    resp = await client.get("/controller/ctrlA/product/frame1.fits/preview")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/jpeg"
    assert resp.content[:3] == b"\xff\xd8\xff"


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["data", "preview", "metadata"])
async def test_route_unknown_product_returns_404(product_api, suffix):
    _, client, _ = product_api

    resp = await client.get(f"/controller/ctrlA/product/missing.fits/{suffix}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_route_list_products_503_when_not_ready(kit, tmp_path, monkeypatch):
    # Point at a directory that does not exist, so the listing never becomes ready.
    monkeypatch.setattr(webapi_mod, "PRODUCT_LISTING_TIMEOUT", 0.2)

    config = WebAPIConfig(
        serve_data_products=[
            ServeLocalFITSConfig(
                root_directory=str(tmp_path / "does_not_exist"),
                controller_id="from_path",
            ),
        ]
    )
    webapi = WebAPI(kit, config)

    async with asyncio.TaskGroup() as tg:
        await webapi._start_forwarders(task_group=tg)

        transport = httpx.ASGITransport(app=webapi.app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")

        try:
            resp = await client.get("/controller/ctrlA/products")
            assert resp.status_code == 503
        finally:
            await client.aclose()
            await webapi.shutdown()
