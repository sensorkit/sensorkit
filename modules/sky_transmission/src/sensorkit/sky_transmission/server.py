# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

from sensorkit.sky_transmission.models import FrameState


async def start_image_server(host: str, port: int, state: FrameState):
    """Serve the annotated all-sky frame as an MJPEG stream.

    Only the MJPEG stream lives here — it's the one thing SensorKit's pub/sub +
    webapi can't carry (binary video frames). SkyTransmission metrics are
    published as a keyword (surfaced by the webapi), and raw/processed image
    viewing is handled by the webapi, so there are no /status or /latest.* HTTP
    endpoints. SensorView's Streams tab consumes this directly as an MJPEG
    source (`multipart/x-mixed-replace`) at `http://<host>:<port>/stream`.
    """
    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse
    from uvicorn import Config, Server

    app = FastAPI(title="SkyTransmission")

    @app.get("/stream")
    async def mjpeg_stream():
        return StreamingResponse(
            _mjpeg_generator(state),
            media_type="multipart/x-mixed-replace; boundary=frame",
        )

    config = Config(app=app, host=host, port=port, log_level="warning")
    server = Server(config)
    await server.serve()


async def _mjpeg_generator(state: FrameState):
    """Yield MJPEG frames as they become available."""
    last_id = 0
    while True:
        current_id = state.frame_id
        if current_id > last_id and state.latest_jpeg:
            last_id = current_id
            data = state.latest_jpeg
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: " + str(len(data)).encode() + b"\r\n"
                b"\r\n" + data + b"\r\n"
            )
        await asyncio.sleep(1.0)
