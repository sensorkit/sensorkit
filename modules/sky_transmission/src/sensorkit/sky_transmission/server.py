from __future__ import annotations

import asyncio
import pathlib

from sensorkit.sky_transmission.models import FrameState


async def start_image_server(host: str, port: int, state: FrameState):
    """Start the FastAPI/uvicorn image server."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse, Response, StreamingResponse
    from starlette.responses import FileResponse
    from uvicorn import Config, Server

    app = FastAPI(title="SkyTransmission")

    @app.get("/latest.jpg")
    async def latest_jpg():
        data = state.latest_jpeg
        if not data:
            return Response(status_code=204)
        return Response(content=data, media_type="image/jpeg")

    @app.get("/latest.mp4")
    async def latest_mp4():
        mp4_path = pathlib.Path(state.output_dir) / "latest.mp4"
        if not mp4_path.exists():
            return Response(status_code=204)
        return FileResponse(str(mp4_path), media_type="video/mp4")

    @app.get("/status")
    async def status():
        return JSONResponse(
            {
                "clear_fraction": state.clear_fraction,
                "n_matched": state.n_matched,
                "n_expected": state.n_expected,
                "status": state.solve_status,
            }
        )

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
