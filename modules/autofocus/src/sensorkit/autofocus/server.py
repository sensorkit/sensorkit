from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sensorkit.autofocus.analyzer import AutofocusAnalyzer
    from sensorkit.autofocus.program import AutofocusProgram


async def start_vcurve_server(
    host: str,
    port: int,
    analyzer: AutofocusAnalyzer,
    program: AutofocusProgram,
):
    """Start the FastAPI/uvicorn autofocus server."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    from uvicorn import Config, Server

    app = FastAPI(title="Autofocus")

    class VCurveCommand(BaseModel):
        target_ra: float | None = None
        target_dec: float | None = None

    @app.post("/vcurve")
    async def vcurve(cmd: VCurveCommand | None = None):
        target = None
        if cmd and cmd.target_ra is not None and cmd.target_dec is not None:
            from sensorkit.astro.coords import Equatorial
            from sensorkit.astro.target import ICRSTarget

            target = ICRSTarget(coords=Equatorial(ra=cmd.target_ra, dec=cmd.target_dec))

        await program.queue_vcurve(target=target)
        return JSONResponse({"status": "queued"})

    config = Config(app=app, host=host, port=port, log_level="warning")
    server = Server(config)
    await server.serve()
