#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Mock UDL: a local UDL-compliant endpoint for developing the udl module.

Run as an independent app (uses the installed sensorkit for astro/backend
helpers but is not a SensorKit service):

    python deploy/simulated/udl/service.py

Configured through MOCK_UDL_* environment variables; the nearest .env is
auto-loaded, so the same .env used for other SensorKit services works. See the
README for the full list. Requires a site: either MOCK_UDL_CONTROLLER
(SitePosition read from that controller's KV over the backend) or
MOCK_UDL_LATITUDE / MOCK_UDL_LONGITUDE / MOCK_UDL_ALTITUDE_KM. The TLS
cert/key always live in certs/ beside this file.
"""

import asyncio
import signal
import ssl
import sys
from pathlib import Path

import uvicorn
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from server import MockUDLSettings, create_app
from tasking import MockTasking, load_catalog

from sensorkit.astro.common import SitePosition
from sensorkit.backend.base import KeyNotFound

# How long to wait for a configured controller to publish SitePosition before
# giving up (it may simply not be running yet).
_SITE_WAIT_S = 30

# TLS material lives beside this file (certs/ is gitignored). The one
# self-signed cert is the server cert, the trust anchor for client-cert auth,
# and the client cert the udl module's use_certs config should point at.
_CERT_DIR = Path(__file__).resolve().parent / "certs"
_CERT = _CERT_DIR / "mock_udl.pem"
_KEY = _CERT_DIR / "mock_udl.key"


async def _resolve_site(settings) -> SitePosition:
    """SitePosition from the configured controller's KV, else from env."""
    if settings.controller:
        from sensorkit.api.bootstrap import connect

        kit = await connect()
        controller = kit.controller(settings.controller)
        for _ in range(_SITE_WAIT_S):
            try:
                return await controller.kv_get_model(SitePosition)
            except KeyNotFound:
                logger.debug(f"waiting for {settings.controller} SitePosition")
                await asyncio.sleep(1)
        raise RuntimeError(
            f"controller {settings.controller} never published SitePosition"
        )
    if None in (settings.latitude, settings.longitude, settings.altitude_km):
        raise RuntimeError(
            "set MOCK_UDL_CONTROLLER, or MOCK_UDL_LATITUDE / MOCK_UDL_LONGITUDE "
            "/ MOCK_UDL_ALTITUDE_KM, so targets can be screened for visibility"
        )
    return SitePosition(
        latitude_degrees=settings.latitude,
        longitude_degrees=settings.longitude,
        altitude_km=settings.altitude_km,
    )


def _server(app, port: int) -> uvicorn.Server:
    # CERT_OPTIONAL with our own cert as the trust anchor: basic-auth clients
    # present nothing, cert clients must present a cert that verifies against
    # the mock's own (shared) certificate.
    return uvicorn.Server(
        uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
            ssl_certfile=str(_CERT),
            ssl_keyfile=str(_KEY),
            ssl_ca_certs=str(_CERT),
            ssl_cert_reqs=ssl.CERT_OPTIONAL,
        )
    )


async def main() -> None:
    load_dotenv(find_dotenv(usecwd=True))

    if not (_CERT.is_file() and _KEY.is_file()):
        raise RuntimeError(
            f"missing {_CERT} / {_KEY}; generate once with:\n"
            f"  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \\\n"
            f'    -keyout {_KEY} -out {_CERT} -subj "/CN=mock-udl" \\\n'
            f'    -addext "subjectAltName=DNS:localhost,DNS:mock-udl,IP:127.0.0.1"'
        )

    settings = MockUDLSettings.from_env()
    site = await _resolve_site(settings)
    catalog = await asyncio.to_thread(load_catalog, settings.tles)

    tasking = MockTasking(
        idle_s=settings.idle_s,
        id_sensor=settings.id_sensor,
        target_types=settings.target_types,
        site=site,
        catalog=catalog,
    )
    app = create_app(settings, tasking)

    ports = [settings.port] + ([settings.upload_port] if settings.upload_port else [])
    servers = [_server(app, port) for port in ports]
    logger.info(
        f"mock UDL: {len(catalog)} TLEs from {settings.tles}; site "
        f"lat={site.latitude_degrees} lon={site.longitude_degrees} "
        f"alt={site.altitude_km}km; targets={settings.target_types}; "
        f"serving https on {ports}"
    )

    # Several uvicorn servers can't each own the process signal handlers;
    # install one set that stops them all.
    for server in servers:
        server.install_signal_handlers = lambda: None
    loop = asyncio.get_running_loop()

    def _stop():
        for server in servers:
            server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    await asyncio.gather(*(server.serve() for server in servers))


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
