# SPDX-License-Identifier: Apache-2.0
"""FastAPI app for the mock UDL endpoint: validate inbound, discard, respond."""

from __future__ import annotations

import base64
import binascii
import io
import json
import os
import zipfile
from dataclasses import dataclass
from email import message_from_bytes

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import ValidationError
from starlette.requests import ClientDisconnect
from tasking import MockTasking
from unifieddatalibrary.types import CollectResponseFull, EoObservationFull
from unifieddatalibrary.types.sky_imagery_get_response import SkyImageryGetResponse

_TARGET_TYPES = ("tle", "sv", "radec")


@dataclass(frozen=True)
class MockUDLSettings:
    """MOCK_UDL_* environment configuration."""

    port: int
    upload_port: int | None
    idle_s: float
    id_sensor: str
    target_types: tuple[str, ...]
    username: str
    password: str
    controller: str | None
    latitude: float | None
    longitude: float | None
    altitude_km: float | None
    tles: str

    @classmethod
    def from_env(cls) -> MockUDLSettings:
        def env(name: str, default: str | None = None) -> str | None:
            # Empty means unset: compose-style passthrough (VAR: ${VAR:-})
            # materializes absent variables as empty strings.
            return os.environ.get(name) or default
        target_types = tuple(
            t.strip() for t in env("MOCK_UDL_TARGET_TYPE", "tle").split(",") if t.strip()
        )
        unknown = set(target_types) - set(_TARGET_TYPES)
        if unknown or not target_types:
            raise RuntimeError(
                f"MOCK_UDL_TARGET_TYPE must be a comma list of {_TARGET_TYPES}, "
                f"got {env('MOCK_UDL_TARGET_TYPE')!r}"
            )
        coord = lambda name: float(env(name)) if env(name) is not None else None  # noqa: E731
        return cls(
            port=int(env("MOCK_UDL_PORT", "9000")),
            # When set, the whole app is also served on this port, so the udl
            # module's api.upload split (SkyImagery to a separate base_url)
            # can be exercised against a genuinely different origin.
            upload_port=(
                int(env("MOCK_UDL_UPLOAD_PORT")) if env("MOCK_UDL_UPLOAD_PORT") else None
            ),
            idle_s=float(env("MOCK_UDL_IDLE_S", "0")),
            id_sensor=env("MOCK_UDL_ID_SENSOR", "MockSensor"),
            target_types=target_types,
            username=env("MOCK_UDL_USERNAME", "udl"),
            password=env("MOCK_UDL_PASSWORD", "udl"),
            controller=env("MOCK_UDL_CONTROLLER"),
            latitude=coord("MOCK_UDL_LATITUDE"),
            longitude=coord("MOCK_UDL_LONGITUDE"),
            altitude_km=coord("MOCK_UDL_ALTITUDE_KM"),
            # Spacebook: full public catalog, no credentials required.
            tles=env("MOCK_UDL_TLES", "https://spacebook.com/api/entity/tle"),
        )


def create_app(settings: MockUDLSettings, tasking: MockTasking) -> FastAPI:
    app = FastAPI(title="Mock UDL", docs_url=None, redoc_url=None)

    async def require_auth(request: Request) -> None:
        """Basic auth must match when presented; absent means cert/anon.

        Client certificates are vetted at the TLS layer (a presented cert must
        chain to the mock's own cert); uvicorn doesn't surface the peer cert to
        the app, so a request with no Authorization header is let through.
        """
        header = request.headers.get("Authorization")
        if header is None:
            return
        try:
            scheme, _, encoded = header.partition(" ")
            username, _, password = (
                base64.b64decode(encoded, validate=True).decode().partition(":")
            )
        except (binascii.Error, UnicodeDecodeError):
            raise HTTPException(
                status_code=401, detail="Malformed Authorization header"
            ) from None
        if scheme != "Basic" or (username, password) != (
            settings.username,
            settings.password,
        ):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    def reject(what: str, error: Exception) -> HTTPException:
        logger.warning(f"rejected {what}: {error}")
        return HTTPException(status_code=400, detail=f"{what} failed validation: {error}")

    @app.get("/udl/collectrequest", dependencies=[Depends(require_auth)])
    async def list_collect_requests(request: Request) -> JSONResponse:
        return JSONResponse(tasking.list_requests(dict(request.query_params)))

    @app.post(
        "/udl/collectresponse", status_code=201, dependencies=[Depends(require_auth)]
    )
    async def create_collect_response(request: Request) -> Response:
        payload = await _json_body(request)
        try:
            CollectResponseFull.model_validate(payload)
        except ValidationError as e:
            raise reject("CollectResponse", e) from e
        logger.info(
            f"CollectResponse {payload.get('status')} for request "
            f"{payload.get('idRequest')}"
        )
        tasking.note_response(payload.get("idRequest"), payload.get("status"))
        return Response(status_code=201)

    # The UDL EO ingest the SDK's unvalidated_publish targets; createBulk is
    # kept as the REST alias some clients still use.
    @app.post("/filedrop/udl-eo", status_code=202, dependencies=[Depends(require_auth)])
    @app.post(
        "/udl/eoobservation/createBulk",
        status_code=204,
        dependencies=[Depends(require_auth)],
    )
    async def create_eo_observations(request: Request) -> Response:
        payload = await _json_body(request)
        if not isinstance(payload, list):
            raise reject("EOObservation bulk", ValueError("body must be a JSON array"))
        for i, record in enumerate(payload):
            try:
                EoObservationFull.model_validate(record)
            except ValidationError as e:
                raise reject(f"EOObservation [{i}]", e) from e
        logger.info(f"accepted {len(payload)} EO observation(s)")
        return Response(status_code=204)

    @app.post(
        "/filedrop/udl-skyimagery",
        status_code=202,
        dependencies=[Depends(require_auth)],
    )
    async def upload_sky_imagery(request: Request) -> Response:
        try:
            body = await request.body()
        except ClientDisconnect:
            # The uploader vanished mid-request (e.g. its stack restarted);
            # there is no one left to answer.
            return Response(status_code=499)
        zip_bytes = _zip_from_request(body, request.headers.get("Content-Type", ""))
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
                names = archive.namelist()
                json_names = [n for n in names if n.endswith(".json")]
                if len(names) != 2 or len(json_names) != 1:
                    raise ValueError(
                        f"zip must contain exactly one .json and one image file, "
                        f"got {names}"
                    )
                metadata = json.loads(archive.read(json_names[0]))
                SkyImageryGetResponse.model_validate(metadata)
        except (zipfile.BadZipFile, ValueError, ValidationError) as e:
            raise reject("SkyImagery", e) from e
        logger.info(
            f"accepted SkyImagery {metadata.get('filename')} "
            f"({metadata.get('sequenceId')}/{metadata.get('imageSetLength')})"
        )
        return Response(status_code=202)

    return app


async def _json_body(request: Request):
    try:
        return await request.json()
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}") from e


def _zip_from_request(body: bytes, content_type: str) -> bytes:
    """Accept the zip as raw application/zip (per the UDL filedrop contract) or
    as the multipart/form-data 'file' part the SDK's upload_zip sends."""
    if not content_type.startswith("multipart/"):
        return body
    message = message_from_bytes(
        b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body
    )
    for part in message.walk():
        if 'name="file"' in part.get("Content-Disposition", ""):
            return part.get_payload(decode=True)
    raise HTTPException(status_code=400, detail="multipart body has no 'file' part")
