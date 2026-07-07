# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import io
import json
import os
import zipfile
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import dotenv_values
from loguru import logger

from sensorkit.data.filesys import FileInfo


def _udl_ts(dt: datetime) -> str:
    """Format a datetime as UDL expects: ISO 8601 UTC with trailing 'Z' (no offset)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _env_credentials(env_file: str, *keys: str) -> dict[str, str | None]:
    """Read credential keys from env_file, falling back to the process environment."""
    env = dotenv_values(env_file)
    return {key: env.get(key) or os.environ.get(key) for key in keys}


class Publisher(ABC):
    """Base class for otto data publishers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifies this publisher in logs and error messages."""

    @abstractmethod
    async def publish(self, context: dict, data: bytes) -> None:
        """Deliver one frame to the destination.

        Args:
            context: DataGraph sink metadata (task_id, frame_num, FileInfo, …).
            data: Raw FITS bytes from the sink's async stream.
        """

    async def close(self) -> None:
        """Release connections and resources after all frames are published."""
        pass


def _filename_from_context(context: dict) -> str:
    """Derive a filename from the DataGraph context."""
    info = context.get(FileInfo)
    if info:
        return info.path.name
    task_id = context.get("task_id", "unknown")
    frame_num = context.get("frame_num", 0)
    return f"{task_id}_{frame_num}.fits"


class GDrivePublisher(Publisher):
    """Uploads FITS files to a Google Drive folder using OAuth2 credentials."""

    def __init__(self, config, *, env_file: str = ".env"):
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        self._config = config

        token_path = _env_credentials(env_file, "GDRIVE_TOKEN_PATH")["GDRIVE_TOKEN_PATH"]
        if not token_path:
            raise RuntimeError(
                f"GDRIVE_TOKEN_PATH must be set in {env_file} or as an environment variable"
            )

        creds = Credentials.from_authorized_user_file(
            token_path,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        self._service = build("drive", "v3", credentials=creds)

    @property
    def name(self) -> str:
        return "gdrive"

    async def publish(self, context: dict, data: bytes) -> None:
        from googleapiclient.http import MediaIoBaseUpload

        filename = _filename_from_context(context)
        media = MediaIoBaseUpload(
            io.BytesIO(data), mimetype="application/octet-stream", resumable=False
        )
        metadata = {"name": filename, "parents": [self._config.folder_id]}

        result = await asyncio.to_thread(
            self._service.files()
            .create(body=metadata, media_body=media, fields="id")
            .execute
        )
        logger.debug(f"GDrive uploaded {filename} (id={result.get('id')})")


class DropboxPublisher(Publisher):
    """Uploads FITS files to Dropbox using OAuth2 refresh-token flow."""

    def __init__(self, config, *, env_file: str = ".env"):
        import dropbox

        self._config = config

        creds = _env_credentials(
            env_file, "DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN"
        )
        if missing := [key for key, value in creds.items() if not value]:
            raise RuntimeError(
                f"{', '.join(missing)} must be set in {env_file} or as environment variables"
            )

        self._dbx = dropbox.Dropbox(
            app_key=creds["DROPBOX_APP_KEY"],
            app_secret=creds["DROPBOX_APP_SECRET"],
            oauth2_refresh_token=creds["DROPBOX_REFRESH_TOKEN"],
        )

    @property
    def name(self) -> str:
        return "dropbox"

    async def publish(self, context: dict, data: bytes) -> None:
        import dropbox as dbx_module

        filename = _filename_from_context(context)
        dest_path = f"{self._config.upload_path}/{filename}"

        await asyncio.to_thread(
            self._dbx.files_upload,
            bytes(data),
            dest_path,
            mode=dbx_module.files.WriteMode.overwrite,
        )
        logger.debug(f"Dropbox uploaded {filename} to {dest_path}")

    async def close(self) -> None:
        if self._dbx:
            self._dbx.close()


class UDLPublisher(Publisher):
    """Uploads FITS frames to the UDL SkyImagery filedrop.

    Each frame is zipped together with a SkyImagery metadata JSON and POSTed as
    raw ``application/zip`` through the udl-sdk client's HTTP transport — the
    payload proven against the live UDL filedrop. The SDK's own
    ``sky_imagery.upload_zip()`` is avoided: it sends multipart/form-data and
    mis-targets the imagery filedrop host for non-default base URLs.
    """

    def __init__(self, config, *, env_file: str = ".env", frame_count: int = 1, site=None):
        from unifieddatalibrary import AsyncUnifieddatalibrary

        self._config = config
        self._frame_count = frame_count
        self._site = site

        creds = _env_credentials(env_file, "UDL_USERNAME", "UDL_PASSWORD")
        username, password = creds["UDL_USERNAME"], creds["UDL_PASSWORD"]
        if bool(username) != bool(password):
            raise RuntimeError(
                f"UDL_USERNAME and UDL_PASSWORD must both be set (or both omitted) "
                f"in {env_file} or as environment variables"
            )
        if not username:
            logger.warning(
                "no UDL credentials configured; publishing unauthenticated "
                f"to {self._imagery_base_url()}"
            )

        self._sdk = AsyncUnifieddatalibrary(
            username=username,
            password=password,
            base_url=self._imagery_base_url(),
            timeout=config.upload_timeout,
        )

    @property
    def name(self) -> str:
        return "udl"

    def _imagery_base_url(self) -> str:
        """Resolve the SkyImagery filedrop host.

        UDL serves the imagery filedrop on a dedicated subdomain, so the known
        UDL hosts map to their imagery counterparts. Any other base URL (a
        custom UDL-compliant endpoint) serves the filedrop on the same host.
        """
        base = self._config.base_url
        if not base:
            return "https://imagery.unifieddatalibrary.com"

        host = base.rstrip("/").split("://", 1)[-1]
        if host == "test.unifieddatalibrary.com":
            return "https://imagery-test.unifieddatalibrary.com"
        if host == "unifieddatalibrary.com":
            return "https://imagery.unifieddatalibrary.com"
        return base.rstrip("/")

    def _build_metadata(self, context: dict, data: bytes, filename: str) -> dict:
        """Build the SkyImagery metadata record for one frame."""
        date_obs = context.get("date_obs")
        exp_start = datetime.fromisoformat(date_obs) if date_obs else datetime.now(UTC)

        exptime = context.get("exptime")
        exp_end = (
            exp_start + timedelta(seconds=float(exptime)) if exptime is not None else exp_start
        )

        metadata = {
            "classificationMarking": self._config.classification_marking,
            "idSensor": self._config.id_sensor,
            "satNo": context.get("sat_no"),
            "expStartTime": _udl_ts(exp_start),
            "expEndTime": _udl_ts(exp_end),
            "imageSetLength": self._frame_count,
            # sequenceId must be >= 1 (UDL requirement)
            "sequenceId": context.get("frame_num", 0) + 1,
            "frameWidthPixels": context.get("image_width"),
            "frameHeightPixels": context.get("image_height"),
            "filename": filename,
            "filesize": len(data),
            "source": self._config.source,
            "dataMode": self._config.data_mode,
            "imageType": context.get("image_type") or self._config.image_type,
        }

        # imageSetId groups multiple frames of one collect into a set. Per UDL:
        # a single-image set doesn't need an imageSetId.
        if self._frame_count > 1 and context.get("task_id"):
            metadata["imageSetId"] = str(context["task_id"])

        if self._site:
            metadata["senlat"] = self._site.latitude_degrees
            metadata["senlon"] = self._site.longitude_degrees
            metadata["senalt"] = self._site.altitude_km

        return {k: v for k, v in metadata.items() if v is not None}

    async def publish(self, context: dict, data: bytes) -> None:
        filename = _filename_from_context(context)
        metadata = self._build_metadata(context, data, filename)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{Path(filename).stem}_skyimagery.json", json.dumps(metadata).encode())
            zf.writestr(filename, data)

        # POST the raw ZIP through the SDK's internal httpx client (bound to the
        # imagery host, with its connection pool and timeout). The filedrop
        # accepts exactly "a zip", so auth rides the SDK's Basic-auth headers.
        resp = await self._sdk._client.post(
            "/filedrop/udl-skyimagery",
            content=zip_buffer.getvalue(),
            headers={"Content-Type": "application/zip", **self._sdk.auth_headers},
        )
        if resp.status_code >= 300:
            raise RuntimeError(
                f"SkyImagery upload failed: HTTP {resp.status_code} {resp.text}"
            )

        logger.debug(
            f"UDL uploaded {filename} ({metadata['sequenceId']}/{self._frame_count})"
        )

    async def close(self) -> None:
        await self._sdk.close()
