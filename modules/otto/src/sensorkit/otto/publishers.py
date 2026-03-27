from __future__ import annotations

import asyncio
import io
from abc import ABC, abstractmethod
from pathlib import Path

from loguru import logger


class Publisher(ABC):
    """Base class for otto data publishers."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def publish(self, context: dict, data: bytes) -> None: ...

    async def close(self) -> None:
        pass


def _filename_from_context(context: dict) -> str:
    """Derive a filename from the DataGraph context."""
    file_path = context.get("file_path")
    if file_path:
        return Path(file_path).name
    task_id = context.get("task_id", "unknown")
    frame_num = context.get("frame_num", 0)
    return f"{task_id}_{frame_num}.fits"


class GDrivePublisher(Publisher):
    """Uploads FITS files to a Google Drive folder using OAuth2 credentials."""

    def __init__(self, config):
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        self._config = config
        creds = Credentials.from_authorized_user_file(
            config.token_file,
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

    def __init__(self, config):
        import dropbox

        self._config = config
        self._dbx = dropbox.Dropbox(
            app_key=config.app_key,
            app_secret=config.app_secret,
            oauth2_refresh_token=config.refresh_token,
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
    """Stub — will be replaced with udl-sdk implementation."""

    def __init__(self, config):
        self._config = config

    @property
    def name(self) -> str:
        return "udl"

    async def publish(self, context: dict, data: bytes) -> None:
        raise NotImplementedError("UDL publisher not yet implemented — see udl module branch")

    async def close(self) -> None:
        pass
