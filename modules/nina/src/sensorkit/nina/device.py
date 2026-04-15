from __future__ import annotations

import asyncio
import hashlib
import hmac
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from loguru import logger
from pydantic import BaseModel


@dataclass
class NinaDevice:
    """Generic NINA device."""

    config: NinaDeviceConfig
    device_connected: bool | None = field(default=None, init=False)
    device_name: str = "Device"
    _client: NinaClient | None = field(default=None, init=False, repr=False)
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)

    @property
    def client(self) -> NinaClient:
        if self._client is None:
            self._client = NinaClient(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                timeout=self.config.timeout,
            )
        return self._client

    def require_connected(self):
        if not self.device_connected:
            raise DeviceConnectionError(f"{self.device_name} not connected")

    async def connect(self, equipment_type: str):
        """Connect to a NINA equipment device."""

        logger.debug(f"connecting to {self.device_name}")
        if self.config.username:
            await self.client.login()
        await self.client.get(f"/equipment/{equipment_type}/connect")
        self.device_connected = True
        logger.debug(f"connected to {self.device_name}")

    async def disconnect(self, equipment_type: str):
        """Disconnect from a NINA equipment device."""

        logger.debug(f"disconnecting from {self.device_name}")
        await self.client.get(f"/equipment/{equipment_type}/disconnect")
        self.device_connected = False
        logger.debug(f"disconnected from {self.device_name}")

    async def info(self, equipment_type: str) -> dict:
        return await self.client.get(f"/equipment/{equipment_type}/info")

    def start_status_loop(self, coro):
        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        self._status_task = asyncio.create_task(coro)

    async def stop_status_loop(self):
        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
            self._status_task = None


class NinaClient:
    """Async HTTP client for the NINA Advanced API.

    All NINA API endpoints are GET requests. Authentication uses
    HMAC-SHA256 signing when credentials are configured.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1888,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
    ):
        self.base_url = f"http://{host}:{port}/v2/api"
        self._username = username
        self._password = password
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._session_key: str | None = None

    async def _ensure_client(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)

    def _sign_request(self, url: str) -> dict[str, str]:
        if not self._token or not self._session_key:
            return {}
        signature = hmac.new(self._session_key.encode(), url.encode(), hashlib.sha256).hexdigest()
        return {"Authorization": f"Bearer {self._token}", "X-Signature": signature}

    async def login(self):
        if not self._username or not self._password:
            return
        await self._ensure_client()
        resp = await self._client.get(
            f"{self.base_url}/auth/login",
            params={"username": self._username, "password": self._password},
        )
        data = resp.json()
        if data.get("Success"):
            self._token = data.get("Response", {}).get("Token")
            secret = data.get("Response", {}).get("Secret", "")
            self._session_key = hmac.new(
                self._password.encode(), secret.encode(), hashlib.sha256
            ).hexdigest()
        else:
            raise NinaAPIError(f"Login failed: {data.get('Error', 'Unknown error')}")

    async def get(self, path: str, **params) -> Any:
        await self._ensure_client()
        url = f"{self.base_url}{path}"
        headers = self._sign_request(url)

        resp = await self._client.get(url, params=params, headers=headers)
        resp.raise_for_status()

        data = resp.json()
        if not data.get("Success", True):
            error = data.get("Error", "Unknown error")
            status = data.get("StatusCode", 0)
            raise NinaAPIError(error, status)

        return data.get("Response")

    async def get_raw(self, path: str, **params) -> httpx.Response:
        """Make a GET request and return the raw httpx.Response (for streaming)."""
        await self._ensure_client()
        url = f"{self.base_url}{path}"
        headers = self._sign_request(url)
        return await self._client.get(url, params=params, headers=headers)

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None


class NinaDeviceConfig[T: NinaDevice = NinaDevice](BaseModel):
    """Generic NINA device configuration."""

    device_type: Literal[None] = None
    host: str = "localhost"
    port: int = 1888
    username: str | None = None
    password: str | None = None
    timeout: float = 30.0
    status_frequency: float = 1.0

    def create_device(self) -> T:
        return NinaDevice(self)


class NinaDeviceState(BaseModel):
    """Base persistent state for a NINA device."""

    device_type: Literal[None] = None


class NinaError(Exception):
    """Base exception for NINA device errors."""


class DeviceConnectionError(NinaError):
    """Device is not connected."""


class NinaAPIError(NinaError):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code
