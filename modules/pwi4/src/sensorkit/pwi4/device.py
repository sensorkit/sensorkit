from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from loguru import logger
from pydantic import BaseModel


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in {"true", "1", "yes"}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


@dataclass
class PWI4Device:
    """Generic PWI4 device."""

    config: PWI4DeviceConfig
    client: PWI4Client
    device_connected: bool | None = field(default=None, init=False)
    device_name: str = "Device"
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)

    def require_connected(self):
        if not self.device_connected:
            raise DeviceConnectionError(f"{self.device_name} not connected")

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


class PWI4Client:
    """Async HTTP client for the PWI4 API.

    All PWI4 endpoints return a flat key=value text response that represents
    the full system status. This client parses that into a dict and provides
    typed accessors.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8220,
        timeout: float = 10.0,
    ):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self):
        await self._client.aclose()

    async def request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        postdata: Any = None,
    ) -> dict[str, str]:
        """Issue a request and return the parsed flat status dict."""
        url = f"{self.base_url}{endpoint}"
        if postdata is not None:
            resp = await self._client.post(url, params=params, data=postdata)
        else:
            resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return self._parse_response(resp.content)

    async def request_raw(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        postdata: Any = None,
    ) -> bytes:
        """Issue a request and return raw response bytes."""
        url = f"{self.base_url}{endpoint}"
        if postdata is not None:
            resp = await self._client.post(url, params=params, data=postdata)
        else:
            resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.content

    async def status(self) -> dict[str, str]:
        return await self.request("/status")

    async def poll(
        self,
        condition,
        interval: float = 1.0,
        delay: float = 0.1,
    ) -> dict[str, str]:
        """Poll status until condition(status_dict) returns True."""
        await asyncio.sleep(delay)
        while True:
            st = await self.status()
            if condition(st):
                return st
            await asyncio.sleep(interval)

    @staticmethod
    def _parse_response(data: bytes) -> dict[str, str]:
        result: dict[str, str] = {}
        for line in data.decode("utf-8", errors="ignore").split("\n"):
            if not line:
                continue
            name, sep, value = line.partition("=")
            if sep:
                result[name.strip()] = value.strip()
        return result

    # ── Convenience typed accessors for status dicts ──

    @staticmethod
    def get_bool(st: dict, key: str, default: bool = False) -> bool:
        return _to_bool(st.get(key), default)

    @staticmethod
    def get_float(st: dict, key: str, default: float = 0.0) -> float:
        return _to_float(st.get(key), default)

    @staticmethod
    def get_int(st: dict, key: str, default: int = 0) -> int:
        return _to_int(st.get(key), default)

    @staticmethod
    def get_str(st: dict, key: str, default: str = "") -> str:
        return st.get(key, default)


class PWI4DeviceConfig[T: PWI4Device = PWI4Device](BaseModel):
    """Generic PWI4 device configuration."""

    device_type: Literal[None] = None
    host: str = "localhost"
    port: int = 8220
    timeout: float = 60.0
    status_frequency: float = 1.0

    def create_device(self, client: PWI4Client) -> T:
        return PWI4Device(config=self, client=client)


class PWI4DeviceState(BaseModel):
    """Generic PWI4 device state."""

    device_type: Literal[None] = None


class PWI4Error(Exception):
    """Base exception for PWI4 device errors."""


class DeviceConnectionError(PWI4Error):
    """Device is not connected."""
