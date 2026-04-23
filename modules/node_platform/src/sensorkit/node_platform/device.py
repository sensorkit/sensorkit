from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

import ourskyai_node_platform_api as osapi
from dotenv import dotenv_values
from loguru import logger
from pydantic import BaseModel


@dataclass
class NodePlatformDevice:
    """Generic Node Platform device."""

    config: NodePlatformDeviceConfig
    _api: NodePlatformAPI | None = field(default=None, init=False)

    device_connected: bool | None = field(default=None, init=False)
    device_name: str = "Device"
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)

    @property
    def api(self) -> NodePlatformAPI:
        """Lazily create and return the SDK API wrapper."""

        if self._api is None:
            env = dotenv_values(self.config.env_file)
            api_key = env.get("NODE_PLATFORM_API_KEY") or os.environ.get("NODE_PLATFORM_API_KEY")

            if not api_key:
                raise RuntimeError(
                    f"NODE_PLATFORM_API_KEY must be set in "
                    f"{self.config.env_file} or as an environment variable"
                )

            self._api = NodePlatformAPI(
                host=self.config.host,
                port=self.config.port,
                api_key=api_key,
                lineage_id=self.config.lineage_id,
            )
        return self._api

    def start_status_loop(self, coro):
        """Start a background status publishing task, cancelling any existing one."""

        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        self._status_task = asyncio.create_task(coro)

    async def stop_status_loop(self):
        """Cancel the background status publishing task."""

        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
            self._status_task = None

    def require_connected(self):
        """Raise DeviceConnectionError if device is not connected."""
        if not self.device_connected:
            raise DeviceConnectionError(code=-3, message=f"{self.device_name} not connected")


class NodePlatformAPI:
    """Async wrapper around the OurSky Node Platform SDK.

    The SDK is synchronous (urllib3-backed), so every call is dispatched
    via ``asyncio.to_thread`` to avoid blocking the event loop.
    """

    def __init__(
        self,
        host: str,
        port: int = 9080,
        api_key: str | None = None,
        lineage_id: str = "",
    ) -> None:
        self.lineage_id = lineage_id

        base_url = f"http://{host}:{port}"
        self._configuration = osapi.Configuration(
            host=base_url,
            access_token=api_key,
        )
        self._client = osapi.ApiClient(self._configuration)
        self._sdk = osapi.DefaultApi(self._client)

    async def call(self, method_name: str, *args, **kwargs) -> Any:
        """Call an SDK method asynchronously.

        The ``lineage_id`` keyword is injected automatically when not
        explicitly provided.
        """

        if "lineage_id" not in kwargs and self.lineage_id:
            kwargs["lineage_id"] = self.lineage_id

        method = getattr(self._sdk, method_name)
        try:
            return await asyncio.to_thread(method, *args, **kwargs)
        except osapi.ApiException as exc:
            raise NodePlatformError.from_api_exception(exc) from exc

    async def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass


class NodePlatformDeviceConfig[T: NodePlatformDevice = NodePlatformDevice](BaseModel):
    """Generic Node Platform device configuration."""

    device_type: Literal[None] = None
    host: str
    port: int = 9080
    lineage_id: str | None = None
    request_timeout: float = 30.0
    env_file: str = ".env"
    operation_mode: Literal["manual", "assisted"] = "assisted"

    def create_device(self) -> T:
        return NodePlatformDevice(self)


class NodePlatformDeviceState(BaseModel):
    """Generic Node Platform device state."""

    device_type: Literal[None] = None


class NodePlatformError(Exception):
    subtypes: ClassVar[dict[int, type[NodePlatformError]]] = {}
    code: int = 0

    def __new__(cls, code: int, message: str):
        return super().__new__(cls.subtypes.get(code, cls), message)

    def __init__(self, message: str, *, code: int = 0):
        if type(self) is NodePlatformError and code is not None:
            self.code = code
        super().__init__(message)

    def __init_subclass__(cls):
        cls.subtypes[cls.code] = cls

    @classmethod
    def from_api_exception(cls, exc: osapi.ApiException) -> NodePlatformError:
        status = getattr(exc, "status", None) or 0
        body = getattr(exc, "body", "") or str(exc)
        return cls(code=status, message=f"HTTP {status}: {body}")


class NodePlatformConnectionError(NodePlatformError):
    """Node Platform is not connected."""

    code = -1


class NodePlatformTimeoutError(NodePlatformError):
    """Node Platform communication timed out."""

    code = -2


class DeviceConnectionError(NodePlatformError):
    """Device is not connected."""

    code = -3


class DeviceTimeoutError(NodePlatformError):
    """Device operation timed out."""

    code = -4


class APIError(NodePlatformError):
    code = 400


class AuthenticationError(NodePlatformError):
    code = 401


class NotFoundError(NodePlatformError):
    code = 404
