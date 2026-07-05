# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from loguru import logger
from pydantic import BaseModel

try:
    import websockets
    from websockets.asyncio.client import ClientConnection
except ImportError:
    websockets = None
    ClientConnection = None


# ── INDIGO protocol constants ─────────────────────────────────────────

INDIGO_VERSION = 0x200  # Protocol 2.0


# ── INDIGO property model ────────────────────────────────────────────


class IndigoItem(BaseModel):
    """A single item within an INDIGO property."""

    name: str
    label: str = ""
    value: Any = None
    target: Any = None
    min: float | None = None
    max: float | None = None
    step: float | None = None


class IndigoProperty(BaseModel):
    """An INDIGO property with its items and metadata."""

    device: str
    name: str
    group: str = ""
    label: str = ""
    perm: str = "ro"
    state: str = "Idle"
    type: str = ""  # Number, Text, Switch, Light
    items: dict[str, IndigoItem] = {}

    def item_value(self, item_name: str, default: Any = None) -> Any:
        if item := self.items.get(item_name):
            return item.value
        return default


class IndigoClient:
    """Async JSON-over-WebSocket client for an INDIGO server.

    Maintains a persistent WebSocket connection and a local cache of all
    properties received from the server. Property updates are dispatched
    to registered callbacks.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7624,
        client_name: str = "SensorKit",
    ):
        self.host = host
        self.port = port
        self.client_name = client_name

        # property cache: (device, property_name) -> IndigoProperty
        self._properties: dict[tuple[str, str], IndigoProperty] = {}
        self._ws: ClientConnection | None = None
        self._recv_task: asyncio.Task | None = None
        self._connected = False

        # callbacks: (device, property_name) -> list[async callable]
        self._callbacks: dict[tuple[str, str], list] = {}
        # wildcard callbacks for a device (all properties): device -> list[async callable]
        self._device_callbacks: dict[str, list] = {}

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self, device_filter: str | None = None):
        """Connect to the INDIGO server and begin receiving property updates."""
        if websockets is None:
            raise RuntimeError("websockets package is required: pip install websockets")

        uri = f"ws://{self.host}:{self.port}"
        logger.debug(f"connecting to INDIGO server at {uri}")
        self._ws = await websockets.connect(
            uri,
            close_timeout=2,
            ping_interval=None,
        )
        self._connected = True

        # Start background receiver
        self._recv_task = asyncio.create_task(self._receive_loop())

        # Request property definitions
        get_props: dict[str, Any] = {
            "version": INDIGO_VERSION,
            "client": self.client_name,
        }
        if device_filter:
            get_props["device"] = device_filter
        await self._send({"getProperties": get_props})

        logger.debug(f"connected to INDIGO server at {uri}")

    async def disconnect(self):
        """Disconnect from the INDIGO server."""
        self._connected = False
        if self._recv_task is not None:
            self._recv_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task

            self._recv_task = None

        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    def get_property(self, device: str, name: str) -> IndigoProperty | None:
        """Look up a cached property."""
        return self._properties.get((device, name))

    def get_properties(self, device: str) -> dict[str, IndigoProperty]:
        """Get all cached properties for a device."""
        return {
            prop_name: prop for (dev, prop_name), prop in self._properties.items() if dev == device
        }

    def on_property(self, device: str, property_name: str | None = None):
        """Register a callback for property updates.

        If property_name is None, the callback fires for all properties on the device.
        The callback receives (property: IndigoProperty, msg_type: str).
        """

        def decorator(func):
            if property_name is None:
                self._device_callbacks.setdefault(device, []).append(func)
            else:
                self._callbacks.setdefault((device, property_name), []).append(func)
            return func

        return decorator

    async def set_property(
        self,
        device: str,
        name: str,
        items: dict[str, Any],
        prop_type: str = "Switch",
    ):
        """Send a newXxxVector message to change property values.

        Args:
            device: The INDIGO device name.
            name: The property name.
            items: Mapping of item name to desired value.
            prop_type: The property type (Switch, Number, Text).
        """
        msg_key = f"new{prop_type}Vector"
        msg = {
            msg_key: {
                "device": device,
                "name": name,
                "items": [{"name": k, "value": v} for k, v in items.items()],
            }
        }
        await self._send(msg)

    # ── Internal ──

    async def _send(self, msg: dict):
        if self._ws is None:
            raise RuntimeError("Not connected to INDIGO server")
        await self._ws.send(json.dumps(msg))

    async def _receive_loop(self):
        """Continuously read messages from the WebSocket and update the cache."""
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._handle_message(msg)
        except websockets.exceptions.ConnectionClosed:
            logger.warning("INDIGO WebSocket connection closed by server")
            self._connected = False
        except Exception as e:
            logger.warning(f"INDIGO receive loop error: {e}")
            self._connected = False

    async def _handle_message(self, msg: dict):
        """Parse an INDIGO JSON message and update the property cache."""
        for msg_type, body in msg.items():
            device = body.get("device", "")
            prop_name = body.get("name", "")

            if msg_type == "deleteProperty":
                if prop_name:
                    self._properties.pop((device, prop_name), None)
                else:
                    # Delete all properties for this device
                    keys = [k for k in self._properties if k[0] == device]
                    for k in keys:
                        del self._properties[k]
                continue

            # Determine property type from message type
            prop_type = ""
            if "Number" in msg_type:
                prop_type = "Number"
            elif "Text" in msg_type:
                prop_type = "Text"
            elif "Switch" in msg_type:
                prop_type = "Switch"
            elif "Light" in msg_type:
                prop_type = "Light"
            elif "BLOB" in msg_type:
                prop_type = "BLOB"

            is_def = msg_type.startswith("def")
            key = (device, prop_name)

            if is_def:
                # Full property definition — create or replace
                items = {}
                for raw_item in body.get("items", []):
                    item = IndigoItem(
                        name=raw_item.get("name", ""),
                        label=raw_item.get("label", ""),
                        value=raw_item.get("value"),
                        target=raw_item.get("target"),
                        min=raw_item.get("min"),
                        max=raw_item.get("max"),
                        step=raw_item.get("step"),
                    )
                    items[item.name] = item

                self._properties[key] = IndigoProperty(
                    device=device,
                    name=prop_name,
                    group=body.get("group", ""),
                    label=body.get("label", ""),
                    perm=body.get("perm", "ro"),
                    state=body.get("state", "Idle"),
                    type=prop_type,
                    items=items,
                )

            elif msg_type.startswith("set"):
                # Property update — merge into existing
                prop = self._properties.get(key)
                if prop is None:
                    # We got an update before a def; create a skeleton
                    prop = IndigoProperty(
                        device=device,
                        name=prop_name,
                        type=prop_type,
                    )
                    self._properties[key] = prop

                if "state" in body:
                    prop.state = body["state"]

                for raw_item in body.get("items", []):
                    item_name = raw_item.get("name", "")
                    if item_name in prop.items:
                        if "value" in raw_item:
                            prop.items[item_name].value = raw_item["value"]
                        if "target" in raw_item:
                            prop.items[item_name].target = raw_item["target"]
                    else:
                        prop.items[item_name] = IndigoItem(
                            name=item_name,
                            label=raw_item.get("label", ""),
                            value=raw_item.get("value"),
                            target=raw_item.get("target"),
                        )

            # Dispatch callbacks
            prop = self._properties.get(key)
            if prop:
                # Specific property callbacks
                for cb in self._callbacks.get(key, []):
                    try:
                        await cb(prop, msg_type)
                    except Exception as e:
                        logger.exception(f"INDIGO callback error: {e}")

                # Device-wide callbacks
                for cb in self._device_callbacks.get(device, []):
                    try:
                        await cb(prop, msg_type)
                    except Exception as e:
                        logger.exception(f"INDIGO device callback error: {e}")


@dataclass
class IndigoDevice:
    """Base class for SensorKit devices backed by an INDIGO server."""

    config: IndigoDeviceConfig
    device_connected: bool | None = field(default=None, init=False)
    device_name: ClassVar[str] = "Device"
    _client: IndigoClient | None = field(default=None, init=False, repr=False)

    @property
    def client(self) -> IndigoClient:
        if self._client is None:
            self._client = IndigoClient(
                host=self.config.host,
                port=self.config.port,
                client_name=self.config.client_name,
            )
        return self._client

    def require_connected(self):
        if not self.device_connected:
            raise DeviceConnectionError(f"{self.device_name} not connected")

    async def connect_to_server(self):
        await self.client.connect(device_filter=self.config.indigo_device)

    async def disconnect_from_server(self):
        await self.client.disconnect()


class IndigoDeviceConfig[T: IndigoDevice = IndigoDevice](BaseModel):
    """Base configuration for an INDIGO device."""

    device_type: Literal[None] = None
    host: str = "localhost"
    port: int = 7624
    client_name: str = "SensorKit"
    indigo_device: str = ""
    timeout: float = 30.0

    def create_device(self) -> T:
        return IndigoDevice(self)


class IndigoDeviceState(BaseModel):
    """Generic INDIGO device state."""

    device_type: Literal[None] = None


class IndigoError(Exception):
    """Base exception for INDIGO device errors."""


class DeviceConnectionError(IndigoError):
    """Device is not connected."""


class PropertyError(IndigoError):
    """A property operation failed (Alert state, missing property, etc.)."""
