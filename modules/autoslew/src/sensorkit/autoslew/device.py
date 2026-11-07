# SPDX-License-Identifier: Apache-2.0
"""ASA extensions to the Alpaca device interface.

Autoslew exposes extension actions on its Telescope device. A shared telescope
client, called the backbone here, carries these actions for the mount and
other devices. Focusers, rotators, and covers also use their own typed Alpaca
clients for standard commands.

`AutoslewMixin` tries the Platform 7 connection methods and falls back to the
`Connected` property when they are unsupported.
"""

from __future__ import annotations

import json
from typing import Any

from alpaca.device import Device
from loguru import logger

from sensorkit.alpaca.device import AlpacaDevice, DeviceConnectionError


def _num(s: Any) -> float:
    """Parse an Autoslew numeric string: tolerate comma decimals and trailing '#'."""
    if isinstance(s, (int, float)):
        return float(s)
    return float(str(s).strip().rstrip("#").strip().replace(",", "."))


def _pick(d: dict, *keys: str, default: Any = None) -> Any:
    """Return the first present key, ignoring case and accepting spelling variants."""
    lower = {k.lower(): v for k, v in d.items()}
    for k in keys:
        if k.lower() in lower:
            return lower[k.lower()]
    return default


class AutoslewMixin:
    """Connection handling and telescope extension calls shared by Autoslew devices.

    Concrete devices supply a telescope client and the Alpaca call helpers.
    """

    telescope: Any
    device_name: str
    device_connected: bool | None

    async def connect(self, device: Device, timeout: float = 60.0):
        try:
            await super().connect(device, timeout)
        except DeviceConnectionError:
            raise
        except Exception:
            # Some Autoslew devices (e.g. the CoverCalibrator) don't implement the
            # Platform-7 Connect() method; fall back to the legacy Connected property.
            logger.debug(f"{self.device_name}: Connect() unavailable, using legacy Connected")
            await self.put(device, "Connected", True)
            if not await self.get(device, "Connected", False):
                raise DeviceConnectionError(
                    f"Failed to connect to {self.device_name}"
                ) from None
            self.device_connected = True
            logger.debug(f"connected to {self.device_name}")

    async def disconnect(self, device: Device, timeout: float = 30.0):
        try:
            await super().disconnect(device, timeout)
        except Exception:
            await self.put(device, "Connected", False)
            self.device_connected = False

    async def action(self, name: str, params: str = "") -> str:
        """PUT .../telescope/N/action — returns a string always."""
        return await self.call(self.telescope, "Action", name, params)

    async def command_string(self, cmd: str, raw: bool = True) -> str:
        return await self.call(self.telescope, "CommandString", cmd, raw)

    async def command_bool(self, cmd: str, raw: bool = True) -> bool:
        return bool(await self.call(self.telescope, "CommandBool", cmd, raw))

    async def action_json(self, name: str, params: str = "") -> dict:
        """An Action whose string return value is a JSON object."""
        return json.loads(await self.action(name, params))


class AutoslewDevice(AutoslewMixin, AlpacaDevice):
    """Generic Autoslew (ASCOM Alpaca) device: an `AlpacaDevice` with the ASA backbone.

    Used directly by devices with no typed Alpaca counterpart of their own (the
    Dome and Tertiary, which ride the Telescope backbone exclusively).
    """
