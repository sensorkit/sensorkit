# SPDX-License-Identifier: Apache-2.0
"""ASA extension surface layered onto ``sensorkit.alpaca``'s generic Alpaca device.

Autoslew's remote surface is plain ASCOM Alpaca. Every ASA-specific feature
(satellite tracking, refraction, pointing model, dome/cover/nasmyth control) rides
the standard ASCOM extension mechanisms — ``Action`` / ``CommandString`` /
``CommandBool`` — on the single **Telescope** device. So the Telescope client is a
shared *backbone* every Autoslew device leans on for its ASA verbs, in addition to
its own typed Alpaca device (Focuser/Rotator/CoverCalibrator) where one exists.

``AutoslewMixin`` supplies that backbone surface, plus the ASA connect/disconnect
quirk shared by every Autoslew device: some (e.g. the CoverCalibrator) don't
implement the Platform-7 ``Connect()``/``Disconnect()`` methods, falling back to
the legacy ``Connected`` property instead. Concrete devices mix it in ahead of the
corresponding `sensorkit.alpaca` device class, e.g.
``AutoslewFocuser(AutoslewMixin, AlpacaFocuser)``, so its overrides take priority
over `AlpacaDevice`'s in the MRO.
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
    """First present key (case-insensitive) from a dict — tolerates ICD misspellings."""
    lower = {k.lower(): v for k, v in d.items()}
    for k in keys:
        if k.lower() in lower:
            return lower[k.lower()]
    return default


class AutoslewMixin:
    """ASA extension behavior shared by every Autoslew device.

    Mixed in ahead of `AlpacaDevice` (or a subclass of it): relies on
    `self.telescope` (the ASA Telescope backbone — the mount's own primary device,
    or an auxiliary client set up purely to carry Action/CommandString/CommandBool
    for a non-Telescope device) and on `self.call`/`self.get`/`self.put` from
    `AlpacaDevice`.
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
