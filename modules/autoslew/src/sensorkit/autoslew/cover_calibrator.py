# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew mirror cover (standard ASCOM Alpaca CoverCalibrator).

Autoslew exposes a CoverCalibrator device but only the *cover* half is implemented
(``Brightness``/``MaxBrightness`` return NotImplemented) — so this is effectively a
mirror cover (``StandardMirrorCover``). ASA also exposes cover control via the
Telescope actions ``telescope:opencover``/``closecover``; we prefer the real device.

`sensorkit.alpaca`'s `AlpacaCoverCalibrator` already degrades gracefully when the
calibrator half is unimplemented (`AlpacaCoverCalibratorStatus.calibrator_state`
reads back "NotPresent", `brightness`/`max_brightness` read back `None`), so nothing
here needs to override its behavior — this class exists purely to name the ASA
device and mix in the ASA connect/disconnect quirk.
"""

from __future__ import annotations

from typing import override

import sensorkit.api as sk
from sensorkit.alpaca.cover_calibrator import (
    AlpacaCoverCalibrator,
    AlpacaCoverCalibratorConfig,
    AlpacaCoverCalibratorState,
)
from sensorkit.autoslew.device import AutoslewMixin


class AutoslewCoverCalibratorState(AlpacaCoverCalibratorState):
    """Autoslew mirror cover state (distinct KV key from the Alpaca base)."""


@sk.declare_device
class AutoslewCoverCalibrator(AutoslewMixin, AlpacaCoverCalibrator):
    """ASA Autoslew mirror cover implementation."""

    config: AutoslewCoverCalibratorConfig
    device_name = "CoverCalibrator"
    state_model = AutoslewCoverCalibratorState


class AutoslewCoverCalibratorConfig(AlpacaCoverCalibratorConfig):
    @override
    def create_device(self):
        return AutoslewCoverCalibrator(self)
