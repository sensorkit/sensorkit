# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew mirror cover through the Alpaca CoverCalibrator interface.

The inherited Alpaca driver tolerates unsupported calibrator properties.
This device adds Autoslew connection handling and a separate persisted state.
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
from sensorkit.std import StandardMirrorCover


class AutoslewCoverCalibratorState(AlpacaCoverCalibratorState):
    """Autoslew mirror cover state (distinct KV key from the Alpaca base)."""


@sk.declare_device(type=StandardMirrorCover)
class AutoslewCoverCalibrator(AutoslewMixin, AlpacaCoverCalibrator):
    """ASA Autoslew mirror cover implementation."""

    config: AutoslewCoverCalibratorConfig
    device_name = "CoverCalibrator"
    state_model = AutoslewCoverCalibratorState


class AutoslewCoverCalibratorConfig(AlpacaCoverCalibratorConfig):
    @override
    def create_device(self):
        return AutoslewCoverCalibrator(self)
