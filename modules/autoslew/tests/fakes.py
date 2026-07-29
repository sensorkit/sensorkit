# SPDX-License-Identifier: Apache-2.0
"""Autoslew SDK device fake."""

from __future__ import annotations

from sensorkit.alpaca.testing import FakeAlpacaSDKDevice


class FakeAutoslewSDKDevice(FakeAlpacaSDKDevice):
    """The alpyca Telescope plus the ASA extension verbs.

    Autoslew layers three mechanisms on standard ASCOM: `Action` for named verbs, `CommandBool`
    for motor state, and `CommandString` for the satellite tracker. The defaults report the
    motors on and the pass acquired, so the mount's motors-first and sat-acquire waits resolve
    immediately; `action_returns` supplies the payload for named `Action` verbs that read back.
    """

    def __init__(
        self,
        sat_tracking: bool = True,
        motors_on: bool = True,
        action_returns: dict | None = None,
        **properties,
    ):
        super().__init__(**properties)
        self._motors_on = motors_on
        self._sat_bit = 1 if sat_tracking else 0
        self._action_returns = action_returns or {}

    def actions(self) -> list[str]:
        """ASA Action names issued, in order."""
        return [args[0] for args, _ in self.calls("Action")]

    def Action(self, name, params=""):
        self._calls.append(("Action", (name, params), {}))
        return self._action_returns.get(name, "")

    def CommandBool(self, cmd, raw=True):
        self._calls.append(("CommandBool", (cmd, raw), {}))
        return self._motors_on if cmd == "MotStat" else False

    def CommandString(self, cmd, raw=True):
        self._calls.append(("CommandString", (cmd, raw), {}))

        if cmd == "getSatStatus":
            return f'{{"status": {self._sat_bit}, "TrackErrAx1": 0.0, "TrackErrAx2": 0.0}}'

        return ""
