# SPDX-License-Identifier: Apache-2.0
"""Alpaca SDK device fake.

alpyca drives an ASCOM Alpaca server over HTTP, so its device objects are stubbed rather than
run against a real stack. `sensorkit.autoslew` extends this the same way it extends the rest of
`sensorkit.alpaca`.
"""

from __future__ import annotations

from typing import Any


class Readings:
    """Successive values returned by repeated reads of one property.

    The final value repeats once the sequence is exhausted, so a property that settles (an
    exposure becoming ready, a slew finishing) can be described by the values leading up to it.
    """

    def __init__(self, *values: Any):
        assert values, "Readings needs at least one value"
        self._values = list(values)

    def read(self) -> Any:
        return self._values.pop(0) if len(self._values) > 1 else self._values[0]


MOTION_METHODS = frozenset(
    {
        "FindHome",
        "MoveAxis",
        "Park",
        "SlewToAltAz",
        "SlewToAltAzAsync",
        "SlewToAzimuth",
        "SlewToCoordinates",
        "SlewToCoordinatesAsync",
        "SlewToTarget",
        "SlewToTargetAsync",
        "Unpark",
    }
)
"""Methods that put the device in motion, so the next `Slewing` read reports a slew."""


class FakeAlpacaSDKDevice:
    """Any alpyca SDK device (Camera, Dome, Telescope, ...).

    Mirrors the synchronous property/method interface alpyca exposes. Properties live in a dict
    reachable as `_properties`; unknown attribute reads return a no-op callable, so SDK methods
    the code under test happens to call succeed and are recorded for `calls()`.

    A property value may also be a `Readings` sequence, or an exception instance to raise on
    read — alpyca signals unsupported properties by raising `NotImplementedException`.
    """

    def __init__(self, **properties):
        super().__setattr__(
            "_properties",
            {
                "Connected": False,
                "Connecting": False,
                **properties,
            },
        )
        super().__setattr__("_calls", [])
        super().__setattr__("_slewing", False)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        props = super().__getattribute__("_properties")

        if name in props:
            # A commanded slew shows up as motion on the next read and has settled by the one
            # after, which is what the driver's onset-then-settle wait expects to see.
            if name == "Slewing" and super().__getattribute__("_slewing"):
                super().__setattr__("_slewing", False)
                return True

            value = props[name]

            if isinstance(value, Readings):
                return value.read()

            if isinstance(value, Exception):
                raise value

            return value

        def method(*args, **kwargs):
            super(FakeAlpacaSDKDevice, self).__getattribute__("_calls").append(
                (name, args, kwargs)
            )

            if name in MOTION_METHODS:
                super(FakeAlpacaSDKDevice, self).__setattr__("_slewing", True)

        return method

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._properties[name] = value

    def Connect(self):
        self._properties["Connected"] = True
        self._properties["Connecting"] = False

    def Disconnect(self):
        self._properties["Connected"] = False
        self._properties["Connecting"] = False

    def calls(self, name: str) -> list[tuple[tuple, dict]]:
        """Every recorded call to the named method, oldest first."""
        return [(args, kwargs) for called, args, kwargs in self._calls if called == name]
