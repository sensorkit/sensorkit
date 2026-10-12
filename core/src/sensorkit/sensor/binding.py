# SPDX-License-Identifier: Apache-2.0
"""Bind reported capabilities to placements.

Binding requires reports for every configured device and verifies authored
trait assertions. Established traits come from reported commands and keywords;
assertions do not restrict which capabilities compilation may use.
"""

from __future__ import annotations

from collections.abc import Mapping

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.entity import DeviceDetails
from sensorkit.core.trait import get_trait, match_traits
from sensorkit.sensor.topology import (
    Device,
    Placement,
    Selector,
    Topology,
)


class BoundSensor:
    """A topology with reported device details and established traits.

    Selections, routing and lowering read these facts. `bind` validates
    report coverage and trait assertions. Direct construction skips validation.

    `device_keywords` supplies device keyword values for planning. `None`
    means no mapping was supplied. The values may describe capabilities or
    current state and may become stale before execution.
    """

    def __init__(
        self,
        topology: Topology,
        details: Mapping[str, DeviceDetails],
        *,
        device_keywords: Mapping[str, KeywordDict] | None = None,
    ):
        self.topology = topology
        self.details = dict(details)
        self._device_keywords = device_keywords

        # Keep every matching trait, including multiple archetypes.
        self._traits = {
            placement: frozenset(t.name for t in match_traits(found))
            for placement in topology.placements()
            if (found := self.details.get(placement.device)) is not None
        }

    @classmethod
    def bind(cls, topology: Topology, details: Mapping[str, DeviceDetails]) -> BoundSensor:
        """Check device reports and trait assertions, then return a sensor.

        Trait matching uses the same capability predicates as device
        declarations. Unsupported workflow commands are found later, during
        compilation.

        Raises:
            ValueError: A configured device has no report, or an authored trait
                is unregistered or not satisfied by its device.
        """
        silent = tuple(p for p in topology.placements() if p.device not in details)

        if silent:
            raise ValueError(
                "the structure names devices that reported nothing: "
                + ", ".join(f"'{p.device}' at '{p.path_str}'" for p in silent)
            )

        sensor = cls(topology, details)
        unmet = tuple(
            f"'{p.device}' at '{p.path_str}' {reason}"
            for p in topology.placements()
            for reason in _unmet(topology.record(p), sensor.traits(p))
        )

        if unmet:
            raise ValueError("declared traits were not established: " + ", ".join(unmet))

        return sensor

    def supported_commands(self, device: str) -> frozenset[str]:
        """Return reported command identifiers, or an empty set for an
        unknown device.
        """
        found = self.details.get(device)

        return found.supported_commands if found else frozenset()

    def published_keywords(self, device: str) -> frozenset[str]:
        """Return reported keyword identifiers, or an empty set for an
        unknown device.
        """
        found = self.details.get(device)

        return found.published_keywords if found else frozenset()

    def device_keywords(self, device: str) -> KeywordDict | None:
        """Return the supplied device keywords, or `None` if no mapping was supplied.

        A device missing from a supplied mapping has an empty keyword set.
        Supplied keyword values are returned without copying.
        """
        if self._device_keywords is None:
            return None

        return self._device_keywords.get(device, KeywordDict())

    def traits(self, placement: Placement) -> frozenset[str]:
        """Return established traits, or an empty set for an unknown
        placement.
        """
        return self._traits.get(placement, frozenset())


def _unmet(record: Device | Selector, established: frozenset[str]) -> tuple[str, ...]:
    """Describe declared traits that binding could not establish.

    Distinguish unregistered names from registered traits the device lacks.
    """
    return tuple(
        f"declares trait '{name}', which is not registered"
        if get_trait(name) is None
        else f"declares trait '{name}', which its device does not satisfy"
        for name in record.traits
        if name not in established
    )
