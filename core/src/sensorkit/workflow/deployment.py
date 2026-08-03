# SPDX-License-Identifier: Apache-2.0
"""Deployment surface: one document = one sensor plus its phase tables.

Everything observatory-shaped (structure, trait vocabulary, workflow tables) lives
in the document; the modules below it contain none of it. Loading validates three
ways:

1. pydantic field validation — shapes, types, enum values; unknown keys are errors
   (`extra="forbid"` throughout), so typos fail loudly at load time;
2. structural validation on the sensor — claim uniqueness;
3. cross-validation — every table is compiled against the sensor at load time, so
   unknown devices, bad `after` and `require` targets, duplicate claims, and
   dependency cycles are load-time errors, not runtime surprises; the same for an
   alias naming a device the sensor does not carry.

The one check that cannot happen here is per-deployment: whether the deployment's
op hook can actually perform every op the tables name. That check belongs to
whoever owns the op vocabulary, which is the dispatcher.

YAML shorthands accepted (dumps normalize to the canonical long form, which also
parses):

* attachments as a mapping — `{tcs-1: mount}` or `{tcs-1: [focuser, rotator]}`;
* parts carry no kind tag — the shape decides: `instrument` makes an instrument,
  `selector` a selector, `parts` alone an assembly;
* an entry's `ops` may be a single item instead of a list, and each op a bare
  string instead of `{op: ...}`;
* an entry's `require` may be a single item instead of a list, and each clause a
  bare string instead of `{name: ...}`;
* a table's name is its key in the `tables` mapping.
"""

from __future__ import annotations

import functools
from collections.abc import Sequence
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

from sensorkit.workflow.dag import Graph
from sensorkit.workflow.lifecycle import PhaseTable, compile_table
from sensorkit.workflow.override import Override
from sensorkit.workflow.structure import DeviceRef, SensorModel
from sensorkit.workflow.views import DeviceIndex, Topology


class SensorPlan(BaseModel, extra="forbid"):
    """One sensor's structure, its workflow tables, and the names it publishes for
    its devices.

    Aliases are the whole of what a deployment authors for a task source: what its
    instruments *are* is discovered from the devices themselves, so the only thing
    left to write down is what a site prefers to call one.
    """

    sensor: SensorModel
    tables: dict[str, PhaseTable] = Field(default_factory=dict)
    aliases: dict[str, DeviceRef] = Field(default_factory=dict)

    # `views.py` deliberately ships two independent views and no container for them,
    # so that a caller takes only the views it can name a use for — often both,
    # since the two are axes rather than audiences. Assembling the pair for a whole
    # deployment is this layer's job — it is the only place that already knows the
    # sensor is fixed. Cached because a plan outlives many compiles; pydantic keeps
    # cached properties out of equality and dumps.

    @functools.cached_property
    def topology(self) -> Topology:
        return Topology(self.sensor)

    @functools.cached_property
    def devices(self) -> DeviceIndex:
        return DeviceIndex(self.sensor)

    @model_validator(mode="before")
    @classmethod
    def _name_tables_from_keys(cls, v: object) -> object:
        if isinstance(v, dict) and isinstance(v.get("tables"), dict):
            v = {**v, "tables": {
                k: ({**t, "name": k} if isinstance(t, dict) else t)
                for k, t in v["tables"].items()}}
        return v

    @model_validator(mode="after")
    def _cross_validate(self) -> SensorPlan:
        # Python-constructed tables adopt the mapping key as their name.
        self.tables = {
            k: (t if t.name == k else t.model_copy(update={"name": k}))
            for k, t in self.tables.items()}

        for name, table in self.tables.items():
            try:
                compile_table(self.devices, table)
            except ValueError as e:
                raise ValueError(f"table '{name}': {e}") from e

        # An alias is a second name for a device, so a name for one the sensor does
        # not carry is a typo — and one that would otherwise surface as a task
        # failing to resolve at night.
        unknown = sorted(f"{name} -> {ref}" for name, ref in self.aliases.items()
                         if ref not in self.devices)
        if unknown:
            raise ValueError(f"aliases name unknown device(s): {unknown}")

        return self

    @classmethod
    def from_yaml(cls, text: str) -> SensorPlan:
        return cls.model_validate(yaml.safe_load(text))

    @classmethod
    def load(cls, path: str | Path) -> SensorPlan:
        return cls.from_yaml(Path(path).read_text())

    def to_yaml(self) -> str:
        return yaml.safe_dump(
            self.model_dump(mode="json", exclude_defaults=True),
            sort_keys=False)

    def save(self, path: str | Path) -> None:
        Path(path).write_text(self.to_yaml())

    def compile(self, table: str,
                overrides: Sequence[Override] = ()) -> Graph:
        # Overrides are the caller's, not the document's: this plan describes what
        # the observatory is, and an override describes how it is being operated
        # tonight. Recompiling is how an operating state reaches a run.
        return compile_table(self.devices, self.tables[table], overrides)
