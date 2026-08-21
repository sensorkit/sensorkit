# SPDX-License-Identifier: Apache-2.0
"""Load and validate sensor definitions without contacting devices.

A definition combines structure, lifecycle tables and deadline rules. Loading
checks model validity, structural uniqueness, references and symbolic cycles.
Capability checks and workflow compilation happen after binding.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

from sensorkit.sensor.lifecycle import LifecycleWorkflow
from sensorkit.sensor.topology import Structure
from sensorkit.sensor.workflow import DeadlineRule


class SensorDefinition(BaseModel, frozen=True, extra="forbid"):
    """A sensor's structure, lifecycle tables and deadline rules.

    Tables may be supplied as a mapping; each key becomes the table name and
    the values are stored as a tuple. Python construction validates the models;
    call `check` for structural and reference checks, or use the YAML loaders.
    """

    sensor: Structure
    tables: tuple[LifecycleWorkflow, ...] = ()
    deadlines: tuple[DeadlineRule, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _name_tables_from_keys(cls, v: object) -> object:
        if not isinstance(v, Mapping) or not isinstance(v.get("tables"),
                                                        Mapping):
            return v

        named = [{**t, "name": k} if isinstance(t, Mapping) else t
                 for k, t in v["tables"].items()]

        return {**v, "tables": named}


    @classmethod
    def from_yaml(cls, text: str) -> SensorDefinition:
        """Parse a YAML definition and run structural and reference checks.

        Raises:
            yaml.YAMLError: The text is not valid YAML.
            ValidationError: The document does not match the models.
            ValueError: A structural, reference or dependency check fails.
        """
        definition = cls.model_validate(yaml.safe_load(text))

        return definition

    @classmethod
    def load(cls, path: str | Path) -> SensorDefinition:
        """Read a UTF-8 YAML file and validate its definition."""
        return cls.from_yaml(Path(path).read_text(encoding="utf-8"))
