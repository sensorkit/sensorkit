from __future__ import annotations

import collections
import importlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field, model_validator

from sensorkit.common.keyword import KeywordDict
from sensorkit.config.section import ConfigSection, get_config_section

PARSER_VERSION = 1


class GeneralConfig(BaseModel, extra="forbid"):
    backend: str | None = None
    modules: list[str] = Field(default_factory=list)
    imports: list[str] = Field(default_factory=list)


class ServiceConfig(BaseModel, extra="forbid"):
    id: str
    python_path: str | None = None
    python_module: str | None = None
    python_file: str | None = None

    @model_validator(mode="after")
    def _validator(self):
        if bool(self.python_module) == bool(self.python_path):
            raise ValueError("Exactly one of 'python_module' or 'python_path' must be set")

        if self.python_file and self.python_path:
            raise ValueError("Cannot set both 'python_file' and 'python_path'")

        return self


class SensorKitBaseConfig(BaseModel, extra="allow"):
    version: int = 0
    sensorkit: GeneralConfig = Field(default_factory=GeneralConfig)
    globals: KeywordDict = Field(default_factory=KeywordDict)
    services: list[ServiceConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self):
        if self.version != PARSER_VERSION:
            raise ConfigVersionUnsupported(f"Unsupported config version: {self.version}")

        return self

    def resolve_dynamic_sections(self):
        # Make sure core sections are imported.
        importlib.import_module("sensorkit.config.core")

        ekv, services = _parse_extra_sections(self.model_extra)
        return SensorKitConfig(
            base=self,
            services=self.services + services,
            entity_kv=ekv,
        )

    def configured_imports(self):
        return [
            *self.sensorkit.imports,
            *(f"sensorkit.{module}" for module in self.sensorkit.modules),
        ]


@dataclass
class SensorKitConfig:
    base: SensorKitBaseConfig
    services: list[ServiceConfig]
    entity_kv: dict[str, list[BaseModel]]

    @property
    def backend(self):
        return self.base.sensorkit.backend

    @property
    def global_kv(self):
        return self.base.globals


def _validate_mapper_return_value[T](value: Any, expected_type: type[T]) -> tuple[T] | None:
    if isinstance(value, expected_type):
        return (value,)
    elif isinstance(value, Iterable):
        value = tuple(value)

    if not isinstance(value, tuple) or not all(isinstance(elem, expected_type) for elem in value):
        logger.debug(f"Invalid mapper return value for type {expected_type.__name__}: {value}")
        return None

    return value


def _parse_section(section: ConfigSection, value: Any) -> Iterable[tuple[str, BaseModel]]:
    try:
        ids = _validate_mapper_return_value(section.entity_mapper(value), str)
    except Exception as e:
        raise ConfigError(f"Error in config section {section.key!r} (id mapper error)") from e

    if ids is None:
        raise ConfigError(f"Error in config section {section.key!r} (bad id mapper?)")

    instance = section.adapter.validate_python(value)

    try:
        models = _validate_mapper_return_value(
            section.model_mapper(instance) if section.model_mapper is not None else instance,
            BaseModel,
        )
    except Exception as e:
        raise ConfigError(f"Error in config section {section.key!r} (model mapper error)") from e

    if models is None:
        raise ConfigError(f"Error in config section {section.key!r} (bad model mapper?)")

    return zip(ids, models, strict=True)


def _parse_extra_sections(config: Mapping[str, Any]):
    ekv: dict[str, list[BaseModel]] = collections.defaultdict(list)
    services: list[ServiceConfig] = []

    for key, value in config.items():
        section = get_config_section(key)

        if not section:
            raise ConfigSectionUnknown(f"Unknown config section {key!r}")

        for entity_id, model in _parse_section(section, value):
            ekv[entity_id].append(model)

            if section.service_path:
                services.append(ServiceConfig(id=entity_id, python_module=section.service_path))

    return ekv, services


def parse_config(config: Mapping[str, Any]) -> SensorKitBaseConfig:
    """Parse the given unified configuration dict.

    Args:
        config: A mapping containing the unified configuration data.

    Returns:
        A SensorKitBaseConfig instance.

    Raises:
        ConfigVersionUnsupported: The config version is mismatched
        ConfigSectionUnknown: A config section cannot be found in the registry
        ConfigError: An internal error with config processing
        ValidationError: A config section fails Pydantic validation during parsing
    """
    return SensorKitBaseConfig.model_validate(config)


class ConfigError(Exception):
    """Raised when an error occurs during config loading."""


class ConfigVersionUnsupported(ConfigError):
    """Raised when the config version is unsupported."""


class ConfigSectionUnknown(ConfigError):
    """Raised when the config section is unknown."""
