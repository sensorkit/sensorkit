from __future__ import annotations

import collections
import importlib
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from loguru import logger
from pydantic import BaseModel, TypeAdapter, model_validator

from sensorkit.common.keyword import KeywordDict
from sensorkit.config.section import ConfigSection, get_config_section

PARSER_VERSION = 1
VERSION_KEY = "version"
GLOBALS_KEY = "sensorkit"
SERVICES_KEY = "services"

# Import core sections.
importlib.import_module("sensorkit.config.core")


class ServiceConfig(BaseModel):
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


class ParsedConfig(NamedTuple):
    global_kv: KeywordDict
    entity_kv: dict[str, list[BaseModel]]
    services: list[ServiceConfig]
    version: int = PARSER_VERSION


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


def _parse_sections(config: Mapping[str, Any]):
    ekv: dict[str, list[BaseModel]] = collections.defaultdict(list)
    services = TypeAdapter(list[ServiceConfig]).validate_python(config.get(SERVICES_KEY, []))

    for key, value in config.items():
        if key in (VERSION_KEY, GLOBALS_KEY, SERVICES_KEY):
            continue

        section = get_config_section(key)

        if not section:
            raise ConfigSectionUnknown(f"Unknown config section {key!r}")

        for entity_id, model in _parse_section(section, value):
            ekv[entity_id].append(model)

            if section.service_path:
                services.append(ServiceConfig(id=entity_id, python_module=section.service_path))

    return ekv, services


def _parse_globals(config: Mapping[str, Any]):
    if global_section := config.get(GLOBALS_KEY, None):
        return TypeAdapter(KeywordDict).validate_python(global_section)
    else:
        return KeywordDict()


def parse_config(config: Mapping[str, Any]) -> ParsedConfig:
    """Parse the given unified configuration dict.

    Args:
        config: A mapping containing the unified configuration data.

    Returns:
        A tuple containing global and per-entity KV settings that describe the input config.

    Raises:
        ConfigVersionUnsupported: The config version is mismatched
        ConfigSectionUnknown: A config section cannot be found in the registry
        ConfigError: An internal error with config processing
        ValidationError: A config section fails Pydantic validation during parsing
    """
    version = config.get(VERSION_KEY)

    if version != PARSER_VERSION:
        raise ConfigVersionUnsupported(f"Unsupported config version: {version}")

    gkv = _parse_globals(config)
    ekv, services = _parse_sections(config)
    return ParsedConfig(gkv, ekv, services)


class ConfigError(Exception):
    """Raised when an error occurs during config loading."""


class ConfigVersionUnsupported(ConfigError):
    """Raised when the config version is unsupported."""


class ConfigSectionUnknown(ConfigError):
    """Raised when the config section is unknown."""
