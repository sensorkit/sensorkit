# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable
from typing import Any, Callable, NamedTuple

from pydantic import BaseModel, TypeAdapter

type UnifiedConfigEntityMapper = Callable[[Any], Iterable[str]]
type UnifiedConfigModelMapper[T] = Callable[[T], Iterable[tuple[BaseModel, ...]]]


class ConfigSection(NamedTuple):
    key: str
    adapter: TypeAdapter
    entity_mapper: UnifiedConfigEntityMapper
    model_mapper: UnifiedConfigModelMapper | None
    service_path: str | None


_registry: dict[str, ConfigSection] = {}


def declare_config_section[T](
    key: str,
    parse_type: Any,
    *,
    entity_mapper: UnifiedConfigEntityMapper,
    model_mapper: UnifiedConfigModelMapper[T] | None = None,
    service_path: str | None = None,
):
    """Register a config section handler.

    Args:
        key: Top-level YAML key this handler claims.
        parse_type: Pydantic-compatible type used to validate the raw YAML value.
            Accepts any form TypeAdapter understands: type[T], list[T], etc.
        entity_mapper: Callable that takes the raw input dictionary and returns an iterable
            of entity IDs to write configuration for.
        model_mapper: Optional callable that takes the validated instance and yields
            tuples of BaseModel instances to write to KV for each entity.
        service_path: Optional path to a service implementation.
    """
    if key in _registry:
        raise ValueError(f"Config section {key!r} already registered")

    _registry[key] = ConfigSection(
        key,
        TypeAdapter(parse_type),
        entity_mapper,
        model_mapper,
        service_path,
    )


def get_config_section(key: str) -> ConfigSection | None:
    """Retrieve a registered config section handler.

    Args:
        key: The top-level YAML key to look up.

    Returns:
        A tuple of (TypeAdapter, entity_mapper, model_mapper) if the key is registered,
        or None if the key is not found in the registry.
    """
    return _registry.get(key)
