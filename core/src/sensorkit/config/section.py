# SPDX-License-Identifier: Apache-2.0
import operator
from collections.abc import Iterable, Mapping
from typing import Any, Callable, Literal, NamedTuple

from pydantic import BaseModel, TypeAdapter

type UnifiedConfigIdMapper = Callable[[Any], Iterable[str]]
type UnifiedConfigModelMapper[T] = Callable[[T], Iterable[BaseModel]]
type IdSource = Literal["by_key", "by_subkey", "mapping_key", "default"]

DEFAULT_ID_KEY = "id"
KEYED_ID_SOURCES = ("by_key", "by_subkey")
DEFAULTABLE_ID_SOURCES = ("by_key", "default")


def _section_entries(value: Any) -> Iterable[Any]:
    return value.values() if isinstance(value, Mapping) else value


def _validate_id_naming(key: str, id_source: str, id_key: str | None, id_default: str | None):
    """Reject a declaration whose ID arguments contradict the source it names.

    Args:
        key: Top-level YAML key the section claims.
        id_source: Where the section takes its entity IDs from.
        id_key: Key holding the entity ID, where one was given.
        id_default: Entity ID to fall back on, where one was given.

    Raises:
        ValueError: The arguments cannot apply together.
    """
    if id_key is not None and id_source not in KEYED_ID_SOURCES:
        raise ValueError(f"Config section {key!r} cannot take 'id_key' with {id_source!r}")

    # One ID cannot stand in for entries that omit their key, which would leave several
    # entries sharing an entity.
    if id_default is not None and id_source not in DEFAULTABLE_ID_SOURCES:
        raise ValueError(f"Config section {key!r} cannot take 'id_default' with {id_source!r}")

    if id_source == "default" and id_default is None:
        raise ValueError(f"Config section {key!r} must give an 'id_default' with {id_source!r}")


class ConfigSection(NamedTuple):
    """A config section handler.

    Args:
        key: Top-level YAML key this handler claims.
        adapter: Validates the section's raw value.
        id_source: Where the section takes its entity IDs from.
        id_mapper: Takes the raw value and returns the entity ID to write each model under,
            one per model the model mapper yields.
        model_mapper: Takes the validated instance and yields the models to write to KV, one
            per ID. None writes the validated instance itself.
        service_path: Path to a service implementation, where the section launches one.
        id_key: Key holding the entity ID, for the keyed ID sources.
        id_key_required: Whether the file has to supply `id_key`. False where the section
            falls back to an ID of its own.
    """

    key: str
    adapter: TypeAdapter
    id_source: IdSource
    id_mapper: UnifiedConfigIdMapper
    model_mapper: UnifiedConfigModelMapper | None = None
    service_path: str | None = None
    id_key: str | None = None
    id_key_required: bool = False


_registry: dict[str, ConfigSection] = {}


def declare_config_section[T](
    key: str,
    parse_type: Any,
    *,
    id_source: IdSource,
    id_key: str | None = None,
    id_default: str | None = None,
    id_mapper: UnifiedConfigIdMapper | None = None,
    model_mapper: UnifiedConfigModelMapper[T] | None = None,
    service_path: str | None = None,
):
    """Declare and register a config section handler.

    The section's records are validated against `parse_type` and written to the KV namespace
    of the entity each one configures. Where those IDs come from is declared rather than
    computed, so the generated JSON Schema can describe the key carrying them alongside the
    model's own fields.

    Args:
        key: Top-level YAML key this handler claims.
        parse_type: Pydantic-compatible type used to validate the raw YAML value. Accepts any
            form TypeAdapter understands: type[T], list[T], etc.
        id_source: Where the section takes the entity ID for each of its records. `by_key`
            reads it from a key of the value itself, for a section configuring one entity.
            `by_subkey` reads it from a key of each entry, for a section configuring one
            entity per entry. `mapping_key` takes the IDs from the value's own keys.
            `default` names the entity at declaration, and the file supplies no ID at all.
        id_key: Key holding the entity ID, for `by_key` and `by_subkey`. Defaults to `id`.
        id_default: Entity ID to use where the file supplies none. Required with `default`,
            which admits no other ID. Given with `by_key` it makes the key optional.
        id_mapper: Optional callable that takes the raw value and returns an entity ID per
            model, replacing the one `id_source` implies. Sections producing several models
            from a single entry need one, since IDs are matched against the models rather
            than against the entries.
        model_mapper: Optional callable that takes the validated instance and yields the
            models to write to KV, one per ID. Defaults to the section's entries.
        service_path: Optional path to a service implementation.

    Raises:
        ValueError: The section is already registered, or its ID naming is inconsistent.
    """
    _validate_id_naming(key, id_source, id_key, id_default)

    resolved_key = id_key or DEFAULT_ID_KEY

    match id_source:
        case "by_key":
            derived = (
                operator.itemgetter(resolved_key)
                if id_default is None
                else lambda value: value.get(resolved_key, id_default)
            )
        case "by_subkey":
            derived = lambda value: (entry[resolved_key] for entry in _section_entries(value))
            model_mapper = model_mapper or _section_entries
        case "mapping_key":
            derived = iter
            model_mapper = model_mapper or _section_entries
        case "default":
            derived = lambda _: id_default
        case _:
            raise ValueError(f"Config section {key!r} has an unknown ID source {id_source!r}")

    if key in _registry:
        raise ValueError(f"Config section {key!r} already registered")

    keyed = id_source in KEYED_ID_SOURCES

    _registry[key] = ConfigSection(
        key=key,
        adapter=TypeAdapter(parse_type),
        id_source=id_source,
        id_mapper=id_mapper or derived,
        model_mapper=model_mapper,
        service_path=service_path,
        id_key=resolved_key if keyed else None,
        id_key_required=keyed and id_default is None,
    )


def get_config_section(key: str) -> ConfigSection | None:
    """Retrieve a registered config section handler.

    Args:
        key: The top-level YAML key to look up.

    Returns:
        The registered section, or None if the key is not found in the registry.
    """
    return _registry.get(key)


def config_sections() -> dict[str, ConfigSection]:
    """Retrieve every registered config section handler.

    Returns:
        A snapshot of the registry, keyed by top-level YAML key.
    """
    return dict(_registry)
