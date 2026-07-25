# SPDX-License-Identifier: Apache-2.0
"""Keyword registration, lookup, and serialization for the sensorkit data model."""

import functools
from collections.abc import Iterable, Mapping
from functools import partial
from typing import (
    Annotated,
    Any,
    ClassVar,
    Literal,
    NamedTuple,
    Protocol,
    Self,
    Unpack,
    overload,
    override,
    runtime_checkable,
)

from pydantic import BaseModel, GetCoreSchemaHandler, TypeAdapter, ValidationError
from pydantic_core import core_schema

from sensorkit.common.model import ModelRegistry

_keyword_unknown_key = "__unknown__"
_keyword_registry = ModelRegistry(default_tag=_keyword_unknown_key)

Keyword = Annotated[object, _keyword_registry.discriminator()]
"""Pydantic type annotation matching any dynamically registered keyword type."""

type KeywordKind = Literal["stream", "state", "config"]


class KeywordInfo(NamedTuple):
    """Registration metadata for a declared keyword type."""

    key: str
    namespace: str | None
    kind: KeywordKind | None


_keyword_adapter = TypeAdapter(Keyword)
_keyword_index: dict[type, KeywordInfo] = {}


def declare_keyword[M](
    cls: type[M] | None = None,
    *,
    key: str | None = None,
    ns: str | None = None,
    kind: KeywordKind = "stream",
) -> partial[type[M]] | type[M]:
    """Decorator to register a type as a Keyword.

    Args:
        cls: The type to register.
        key: The keyword key. Defaults to the class name.
        ns: Optional namespace, forwarded to the model registry.
        kind: The keyword variant; can be `config`, `state`, or `stream` (the default).
    """
    if cls is None:
        return functools.partial(declare_keyword, key=key, ns=ns, kind=kind)

    if cls in _keyword_index:
        raise KeywordError(f"type '{cls.__name__}' already declared as '{_keyword_index[cls].key}'")

    key = key or cls.__name__
    _keyword_registry.add(cls, tag=key, namespace=ns)
    _keyword_index[cls] = KeywordInfo(key=key, namespace=ns, kind=kind)
    return cls


def get_keyword_info(obj: type | object) -> KeywordInfo | None:
    """Return the `KeywordInfo` for a registered keyword type or instance, or `None` if not registered."""
    if not isinstance(obj, type):
        obj = type(obj)

    return _keyword_index.get(obj)


def is_keyword(key: str) -> bool:
    """Return whether `key` is declared as a keyword in any namespace."""
    return bool(_keyword_registry.get_namespaces(key))


def dump_keyword_json(obj: object):
    """Serialize a keyword object to JSON bytes using the shared keyword type adapter."""
    return _keyword_adapter.dump_json(obj)


def validate_keyword(key: str, data: Any):
    """Validate and deserialize `data` as the keyword type identified by `key`."""
    return _keyword_adapter.validate_python(data, context={ModelRegistry.DISCRIMINATOR_CONTEXT: key})


def validate_keyword_json(key: str, json: bytes):
    """Validate and deserialize a JSON byte string as the keyword type identified by `key`."""
    return _keyword_adapter.validate_json(json, context={ModelRegistry.DISCRIMINATOR_CONTEXT: key})


def validated_items(dct: dict[str, object]) -> Iterable[tuple[str, object]]:
    """Yield `(key, value)` pairs from `dct`, deserializing dict values as keywords where possible."""
    for k, v in dct.items():
        if isinstance(v, dict):
            try:
                yield k, validate_keyword(k, v)
                continue
            except ValidationError:
                pass

        yield k, v


@declare_keyword(key=_keyword_unknown_key)
class UnknownKeyword(BaseModel, extra="allow"):
    """Fallback keyword type that accepts any extra fields for unrecognized keyword keys."""


class KeywordError(Exception):
    """Keyword error."""


@runtime_checkable
class CompositeKeyword(Protocol):
    """Protocol for a keyword that exports other keywords.

    Composite keywords may only compose a set of keywords that are a pure projection over
    its own fields. Expansion runs wherever a composite is read into a `Context`, including
    on the consumer side after deserialization, so anything not carried in the keyword's own
    serialized fields would simply be absent there.
    """

    def composed_keywords(self) -> Iterable[object]:
        """Yield the child keywords this keyword carries."""


class KeywordDict(Mapping[str, Any]):
    """A mapping of keywords, keyed by their registered keyword key.

    Values are inserted through `set` (keyword objects, keyed by their type) or `set_value` (a
    named non-keyword value). Some, but not all, of the `MutableMapping` interface is supported.
    """

    NO_DEFAULT: ClassVar[object] = object()

    def __init__(
        self,
        arg: Any = None,
        *objs: Unpack[tuple[object, ...]],
    ):
        self._data: dict[str, Any] = {}

        match arg:
            case None:
                pass
            case _ if type(arg) in _keyword_index:
                self.set(arg)
            case KeywordDict():
                self.update(arg)
            case Mapping():
                for key, value in arg.items():
                    self.set_value(key, value)
            case Iterable():
                for key, value in arg:
                    self.set_value(key, value)
            case _:
                raise RuntimeError(f"KeywordDict arg has invalid type {type(arg)}")

        self.set(*objs)

    def set(self, *objs: Unpack[tuple[object, ...]]):
        """Insert keywords."""
        for obj in objs:
            self.set_keyword(obj)

    def set_keyword(self, obj: object):
        """Insert a keyword."""
        self._data[_keyword_index[type(obj)].key] = obj

    def set_value(self, key: str, value: Any):
        """Insert a named non-keyword value, reachable by that name.

        Transitional accommodation for values not (yet) modeled as keywords.
        """
        self._data[key] = value

    def update(self, other: Mapping[str, Any]):
        """Copy every entry from another mapping into this one, overwriting on key collision."""
        self._data.update(other._data if isinstance(other, KeywordDict) else other)

    @classmethod
    def _validate(cls, obj):
        if obj is None:
            return cls()

        if isinstance(obj, dict):
            return cls(validated_items(obj))

        return obj

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, handler: GetCoreSchemaHandler):
        return core_schema.json_or_python_schema(
            json_schema=core_schema.chain_schema(
                [
                    core_schema.nullable_schema(core_schema.dict_schema()),
                    core_schema.no_info_plain_validator_function(
                        function=cls._validate,
                    ),
                ]
            ),
            python_schema=core_schema.no_info_plain_validator_function(
                function=cls._validate,
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: dict(v) if v is not None else None,
                return_schema=core_schema.nullable_schema(core_schema.dict_schema()),
            ),
        )

    def copy(self) -> Self:
        return self.__class__(self)

    @override
    def __iter__(self):
        return iter(self._data)

    @override
    def __len__(self):
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        match other:
            case KeywordDict():
                return self._data == other._data
            case Mapping():
                return self._data == dict(other)
            case _:
                return NotImplemented

    __hash__ = None

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    @overload
    def get[M](self, cls: type[M], default: M | None = None) -> M | None: ...

    @overload
    def get(self, key: str, default: Any = None) -> Any: ...

    @override
    def get(self, key, default=None):
        if isinstance(key, type):
            entry = _keyword_index.get(key)
            return default if entry is None else self._data.get(entry.key, default)

        return self._data.get(key, default)

    @overload
    def pop[M](self, cls: type[M], default: Any = ...) -> M | None: ...

    @overload
    def pop(self, key: str, default: Any = ...) -> Any: ...

    def pop(self, key, default=NO_DEFAULT):
        if isinstance(key, type):
            entry = _keyword_index.get(key)

            if entry is None:
                if default is self.NO_DEFAULT:
                    raise KeyError(key.__name__)
                return default

            key = entry.key

        if default is self.NO_DEFAULT:
            return self._data.pop(key)

        return self._data.pop(key, default)

    @overload
    def __getitem__[M](self, cls: type[M]) -> M: ...

    @overload
    def __getitem__(self, key: str) -> Any: ...

    @override
    def __getitem__(self, key, /):
        if isinstance(key, type):
            return self._data[_keyword_index[key].key]
        return self._data[key]

    @overload
    def __delitem__(self, cls: type) -> None: ...

    @overload
    def __delitem__(self, key: str) -> None: ...

    def __delitem__(self, key, /):
        if isinstance(key, type):
            del self._data[_keyword_index[key].key]
        else:
            del self._data[key]
