# SPDX-License-Identifier: Apache-2.0
"""Keyword registration, lookup, and serialization for the sensorkit data model."""

import functools
from collections.abc import Iterable
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


@overload
def declare_keyword[M](
    *,
    key: str | None = ...,
    ns: str | None = ...,
    kind: KeywordKind = ...,
) -> partial[type[M]]: ...


@overload
def declare_keyword[M](
    cls: type[M],
    *,
    key: str | None = ...,
    ns: str | None = ...,
    kind: KeywordKind = ...,
) -> type[M]: ...


def declare_keyword[M](
    cls: type[M] | None = None,
    *,
    key: str | None = None,
    ns: str | None = None,
    kind: KeywordKind = "stream",
):
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
    """Protocol for a keyword that exports other keywords."""

    def composed_keywords(self) -> Iterable[object]: ...


class KeywordDict(dict[str, Any]):
    """A dict with keyword-specific methods and overloads."""

    NO_DEFAULT: ClassVar[object] = object()

    def __init__(
        self,
        arg: Any = None,
        *objs: Unpack[tuple[object, ...]],
        **kwargs,
    ):
        match arg:
            case None:
                super().__init__(**kwargs)
            case _ if type(arg) in _keyword_index:
                super().__init__(**kwargs)
                self.set(arg)
            case KeywordDict():
                super().__init__(arg.items(), **kwargs)
            case Iterable():
                super().__init__(arg, **kwargs)
            case _:
                raise RuntimeError(f"KeywordDict arg has invalid type {type(arg)}")

        if objs:
            self.set(*objs)

    def _set_composed(self, obj: CompositeKeyword, visited: set[int]):
        # DFS over composed keywords.
        for composed in obj.composed_keywords():
            if id(composed) in visited:
                continue

            visited.add(id(composed))

            if isinstance(composed, CompositeKeyword):
                self._set_composed(composed, visited)

            self[type(composed)] = composed

    def set(self, *objs: Unpack[tuple[object, ...]]):
        """Insert one or more keyword objects, keying each by its registered keyword key.

        If a keyword is a `CompositeKeyword`, first insert its composed keywords, recursively.
        """
        for obj in objs:
            if isinstance(obj, CompositeKeyword):
                self._set_composed(obj, {id(obj)})

            self[type(obj)] = obj

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

    @override
    def copy(self) -> Self:
        return self.__class__(self)

    @overload
    def get[M](self, cls: type[M], default: M = None) -> M | None: ...

    @overload
    def get(self, key: str, default: Any = None) -> Any: ...

    @override
    def get(self, key, default=None):
        if isinstance(key, type):
            entry = _keyword_index.get(key)
            return default if entry is None else super().get(entry.key, default)
        return super().get(key, default)

    @overload
    def pop[M](self, cls: type[M], default: Any = NO_DEFAULT) -> M | None: ...

    @overload
    def pop(self, key: str, default: Any = NO_DEFAULT) -> Any: ...

    @override
    def pop(self, key, default=NO_DEFAULT):
        if isinstance(key, type):
            entry = _keyword_index.get(key)

            if entry is None:
                if default is self.NO_DEFAULT:
                    raise KeyError(key.__name__)
                return default

            return (
                super().pop(entry.key)
                if default is self.NO_DEFAULT
                else super().pop(entry.key, default)
            )

        return super().pop(key) if default is self.NO_DEFAULT else super().pop(key, default)

    @overload
    def __getitem__[M](self, cls: type[M]) -> M: ...

    @overload
    def __getitem__(self, key: str) -> Any: ...

    @override
    def __getitem__(self, key, /):
        if isinstance(key, type):
            return super().__getitem__(_keyword_index[key].key)
        return super().__getitem__(key)

    @overload
    def __delitem__(self, cls: type) -> None: ...

    @overload
    def __delitem__(self, key: str) -> None: ...

    @override
    def __delitem__(self, key, /):
        if isinstance(key, type):
            super().__delitem__(_keyword_index[key].key)
        else:
            super().__delitem__(key)

    @overload
    def __setitem__(self, cls: type, value: Any) -> None: ...

    @overload
    def __setitem__(self, key: str, value: Any) -> None: ...

    @override
    def __setitem__(self, key, value, /):
        if isinstance(key, type):
            super().__setitem__(_keyword_index[key].key, value)
        else:
            super().__setitem__(key, value)
