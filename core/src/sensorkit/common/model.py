"""Pydantic model registry infrastructure for discriminated-union validation."""

from __future__ import annotations

import collections
import contextlib
import functools
import inspect
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Any, ClassVar, Self, override

from loguru import logger
from pydantic import (
    BaseModel,
    GetCoreSchemaHandler,
    ModelWrapValidatorHandler,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    ValidationInfo,
    WrapSerializer,
    WrapValidator,
)
from pydantic_core import PydanticUndefined


class RegistryError(Exception):
    """Raised for tag conflicts during ModelRegistryView resolution."""


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    """Metadata record for a type registered in a ModelRegistry."""

    model_type: type
    tag: str | None
    namespace: str | None


class ModelRegistryBase[T](ABC):
    """Base for model registry types."""

    DISCRIMINATOR_CONTEXT: ClassVar[str] = "discriminator"
    REGISTRY_CONTEXT: ClassVar[str] = "registry"

    @abstractmethod
    def validate(self, data: Any, *, info: ValidationInfo | None = None) -> T:
        """Validate ``data`` and return a typed model instance from this registry."""
        ...

    @abstractmethod
    def __contains__(self, val: type[T]) -> bool: ...


def extract_field_default_value(type_: type, field: str | None):
    """Return the default value of ``field`` on ``type_``, checking class dict and pydantic field info."""
    if not field:
        return None

    # Only defaults declared directly on this class, not inherited attrs.
    if field in type_.__dict__:
        value = type_.__dict__[field]

        if value is not None:
            return value

    if issubclass(type_, BaseModel):
        field_info = type_.model_fields.get(field)

        if field_info is not None and field_info.default is not PydanticUndefined:
            return field_info.default

    return None


class ModelRegistry[T](ModelRegistryBase[T]):
    """A mutable collection of pydantic models.

    Types register here at import time via ``__init_subclass__`` hooks or decorators, with
    optional namespace metadata.

    Operations are not thread-safe.
    """

    def __init__(
        self,
        *models: type[T],
        discriminator: str | None = None,
        default_tag: str = None,
    ):
        self.discriminator_field = discriminator
        self.default_tag = default_tag
        self._entries: dict[type[T], RegistryEntry] = {}
        self._tag_index: dict[str, dict[str | None, type[T]]] = collections.defaultdict(dict)
        self._update_default_view = False

        for model_type in models:
            self.add(model_type)

    def add(self, model_type: type[T], *, tag: str | None = None, namespace: str | None = None):
        """Register a model type in the registry.

        The next time `validate()` is called, the registry cache will be updated to include the new
        model. This interaction is not thread safe.

        Args:
            model_type: The model class to register. Abstract classes are ignored and trigger a
                warning.
            tag: Optional discriminator tag value. If None, will be extracted from the model's
                discriminator field default value.
            namespace: Optional namespace for organizing models with the same tag.

        Raises:
            RegistryError: If the model type is already registered, if tag cannot be determined
                when no discriminator is set, if a tag conflict occurs in the same namespace,
                or if the registry has no discriminator field configured.
        """
        if inspect.isabstract(model_type):
            warnings.warn(
                f"cannot add abstract type {model_type.__name__} to registry", stacklevel=2
            )
            return

        if tag is None:
            if self.discriminator_field is None:
                raise RegistryError("cannot add model to registry without a discriminator value")

            tag = extract_field_default_value(model_type, self.discriminator_field)

            if tag in (None, PydanticUndefined) and self.default_tag is None:
                raise RegistryError(
                    f"could not determine discriminator value for {model_type.__name__}"
                )

        if model_type in self._entries:
            raise RegistryError(f"model type {model_type.__name__} already registered")

        self._entries[model_type] = RegistryEntry(
            model_type=model_type, tag=tag, namespace=namespace
        )

        if tag is not None:
            if namespace in self._tag_index[tag]:
                raise RegistryError(f"tag '{tag}' already defined in namespace '{namespace}'")

            self._tag_index[tag][namespace] = model_type

        self._update_default_view = True

    @property
    def entries(self):
        """A read-only mapping of registered model types to their ``RegistryEntry`` metadata."""
        return MappingProxyType(self._entries)

    def get_tags(self):
        """Return all discriminator tags currently registered in this registry."""
        return tuple(self._tag_index.keys())

    def get_namespaces(self, tag: str):
        """Get all namespaces for a given tag."""
        if tag in self._tag_index:
            return tuple(self._tag_index[tag].keys())
        return ()

    def get_type(self, tag: str, namespace: str | None = None):
        """Return the model type registered under ``tag`` in the given ``namespace``, or ``None``."""
        return self._tag_index[tag].get(namespace)

    def create_resolved_view(self, namespace_precedence: Iterable[str]) -> ModelRegistryView:
        """Create a ``ModelRegistryView`` of this registry with key conflict resolution."""
        return ModelRegistryView(self, namespace_precedence)

    @functools.cached_property
    def _default_view(self):
        return self.create_resolved_view(namespace_precedence=())

    def discriminator(self):
        """Return a ``RegistryDiscriminator`` pydantic annotation backed by this registry."""
        return RegistryDiscriminator(self)

    @functools.cache
    def type_adapter(self, model_type: type[T]) -> TypeAdapter:
        """Get a cached TypeAdapter for a given model type."""
        return TypeAdapter(model_type)

    @override
    def validate(self, data: Any, *, info: ValidationInfo | None = None) -> T:
        """Validate data against registered models.

        This method delegates validation to the default ModelRegistryView, which resolves
        model types based on discriminator tags without namespace precedence. The view is
        automatically updated if new models have been added to the registry since the last
        validation.

        Args:
            data: The data to validate. Can be a dict, model instance, or any object with
                the discriminator field.
            info: Optional validation context information that may contain discriminator
                tag overrides or registry view references.

        Returns:
            A validated instance of one of the registered model types.

        Raises:
            ValueError: If no discriminator tag is available, if no model is found for the
                tag, or if there's a discriminator mismatch.
            RegistryError: If tag conflicts exist in the default namespace resolution.
        """
        if self._update_default_view:
            self._default_view.update()
            self._update_default_view = False

        return self._default_view.validate(data, info=info)

    @override
    def __contains__(self, val: type[T]) -> bool:
        return val in self._entries


_registry_var: ContextVar[ModelRegistryView | None] = ContextVar("model_registry", default=None)
"""Active ModelRegistryView for the current execution context, if any."""


class ModelRegistryView[T](ModelRegistryBase[T]):
    """Immutable view of a ModelRegistry with effective keys resolved by namespace precedence."""

    def __init__(self, registry: ModelRegistry, precedence: Iterable[str] = ()):
        self._registry = registry
        self._precedence: dict[str, int] = {ns: i for i, ns in enumerate(precedence)}
        self._resolved: dict[str, type[T]] = {}
        self._index: dict[type[T], str] = {}

        self.update()

    def _resolve(self, tag: str, namespaces: tuple[str | None, ...]):
        logger.debug(f"resolving tag '{tag}' with namespaces: {namespaces}")
        best_prio = (None, float("inf"))

        for ns in namespaces:
            if ns is None:
                if best_prio[0] is None:
                    best_prio = (ns, float("inf"))
            else:
                prio = self._precedence.get(ns, float("inf"))

                if prio < best_prio[1]:
                    best_prio = (ns, prio)

        return best_prio[0]

    def update(self):
        """Update the registry view based on the current state of the underlying registry."""
        self._resolved.clear()
        self._index.clear()

        for tag in self._registry.get_tags():
            namespaces = self._registry.get_namespaces(tag)
            assert namespaces

            # Determine the namespace that takes precedence.
            resolved = self._resolve(tag, namespaces) if len(namespaces) > 1 else namespaces[0]

            # Build the forward and reverse tag to model mappings.
            self._resolved[tag] = self._registry.get_type(tag, namespace=resolved)
            self._index[self._resolved[tag]] = tag

    def validate(self, data: Any, *, info: ValidationInfo | None = None) -> T:
        """Validate data against this registry view."""
        # Determine the tag to use for validation.
        tag_from_context: str | None = (
            info.context.get(self.DISCRIMINATOR_CONTEXT) if info and info.context else None
        )
        tag_from_data = None

        if discriminator := self._registry.discriminator_field:
            match data:
                case dict():
                    tag_from_data = data.get(discriminator)
                case _:
                    tag_from_data = getattr(data, discriminator, None)

        # If both data and context provide a discriminator and they differ, error out.
        if tag_from_data and tag_from_context and tag_from_data != tag_from_context:
            raise ValueError(
                f"tag mismatch: data='{tag_from_data}' context='{tag_from_context}'"
            )

        model_type: type[T] | None = None
        tag: str | None = None

        if tag := tag_from_data or tag_from_context:
            model_type = self._resolved.get(tag)

        if model_type is None:
            tag = self._registry.default_tag
            model_type = self._resolved.get(tag)

            if model_type is None:
                raise ValueError(f"no model resolved for tag: {tag}")

        if not isinstance(data, dict):
            if type(data) is not model_type:
                raise ValueError(f"model {model_type.__name__} does not match tag: {tag}")

        return self._registry.type_adapter(model_type).validate_python(data, context=info.context)

    def __contains__(self, val: type[T]) -> bool:
        """Return ``True`` if ``val`` is a model type resolved by this view."""
        return val in self._index

    @contextlib.contextmanager
    def as_current(self):
        """Context manager that installs this registry as the active ContextVar."""
        token = _registry_var.set(self)
        try:
            yield self
        finally:
            _registry_var.reset(token)


class RegistryDiscriminator:
    """A pydantic discriminator implementation backed by a model registry or view."""

    def __init__(self, registry: ModelRegistry):
        self.registry = registry

    def _validate(self, data: Any, _handler: ModelWrapValidatorHandler, info: ValidationInfo):
        model_type = type(data)

        # Look for a registry view in the validation context. If none is found, fallback to the
        # contextvar.
        registry_view: ModelRegistryBase | None = (
            info.context and info.context.get(ModelRegistryView.REGISTRY_CONTEXT)
        ) or _registry_var.get()

        # If a concrete model instance is provided, accept it if it's part of the registry.
        if isinstance(data, BaseModel):
            exists = model_type in registry_view if registry_view else model_type in self.registry

            if not exists:
                raise ValueError(f"model {model_type.__name__} not in registry")

            return data

        # Validate using the registry view if available. This is guaranteed to be conflict-free
        # since namespaces have been resolved.
        if registry_view is not None:
            return registry_view.validate(data, info=info)

        # Otherwise, validate against the entire deep registry. This will fail if the tag is
        # defined in multiple namespaces.
        return self.registry.validate(data, info=info)

    def _serialize(self, data: Any, handler: SerializerFunctionWrapHandler):
        if isinstance(data, BaseModel):
            return data.model_dump()

        return handler(data)

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ):
        return handler(
            Annotated[
                source_type,
                WrapValidator(self._validate),
                WrapSerializer(self._serialize),
            ]
        )


class RegistryBaseModel(BaseModel, ABC):
    """Base class for pydantic models that auto-register into a ModelRegistry upon subclassing.

    This abstract base class does automatic registration of model subclasses into a ModelRegistry
    using the discriminator pattern. Each concrete subclass is registered when defined, with its
    class name used as the discriminator value by default.

    Subclasses must implement get_registry() to specify which ModelRegistry instance they should
    register with. The registry's discriminator field is automatically populated on each subclass
    with the appropriate discriminator value.

    The registration happens in two hooks:
    - __init_subclass__: Registers the class in the registry and sets up the discriminator field
    - __pydantic_init_subclass__: Ensures discriminator values are available at class scope

    Example:
        >>> registry = ModelRegistry(discriminator="type")
        >>> class MyBase(RegistryBaseModel):
        ...     type: str
        ...
        ...     @classmethod
        ...     def get_registry(cls):
        ...         return registry
        >>>
        >>> class MyModel(MyBase):
        ...     pass  # Automatically registered with discriminator value "MyModel"

    Note:
        The root RegistryBaseModel class and direct subclasses that define get_registry() are
        not registered themselves - only their concrete descendant classes are registered.
    """

    @classmethod
    @abstractmethod
    def model_registry(cls) -> ModelRegistry[Self]:
        """Return the ``ModelRegistry`` that this model class family registers into."""
        raise NotImplementedError

    @classmethod
    def model_tag(cls):
        """Get the effective discriminator value for this model class.

        Raises:
            KeyError: the model is not present in the registry
        """
        return cls.model_registry().entries[cls].tag

    @classmethod
    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        # Skip abstract classes.
        if inspect.isabstract(cls):
            return

        # Skip the root registered model class itself (e.g. Event), only process its subclasses.
        if cls is cls._find_registered_base():
            return

        registry = cls.model_registry()
        field = registry.discriminator_field
        tag = extract_field_default_value(cls, field)

        if tag is None:
            tag = cls.__name__

        if field:
            # RegistryBaseModel subclasses are expected to narrow the discriminator field type
            # (e.g. `op: Literal["foo"] = "foo"`), which pydantic treats as field shadowing and
            # warns about. Suppress that warning for this specific field name.
            warnings.filterwarnings(
                "ignore",
                message=rf'Field name "{field}" in ".*{cls.__name__}" shadows an attribute',
                category=UserWarning,
            )

        registry.add(cls, tag=tag)

    def model_post_init(self, __context: Any):
        """Ensure the registry's discriminator field is populated in each instance."""
        registry = self.model_registry()
        field = registry.discriminator_field
        cls = type(self)

        if cls not in registry.entries:
            raise RegistryError(f"Model {type(self).__name__} is not registered")

        tag = registry.entries[cls].tag

        if tag != registry.default_tag:
            object.__setattr__(self, field, tag)

    @classmethod
    def _find_registered_base(cls) -> type | None:
        """Find the first class in the MRO that directly inherits from RegisteredBaseModel."""
        for klass in cls.__mro__:
            if RegistryBaseModel in klass.__bases__:
                return klass

        return None

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any):
        if source_type.__name__ == "RegistryBaseModel":
            # This class should never be validated directly, but pydantic still builds its schema.
            return handler(Any)
        elif source_type is cls._find_registered_base():
            # Our direct subclasses, the "user" base model, use the registry's custom discriminator
            # schema to validate.
            return handler.generate_schema(Annotated[Any, cls.model_registry().discriminator()])
        else:
            # Each descendant subclass, members of the registry, are validated as normal.
            return handler(source_type)
