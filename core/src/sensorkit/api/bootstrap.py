import asyncio
import os
import pathlib
import warnings
from collections.abc import Iterable
from typing import Literal, cast, overload

from sensorkit.backend.base import BackendImpl
from sensorkit.common.importutil import import_module_or_file, obj_from_spec
from sensorkit.config.parser import SensorKitBaseConfig, SensorKitConfig, parse_config

DEFAULT_BACKEND = "nats"
DEFAULT_CONFIG_FILE = "sensorkit.yaml"
DEFAULT_BASE_IMPORTS = (
    "sensorkit.std",
    "sensorkit.data.filesys",
    "sensorkit.data.fits",
    "sensorkit.data.local",
    "sensorkit.models.devices",
)
BACKEND_MODULES = {
    "nats": "sensorkit.backend.nats",
    "fake": "sensorkit.backend.fake",
}

_read_lock = asyncio.Lock()
_base: SensorKitBaseConfig | None = None
_resolved: SensorKitConfig | None = None


@overload
async def _read_config(*, required: Literal[False] = False) -> SensorKitBaseConfig | None: ...


@overload
async def _read_config(*, required: Literal[True]) -> SensorKitBaseConfig: ...


async def _read_config(*, required: bool = False):
    """Read and memoize the base config.

    Callers must hold `_read_lock`.
    """
    global _base

    def _read_config_sync(location: str | None):
        import yaml

        path = pathlib.Path(location or DEFAULT_CONFIG_FILE)

        try:
            base = parse_config(yaml.safe_load(path.read_text()))
        except FileNotFoundError:
            if required or location is not None:
                raise

            base = None

        return base

    if _base is None:
        _base = await asyncio.to_thread(
            _read_config_sync,
            os.environ.get("SENSORKIT_CONFIG"),
        )

    return _base


def _imports_from_env(var: str, default: Iterable[str] = ()) -> list[str]:
    return [mod.strip() for mod in os.environ.get(var, "").split(",") if mod] or list(default)


def _import_modules_sync(*, fail_policy: Literal["error", "warn", "ignore"]):
    imports = _imports_from_env("SENSORKIT_BASE_IMPORTS", DEFAULT_BASE_IMPORTS)
    imports.extend(_imports_from_env("SENSORKIT_IMPORTS"))

    if _base is not None:
        imports.extend(_base.configured_imports())

    for module in imports:
        try:
            import_module_or_file(module)
        except Exception:
            match fail_policy:
                case "error":
                    raise
                case "warn":
                    warnings.warn(f"Failed to import: {module}", stacklevel=1)
                case "ignore":
                    pass


def set_config_location(location: str | None) -> None:
    """Set the location of the SensorKit configuration file.

    This must be called before `load_config`, `import_modules`, and `connect`
    to have effect.
    """
    if location is not None:
        current = os.environ.get("SENSORKIT_CONFIG", DEFAULT_CONFIG_FILE)

        if _base is not None and location != current:
            raise RuntimeError(f"config already loaded from {current}, cannot set to {location}")

        os.environ["SENSORKIT_CONFIG"] = location


async def load_config(
    *,
    fail_policy: Literal["error", "warn", "ignore"] = "warn",
) -> SensorKitConfig:
    """Loads and fully resolves configuration, importing modules as needed.

    The fully resolved configuration is memoized; subsequent calls return the
    cached result without re-resolving.
    """
    global _resolved

    async with _read_lock:
        if _resolved is None:
            base = await _read_config(required=True)
            await asyncio.to_thread(_import_modules_sync, fail_policy=fail_policy)
            _resolved = base.resolve_dynamic_sections()

    return cast(SensorKitConfig, _resolved)


async def import_modules(*, fail_policy: Literal["error", "warn", "ignore"] = "error"):
    """Imports modules based on environment and configuration."""
    async with _read_lock:
        await _read_config()

    await asyncio.to_thread(_import_modules_sync, fail_policy=fail_policy)


async def connect(*, backend: str | None = None):
    """Creates a SensorKit client and connects to the backend.

    The backend is either provided as a string, read from the environment, or
    (if neither is present) read from the SensorKit configuration file.

    User modules are not imported automatically. Callers that need to access
    types provided by modules must opt in to these imports by calling
    `import_modules()` or `load_config()` (to retrieve the fully resolved
    configuration structure).
    """
    from sensorkit.core.client import SensorKit

    backend = backend or os.environ.get("SENSORKIT_BACKEND")

    if not backend:
        async with _read_lock:
            config = await _read_config()

        backend = config.sensorkit.backend if config else None

    backend_module = BACKEND_MODULES.get(backend) or BACKEND_MODULES[DEFAULT_BACKEND]
    backend_cls = await asyncio.to_thread(
        obj_from_spec,
        spec=backend_module,
        base=BackendImpl,
        subclass=True,
    )
    backend_impl = await backend_cls.create()
    return SensorKit(backend=backend_impl)
