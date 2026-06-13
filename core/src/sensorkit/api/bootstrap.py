import asyncio
import os
import pathlib
import warnings
from typing import Literal

from dotenv import find_dotenv, load_dotenv

from sensorkit.backend.base import BackendImpl
from sensorkit.common.importutil import import_module_or_file, obj_from_spec
from sensorkit.config.parser import SensorKitConfig, parse_config

DEFAULT_BACKEND = "sensorkit.backend.nats"
DEFAULT_CONFIG_FILE = "sensorkit.yaml"
DEFAULT_BASE_IMPORTS = (
    "sensorkit.std",
    "sensorkit.data.filesys",
    "sensorkit.data.fits",
    "sensorkit.data.local",
    "sensorkit.models.devices",
)


def import_modules(
    *,
    extra_imports: list[str] | None = None,
    fail_policy: Literal["error", "warn", "ignore"] = "error",
    warn_stacklevel: int = 2,
):
    load_dotenv(find_dotenv(usecwd=True))

    imports = [
        mod.strip() for mod in os.environ.get("SENSORKIT_BASE_IMPORTS", "").split(",") if mod
    ]

    if not imports:
        imports.extend(DEFAULT_BASE_IMPORTS)

    imports.extend(
        mod.strip() for mod in os.environ.get("SENSORKIT_IMPORTS", "").split(",") if mod
    )

    if extra_imports:
        imports.extend(extra_imports)

    for module in imports:
        try:
            import_module_or_file(module)
        except Exception:
            match fail_policy:
                case "error":
                    raise
                case "warn":
                    warnings.warn(f"Failed to import: {module}", stacklevel=warn_stacklevel)
                case "ignore":
                    pass


def _connect_sync(default_backend: str):
    # Do dynamic module imports based on configuration.
    import_modules(fail_policy="warn", warn_stacklevel=5)

    # Determine the backend based on user configuration.
    backend_module = os.environ.get(
        "SENSORKIT_BACKEND",
        default_backend,
    )

    return obj_from_spec(
        spec=backend_module,
        base=BackendImpl,
        subclass=True,
    )


async def connect(*, default_backend: str | None = None):
    """Reads configuration to determine a backend and then creates a SensorKit client."""
    from sensorkit.core.client import SensorKit

    default_backend = default_backend or DEFAULT_BACKEND
    backend_cls = await asyncio.to_thread(_connect_sync, default_backend)
    backend_impl = await backend_cls.create()
    return SensorKit(backend=backend_impl)


def _load_config_sync(path: pathlib.Path):
    import yaml

    base = parse_config(yaml.safe_load(path.read_text()))

    import_modules(
        extra_imports=base.configured_imports(),
        fail_policy="warn",
        warn_stacklevel=5,
    )

    return base.resolve_dynamic_sections()


async def load_config(*, default_location: str | None = None) -> SensorKitConfig:
    default_location = default_location or DEFAULT_CONFIG_FILE
    path = pathlib.Path(os.environ.get("SENSORKIT_CONFIG", default_location))
    return await asyncio.to_thread(_load_config_sync, path)
