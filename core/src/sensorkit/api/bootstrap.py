import os
import warnings
from typing import Literal

from dotenv import find_dotenv, load_dotenv

from sensorkit.backend.base import BackendImpl
from sensorkit.common.importutil import import_module_or_file, obj_from_spec
from sensorkit.core.client import SensorKit

DEFAULT_BASE_IMPORTS = (
    "sensorkit.std",
    "sensorkit.data.filesys",
    "sensorkit.data.fits",
    "sensorkit.data.local",
    "sensorkit.models.devices",
)


def import_plugins(
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


async def connect():
    """Reads configuration to determine a backend and then creates a SensorKit client."""
    load_dotenv(find_dotenv(usecwd=True))

    # Determine the backend based on user configuration.
    backend_module = os.environ.get("SENSORKIT_BACKEND", "sensorkit.backend.nats")

    cls = obj_from_spec(
        spec=backend_module,
        base=BackendImpl,
        subclass=True,
    )

    # Do dynamic module imports based on configuration.
    import_plugins(fail_policy="warn", warn_stacklevel=3)

    # Create the Backend instance.
    backend_impl = await cls.create()
    assert backend_impl
    return SensorKit(backend=backend_impl)
