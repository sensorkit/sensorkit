import importlib
import os
import warnings

from sensorkit.backend.base import BackendImpl
from sensorkit.common.importutil import obj_from_spec
from sensorkit.core.client import SensorKit


async def connect():
    """Reads configuration to determine a backend and then creates a SensorKit client."""
    # Determine the backend based on user configuration.
    # TODO: minimal implementation for now
    backend_module = os.environ.get("SENSORKIT_BACKEND", "sensorkit.backend.nats")
    backend_arg = os.environ.get("SENSORKIT_BACKEND_ARG", "nats://127.0.0.1:4222")

    cls = obj_from_spec(
        spec=backend_module,
        base=BackendImpl,
        subclass=True,
    )

    # Do dynamic module imports based on configuration.
    # TODO: Implement real configurability.
    imports = ["sensorkit.std.collect", "sensorkit.models.devices", "sensorkit.data.filesys",
               "sensorkit.data.fits", "sensorkit.data.local"]
    imports.extend(
        mod.strip() for mod in os.environ.get("SENSORKIT_IMPORTS", "").split(",") if mod
    )

    for mod in imports:
        try:
            importlib.import_module(mod)
        except Exception:
            warnings.warn(f"Failed to import: {mod}", stacklevel=2)

    # Create the Backend instance.
    backend_impl = await cls.create(backend_arg)
    assert backend_impl
    return SensorKit(backend=backend_impl)
