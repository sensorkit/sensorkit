import asyncio
from typing import Self

from alpaca.exceptions import (
    InvalidValueException,
    NotConnectedException,
    NotImplementedException,
)
from alpaca.filterwheel import FilterWheel as AscomFilterWheel
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Filter, Filters


# Safely read Alpaca device properties that may not be implemented by the driver.
def _safe_get(d: AscomFilterWheel, name: str, default=None):
    try:
        return getattr(d, name)
    except NotImplementedException:
        logger.debug(f"FilterWheel driver reports '{name}' not implemented; using default")
        return default
    except Exception as e:
        logger.debug(f"Failed to read filter wheel property '{name}': {e}")
        return default


# Async version of _safe_get using to_thread
async def _safe_get_async(d: AscomFilterWheel, name: str, default=None):
    return await asyncio.to_thread(_safe_get, d, name, default)


@sk.declare_keyword
class FilterWheelCapabilities(BaseModel):
    has_names: bool | None = None
    has_focus_offsets: bool | None = None
    slots_count: int | None = None

    @classmethod
    def from_device(cls, d: AscomFilterWheel):
        names = _safe_get(d, "Names", None) or []
        focus = _safe_get(d, "FocusOffsets", None)
        return cls(
            has_names=True if names else False,
            has_focus_offsets=True if (focus is not None and len(focus) == len(names)) else False,
            slots_count=len(names) if names else None,
        )


@sk.declare_keyword
class FilterWheelCurrentSettings(BaseModel):
    position: int | None = None
    name: str | None = None
    moving: bool | None = None
    focus_offset: int | float | None = None

    @classmethod
    def from_device(cls, d: AscomFilterWheel):
        pos = _safe_get(d, "Position", None)
        names = _safe_get(d, "Names", None) or []
        moving = True if pos == -1 else False if pos is not None else None
        name = None
        if isinstance(pos, int) and pos >= 0 and pos < len(names):
            try:
                name = names[pos]
            except Exception:
                name = None
        focus = _safe_get(d, "FocusOffsets", None)
        focus_offset = None
        if isinstance(pos, int) and pos >= 0 and focus is not None and pos < len(focus):
            try:
                focus_offset = focus[pos]
            except Exception:
                focus_offset = None
        return cls(position=pos if isinstance(pos, int) and pos >= -1 else None, name=name, moving=moving, focus_offset=focus_offset)


class FilterWheelTelemetryPublisher:
    """Publishes filter wheel telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        filter_wheel: AscomFilterWheel,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.fw = filter_wheel
        self.frequency = frequency
        self._task: asyncio.Task | None = None
        self.capabilities: FilterWheelCapabilities | None = None
        self.settings: FilterWheelCurrentSettings | None = None

    async def start(self):
        """Start the telemetry publisher background task."""
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        """Stop the telemetry publisher background task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        """Background loop publishing telemetry at the configured frequency."""
        backoff = 1.0
        while True:
            try:
                await asyncio.gather(
                    self.publish_connected(),
                    self.publish_filter(),
                    self.publish_capabilities(),
                    self.publish_current_settings(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Filter wheel telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.fw.Connected)
            await self.binding.publish(
                sk.Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_filter(self):
        try:
            pos = await _safe_get_async(self.fw, "Position", None)
            names = await _safe_get_async(self.fw, "Names", None) or []
            name = None
            if isinstance(pos, int) and pos >= 0 and pos < len(names):
                try:
                    name = names[pos]
                except Exception:
                    name = None
            await self.binding.publish(
                sk.Filter(name=name, position=pos if isinstance(pos, int) else None)
            )
        except Exception:
            return None

    async def publish_capabilities(self):
        try:
            if self.capabilities:
                await self.binding.publish(self.capabilities)
        except Exception:
            return None

    async def publish_current_settings(self):
        try:
            self.settings = await asyncio.to_thread(FilterWheelCurrentSettings.from_device, self.fw)
            await self.binding.publish(self.settings)
        except Exception:
            return None


@sk.declare_device
class FilterWheelService:

    def __init__(
        self,
        device: AscomFilterWheel,
        status_frequency: float,
    ):
        self.fw = device
        self.status_frequency = status_frequency
        self.filters: dict[str, int] = {}
        self.telemetry_publisher: FilterWheelTelemetryPublisher | None = None

    def _names(self) -> list[str]:
        names = _safe_get(self.fw, "Names", None) or []
        return names

    def _ensure_filter_map(self):
        names = self._names()
        self.filters = {str(n).strip().lower(): i for i, n in enumerate(names) if n is not None and str(n).strip() != ""}

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomFilterWheel(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        device = sk.device()

        self.fw.Connected = True
        await device.publish(sk.Connected(is_connected=True))

        try:
            names = _safe_get(self.fw, "Names", None) or []
            self.filters = {str(n).strip().lower(): i for i, n in enumerate(names) if n is not None and str(n).strip() != ""}
            if names:
                await device.kv_put_model(Filters(
                    filters=[
                        Filter(name=str(n).strip().lower(), position=i) for i,n in enumerate(names)
                    ]
                ))

            # Initialize telemetry publisher
            self.telemetry_publisher = FilterWheelTelemetryPublisher(
                binding=device,
                filter_wheel=self.fw,
                frequency=self.status_frequency
            )

            # Populate initial state
            self.telemetry_publisher.capabilities = FilterWheelCapabilities.from_device(self.fw)
            self.telemetry_publisher.settings = FilterWheelCurrentSettings.from_device(self.fw)

            # Start background telemetry publishing
            await self.telemetry_publisher.start()

        except NotConnectedException as e:
            raise RuntimeError("Not connected") from e
        except NotImplementedException as e:
            logger.warning(f"Some filter wheel properties are not implemented: {e}")
        except Exception as e:
            logger.warning(f"Filter wheel startup property read error: {e}")

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.fw.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: sk.Connect):
        try:
            self.fw.Connected = True
            await sk.device().publish(sk.Connected(is_connected=True))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Filter wheel connect failed")
            raise RuntimeError("Connect failed") from e

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: sk.Disconnect):
        try:
            self.fw.Connected = False
            await sk.device().publish(sk.Connected(is_connected=False))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Filter wheel disconnect failed")
            raise RuntimeError("Disconnect failed") from e

    @sk.command_handler
    async def filter_wheel_set_position(self, cmd: sk.SetFilter):
        try:
            # Determine target index
            target_index: int | None = None
            if isinstance(cmd.filter, int):
                target_index = cmd.filter
                logger.debug(f"SetFilter: received integer position {target_index}")
            elif isinstance(cmd.filter, str):
                self._ensure_filter_map()
                filter_key = cmd.filter.strip().lower()
                target_index = self.filters.get(filter_key)
                logger.debug(f"SetFilter: received string '{cmd.filter}' -> key '{filter_key}' -> index {target_index}")
                logger.debug(f"SetFilter: available filter map: {self.filters}")
            else:
                target_index = None

            if target_index is None:
                raise InvalidValueException(f"Filter unavailable: '{cmd.filter}' not found in {list(self.filters.keys())}")

            names = self._names()
            if target_index < 0 or target_index >= len(names):
                raise InvalidValueException("Filter index out of range")

            current = _safe_get(self.fw, "Position", None)
            if isinstance(current, int) and current == target_index:
                return

            # Issue move and wait for completion
            self.fw.Position = target_index

            async def _wait_for_position(target: int, timeout_s: float = 60.0):
                async with asyncio.timeout(timeout_s):
                    while True:
                        pos = _safe_get(self.fw, "Position", None)
                        if isinstance(pos, int) and pos >= 0 and pos == target:
                            break
                        await asyncio.sleep(0.1)

            await _wait_for_position(target_index)
        except asyncio.TimeoutError as e:
            logger.error("Filter wheel move timed out")
            raise RuntimeError("ChangeFilter timeout") from e
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except InvalidValueException:
            raise
        except Exception as e:
            logger.exception("ChangeFilter failed")
            raise RuntimeError("ChangeFilter failed") from e
