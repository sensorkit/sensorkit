import asyncio
from typing import Self

from alpaca.dome import Dome as AscomDome
from alpaca.dome import ShutterState
from alpaca.exceptions import NotConnectedException, NotImplementedException
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk


def _safe_get(d: AscomDome, name: str, default=None):
    try:
        return getattr(d, name)
    except NotImplementedException:
        logger.debug(f"Dome driver reports '{name}' not implemented; using default")
        return default
    except Exception as e:
        logger.debug(f"Failed to read dome property '{name}': {e}")
        return default


# Async version of _safe_get using to_thread
async def _safe_get_async(d: AscomDome, name: str, default=None):
    return await asyncio.to_thread(_safe_get, d, name, default)


@sk.declare_keyword
class DomeCapabilities(BaseModel):
    can_find_home: bool | None = None
    can_park: bool | None = None
    can_set_altitude: bool | None = None
    can_set_azimuth: bool | None = None
    can_set_park: bool | None = None
    can_set_shutter: bool | None = None
    can_slave: bool | None = None
    can_sync_azimuth: bool | None = None

    @classmethod
    def from_device(cls, d: AscomDome):
        return cls(
            can_find_home=_safe_get(d, "CanFindHome", None),
            can_park=_safe_get(d, "CanPark", None),
            can_set_altitude=_safe_get(d, "CanSetAltitude", None),
            can_set_azimuth=_safe_get(d, "CanSetAzimuth", None),
            can_set_park=_safe_get(d, "CanSetPark", None),
            can_set_shutter=_safe_get(d, "CanSetShutter", None),
            can_slave=_safe_get(d, "CanSlave", None),
            can_sync_azimuth=_safe_get(d, "CanSyncAzimuth", None),
        )


@sk.declare_keyword
class DomeCurrentSettings(BaseModel):
    slaved: bool | None = None
    at_home: bool | None = None
    at_park: bool | None = None
    slewing: bool | None = None
    shutter_state: str | None = None
    azimuth: float | None = None
    altitude: float | None = None

    @classmethod
    def from_device(cls, d: AscomDome):
        shutter = _safe_get(d, "ShutterStatus", None)
        shutter_str = None
        try:
            shutter_str = shutter.name if hasattr(shutter, "name") else (str(shutter) if shutter is not None else None)
        except Exception:
            shutter_str = None
        return cls(
            slaved=_safe_get(d, "Slaved", None),
            at_home=_safe_get(d, "AtHome", None),
            at_park=_safe_get(d, "AtPark", None),
            slewing=_safe_get(d, "Slewing", None),
            shutter_state=shutter_str,
            azimuth=_safe_get(d, "Azimuth", None),
            altitude=_safe_get(d, "Altitude", None),
        )


class DomeTelemetryPublisher:
    """Publishes dome telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        dome: AscomDome,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.dome = dome
        self.frequency = frequency
        self._task: asyncio.Task | None = None
        self.capabilities: DomeCapabilities | None = None
        self.settings: DomeCurrentSettings | None = None

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
                    self.publish_opened(),
                    self.publish_capabilities(),
                    self.publish_current_settings(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Dome telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.dome.Connected)
            await self.binding.publish(
                sk.Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_opened(self):
        try:
            status = await _safe_get_async(self.dome, "ShutterStatus", None)
            is_open = (status == ShutterState.shutterOpen) if status is not None else False
            await self.binding.publish(sk.Opened(is_open=is_open))
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
            self.settings = await asyncio.to_thread(DomeCurrentSettings.from_device, self.dome)
            await self.binding.publish(self.settings)
        except Exception:
            return None


@sk.declare_device
class DomeService:

    def __init__(
        self,
        device: AscomDome,
        status_frequency: float,
    ):
        self.dome = device
        self.status_frequency = status_frequency
        self.telemetry_publisher: DomeTelemetryPublisher | None = None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomDome(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        self.dome.Connected = True
        await sk.device().publish(sk.Connected(is_connected=True))

        try:
            # Initialize telemetry publisher
            self.telemetry_publisher = DomeTelemetryPublisher(
                binding=sk.device(),
                dome=self.dome,
                frequency=self.status_frequency
            )

            # Populate initial state
            self.telemetry_publisher.capabilities = DomeCapabilities.from_device(self.dome)
            self.telemetry_publisher.settings = DomeCurrentSettings.from_device(self.dome)

            # Start background telemetry publishing
            await self.telemetry_publisher.start()

        except NotConnectedException as e:
            raise RuntimeError("Not connected") from e
        except NotImplementedException as e:
            logger.warning(f"Some dome properties are not implemented: {e}")
        except Exception as e:
            logger.warning(f"Dome startup property read error: {e}")

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.dome.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def dome_connect(self, cmd: sk.Connect):
        try:
            self.dome.Connected = True
            await sk.device().publish(sk.Connected(is_connected=True))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Dome connect failed")
            raise RuntimeError("Connect failed") from e

    @sk.command_handler
    async def dome_disconnect(self, cmd: sk.Disconnect):
        try:
            self.dome.Connected = False
            await sk.device().publish(sk.Connected(is_connected=False))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Dome disconnect failed")
            raise RuntimeError("Disconnect failed") from e

    @sk.command_handler
    async def dome_open(self, cmd: sk.Open):
        try:
            if _safe_get(self.dome, "CanSetShutter", False) is False:
                raise RuntimeError("Device does not support shutter control")

            await asyncio.to_thread(self.dome.OpenShutter)
            await asyncio.sleep(0.1)

            # Wait for completion if status is readable, else return after command
            async def _complete():
                while True:
                    status = _safe_get(self.dome, "ShutterStatus", None)
                    if status is ShutterState.shutterOpen:
                        break
                    await asyncio.sleep(0.25)

            async with asyncio.timeout(300):
                await _complete()
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Open shutter failed")
            raise RuntimeError("Open failed") from e

    @sk.command_handler
    async def dome_close(self, cmd: sk.Close):
        try:
            if _safe_get(self.dome, "CanSetShutter", False) is False:
                raise RuntimeError("Device does not support shutter control")

            await asyncio.to_thread(self.dome.CloseShutter)
            await asyncio.sleep(0.1)

            async def _complete():
                while True:
                    status = _safe_get(self.dome, "ShutterStatus", None)
                    if status is ShutterState.shutterClosed:
                        break
                    await asyncio.sleep(0.25)

            async with asyncio.timeout(300):
                await _complete()
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Close shutter failed")
            raise RuntimeError("Close failed") from e
