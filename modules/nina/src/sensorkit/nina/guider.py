"""NINA Guider device implementation.

SensorKit does not yet have a standard guider archetype, so this is a custom entity
that publishes guiding state and provides start/stop/dither commands.
"""

from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState


@sk.declare_keyword
class NinaGuiderStatus(BaseModel):
    """NINA guider status."""

    guiding: bool = False
    rms_ra: float | None = None
    rms_dec: float | None = None
    rms_total: float | None = None


class StartGuiding(sk.DeviceCommand):
    """Start autoguiding."""

    command_id: str = "StartGuiding"
    calibrate: bool = False


class StopGuiding(sk.DeviceCommand):
    """Stop autoguiding."""

    command_id: str = "StopGuiding"


class Dither(sk.DeviceCommand):
    """Dither the guide star."""

    command_id: str = "Dither"
    amount: float = 1.0
    ra_only: bool = False


@sk.declare_device
class NinaGuider(NinaDevice):
    """NINA Guider implementation."""

    config: NinaGuiderConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaGuiderState)
        except Exception:
            self.state = NinaGuiderState()

        await self.guider_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.guider_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def guider_init(self, cmd: sk.Init):
        self.device_name = "Guider"
        await self.connect("guider")
        await sk.device().publish(Connected(is_connected=True))
        self.start_status_loop(self.status_publish())

    @sk.command_handler
    async def guider_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        if not self.device_connected:
            return
        try:
            await self.client.get("/equipment/guider/stop")
        except Exception:
            pass
        await self.disconnect("guider")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def guider_connect(self, cmd: sk.Connect):
        await self.connect("guider")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def guider_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("guider")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def guider_start(self, cmd: StartGuiding):
        self.require_connected()
        logger.debug(f"starting guiding (calibrate={cmd.calibrate})")
        await self.client.get("/equipment/guider/start", calibrate=cmd.calibrate)

    @sk.command_handler
    async def guider_stop(self, cmd: StopGuiding):
        self.require_connected()
        logger.debug("stopping guiding")
        await self.client.get("/equipment/guider/stop")

    @sk.command_handler
    async def guider_dither(self, cmd: Dither):
        self.require_connected()
        logger.debug(f"dithering (amount={cmd.amount}, ra_only={cmd.ra_only})")
        await self.client.get(
            "/equipment/guider/dither",
            amount=cmd.amount, raOnly=cmd.ra_only, waitToFinish=True,
        )

    async def status_publish(self):
        while True:
            try:
                info = await self.info("guider")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    fields: dict = {
                        "guiding": info.get("Guiding", False),
                    }

                    rms_ra = info.get("RMSErrorRA")
                    rms_dec = info.get("RMSErrorDec")
                    if rms_ra is not None:
                        fields["rms_ra"] = rms_ra
                    if rms_dec is not None:
                        fields["rms_dec"] = rms_dec
                    rms_total = info.get("RMSErrorTotal")
                    if rms_total is not None:
                        fields["rms_total"] = rms_total

                    await device.publish(NinaGuiderStatus(**fields))
            except Exception as e:
                logger.exception(f"Error in guider status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaGuiderConfig(NinaDeviceConfig[NinaGuider]):
    device_type: Literal["guider"] = "guider"
    timeout: float = 60.0
    status_frequency: float = 2.0

    @override
    def create_device(self):
        return NinaGuider(self)


class NinaGuiderState(NinaDeviceState):
    device_type: Literal["guider"] = "guider"
