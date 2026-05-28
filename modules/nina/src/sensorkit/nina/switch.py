from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Deinit, Init
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState


@sk.declare_keyword
class NinaSwitchState(BaseModel):
    id: str
    name: str
    value: float


@sk.declare_keyword
class NinaSwitchStatus(BaseModel):
    switches: list[NinaSwitchState]


@sk.declare_device
class NinaSwitch(NinaDevice):
    """NINA Switch implementation."""

    config: NinaSwitchConfig
    device_name = "Switch"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaSwitchDeviceState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaSwitchDeviceState()

        # Initialize the switch
        await self.switch_init(Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.switch_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def switch_init(self, cmd: Init):
        self._reconnect = lambda: self.switch_connect(Connect())
        await self.switch_connect(Connect())

    @sk.command_handler
    async def switch_deinit(self, cmd: Deinit):
        pass

    @sk.command_handler
    async def switch_connect(self, cmd: Connect):
        await self.connect("switch")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def switch_disconnect(self, cmd: Disconnect):
        await self.disconnect("switch")
        await sk.device().publish(Connected(is_connected=False))

    async def status_publish(self):
        while True:
            try:
                info = await self.info("switch")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    raw_switches = info.get("WritableSwitches", []) + info.get(
                        "ReadonlySwitches", []
                    )
                    switches = []
                    for s in raw_switches:
                        switches.append(
                            NinaSwitchState(
                                id=str(s.get("Id", "")),
                                name=s.get("Name", ""),
                                value=s.get("Value", 0),
                            )
                        )

                    if switches:
                        await device.publish(NinaSwitchStatus(switches=switches))

                    switches_str = "; ".join(f"{s.name}={s.value}" for s in switches)
                    # logger.debug(f"NINA switch status: connected={connected}, {switches_str}")
            except Exception as e:
                logger.exception(f"Error in switch status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaSwitchConfig(NinaDeviceConfig[NinaSwitch]):
    device_type: Literal["switch"] = "switch"
    status_frequency: float = 5.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return NinaSwitch(self)


class NinaSwitchDeviceState(NinaDeviceState):
    device_type: Literal["switch"] = "switch"
