from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.switch import Switch
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected


@sk.declare_keyword
class AlpacaSwitchState(BaseModel):
    """State of a single switch."""

    index: int
    name: str
    description: str | None = None
    value: float
    min_value: float
    max_value: float
    step: float
    can_write: bool
    can_async: bool = False


@sk.declare_keyword
class AlpacaSwitchStates(BaseModel):
    """Aggregate state of all switches."""

    switches: list[AlpacaSwitchState]


@sk.declare_device
class AlpacaSwitch(AlpacaDevice):
    """Alpaca Switch implementation."""

    config: AlpacaSwitchConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaSwitchDeviceState)
        except Exception:
            self.state = AlpacaSwitchDeviceState()

        self._start_init(self.switch_init(sk.Init()))

    @sk.on_detach
    async def entity_deinit(self):
        await self._cancel_init()
        await self.switch_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def switch_init(self, cmd: sk.Init):
        self.device_name = "Switch"
        self.switch = Switch(
            self.address, self.config.device_number, self.config.protocol
        )
        self._max_switch: int = 0

        await self.switch_connect(sk.Connect())

        # Read capabilities
        self._max_switch = await self.get(self.switch, "MaxSwitch", 0)

        self.start_status_loop(self.status_publish())

    @sk.command_handler
    async def switch_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.switch_disconnect(sk.Disconnect())

    @sk.command_handler
    async def switch_connect(self, cmd: sk.Connect):
        await self.connect(self.switch, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def switch_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.switch)
        await sk.device().publish(Connected(is_connected=False))

    async def _read_switch(self, index: int) -> AlpacaSwitchState | None:
        try:
            s = self.switch
            name = await asyncio.to_thread(s.GetSwitchName, index)
            description = await asyncio.to_thread(s.GetSwitchDescription, index)
            value = await asyncio.to_thread(s.GetSwitchValue, index)
            min_val = await asyncio.to_thread(s.MinSwitchValue, index)
            max_val = await asyncio.to_thread(s.MaxSwitchValue, index)
            step = await asyncio.to_thread(s.SwitchStep, index)
            can_write = await asyncio.to_thread(s.CanWrite, index)

            # V3: check async support
            try:
                can_async = await asyncio.to_thread(s.CanAsync, index)
            except Exception:
                can_async = False

            return AlpacaSwitchState(
                index=index,
                name=name,
                description=description,
                value=value,
                min_value=min_val,
                max_value=max_val,
                step=step,
                can_write=can_write,
                can_async=can_async,
            )
        except Exception as e:
            logger.debug(f"Failed to read switch {index}: {e}")
            return None

    async def status_publish(self):
        while True:
            try:
                connected = await self.get(self.switch, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected and self._max_switch > 0:
                    switches = []
                    for i in range(self._max_switch):
                        state = await self._read_switch(i)
                        if state is not None:
                            switches.append(state)

                    switches_str = "; ".join(f"{s.name}={s.value}" for s in switches)
                    logger.debug(
                        f"Alpaca switch status: connected={connected}, [{switches_str}]"
                    )

                    if switches:
                        await device.publish(AlpacaSwitchStates(switches=switches))
            except Exception as e:
                logger.exception(f"Error in switch status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AlpacaSwitchConfig(AlpacaDeviceConfig[AlpacaSwitch]):
    device_type: Literal["switch"] = "switch"
    timeout: float = 30.0
    status_frequency: float = 5.0

    @override
    def create_device(self):
        return AlpacaSwitch(self)


class AlpacaSwitchDeviceState(AlpacaDeviceState):
    device_type: Literal["switch"] = "switch"
