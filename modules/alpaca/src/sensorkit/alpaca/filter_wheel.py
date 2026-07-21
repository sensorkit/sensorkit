# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from alpaca.filterwheel import FilterWheel
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.std.optics import Filter, Filters, SetFilter


class AlpacaFilterWheelState(AlpacaDeviceState):
    device_type: Literal["filter_wheel"] = "filter_wheel"


@sk.declare_device
class AlpacaFilterWheel(AlpacaDevice):
    """Alpaca FilterWheel implementation."""

    config: AlpacaFilterWheelConfig
    device_name = "FilterWheel"
    state_model = AlpacaFilterWheelState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        self.filter_wheel_position: float | None = None

        # Initialize the filter wheel
        await self._initialize()
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.filter_wheel_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.filter_wheel_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.filter_wheel_connect(Connect())
        self.filter_wheel = FilterWheel(
            self.address, self.config.device_number, self.config.protocol
        )
        await self.filter_wheel_connect(Connect())

        self._filter_names: list[str] = []

        # Read capabilities
        self._filter_names = await self.get(self.filter_wheel, "Names", [])
        self._name_to_index: dict[str, int] = {
            name: i for i, name in enumerate(self._filter_names)
        }

        await sk.device().publish(
            Filters(
                filters=[
                    Filter(name=name, position=i) for i, name in enumerate(self._filter_names)
                ]
            )
        )

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: Connect):
        await self.connect(self.filter_wheel, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.filter_wheel)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_set_filter(self, cmd: SetFilter):
        await self.require_connected()
        logger.debug(f"setting filter wheel position to {cmd.filter}")

        if isinstance(cmd.filter, int):
            position = cmd.filter
        else:
            position = self._name_to_index.get(cmd.filter)
            if position is None:
                logger.error(f"Unknown filter name: {cmd.filter}")
                return

        await self.put(self.filter_wheel, "Position", position)

        async with asyncio.timeout(self.config.timeout):
            while True:
                pos = await self.get(self.filter_wheel, "Position", -1)
                if pos == position:
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.filter_wheel_position = pos

        logger.debug(f"set filter wheel position to {self.filter_wheel_position}")

    async def status_publish(self):
        while True:
            try:
                fw = self.filter_wheel
                connected = await self.get(fw, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    position = await self.get(fw, "Position", -1)
                    if position >= 0:
                        self.filter_wheel_position = position
                        name = (
                            self._filter_names[position]
                            if position < len(self._filter_names)
                            else str(position)
                        )

                        # logger.debug(
                        #     f"Alpaca filter wheel status: connected={connected}, position={position}"
                        # )

                        await device.publish(Filter(name=name, position=position))
            except Exception as e:
                logger.exception(f"Error in filter wheel status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class AlpacaFilterWheelConfig(AlpacaDeviceConfig[AlpacaFilterWheel]):
    device_type: Literal["filter_wheel"] = "filter_wheel"
    status_frequency: float = 5.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AlpacaFilterWheel(self)
