# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std import Connect, Connected, Disconnect, Filter, Filters, SetFilter


@sk.declare_device
class NinaFilterWheel(NinaDevice):
    """NINA FilterWheel implementation."""

    config: NinaFilterWheelConfig
    device_name = "FilterWheel"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaFilterWheelState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaFilterWheelState()

        self.filter_wheel_position: int | None = None

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
        await self.filter_wheel_connect(Connect())

        self._filter_names: list[str] = []

        # Read filter names from NINA
        info = await self.info("filterwheel")
        available = info.get("Filters", [])
        self._filter_names = [f.get("Name", str(i)) for i, f in enumerate(available)]
        self._name_to_id: dict[str, int] = {
            f.get("Name", ""): f.get("Id", i) for i, f in enumerate(available)
        }

        await sk.device().publish(
            Filters(
                filters=[
                    Filter(name=f.get("Name", str(i)), position=f.get("Id", i)) for i, f in enumerate(available)
                ]
            )
        )

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: Connect):
        await self.connect("filterwheel")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: Disconnect):
        await self.disconnect("filterwheel")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_set_filter(self, cmd: SetFilter):
        await self.require_connected()

        if isinstance(cmd.filter, int):
            filter_id = cmd.filter
        else:
            filter_id = self._name_to_id.get(cmd.filter)
            if filter_id is None:
                logger.error(f"Unknown filter name: {cmd.filter}")
                return

        logger.debug(f"changing filter to {filter_id}")
        await self.client.get("/equipment/filterwheel/change-filter", filterId=filter_id)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("filterwheel")
                current = info.get("SelectedFilter", {})
                if current.get("Id") == filter_id:
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.filter_wheel_position = filter_id
        logger.debug(f"changed filter to {filter_id}")

    async def status_publish(self):
        while True:
            try:
                info = await self.info("filterwheel")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    selected_filter = info.get("SelectedFilter", {})
                    position = selected_filter.get("Id")
                    name = selected_filter.get("Name", "")
                    if position is not None:
                        self.filter_wheel_position = position
                        await device.publish(Filter(name=name, position=position))

                    # logger.debug(
                    #     f"NINA filter wheel status: connected={connected}, "
                    #     f"position={position}, name={name}"
                    # )
            except Exception as e:
                logger.exception(f"Error in filter wheel status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaFilterWheelConfig(NinaDeviceConfig[NinaFilterWheel]):
    device_type: Literal["filter_wheel"] = "filter_wheel"
    status_frequency: float = 5.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NinaFilterWheel(self)


class NinaFilterWheelState(NinaDeviceState):
    device_type: Literal["filter_wheel"] = "filter_wheel"
