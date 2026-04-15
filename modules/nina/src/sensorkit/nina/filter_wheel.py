from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std.optics import Filter, SetFilter


@sk.declare_device
class NinaFilterWheel(NinaDevice):
    """NINA FilterWheel implementation."""

    config: NinaFilterWheelConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaFilterWheelState)
        except Exception:
            self.state = NinaFilterWheelState()

        await self.filter_wheel_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.filter_wheel_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def filter_wheel_init(self, cmd: sk.Init):
        self.device_name = "FilterWheel"
        self.filter_position: int | None = None
        self._filter_names: list[str] = []

        await self.connect("filterwheel")
        await sk.device().publish(Connected(is_connected=True))

        # Read filter names from NINA
        info = await self.info("filterwheel")
        # Build filter name list from available filters
        available = info.get("Filters", [])
        self._filter_names = [f.get("Name", str(i)) for i, f in enumerate(available)]
        self._name_to_id: dict[str, int] = {
            f.get("Name", ""): f.get("Id", i) for i, f in enumerate(available)
        }

        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.filter_position is None:
                await asyncio.sleep(0.5)

    @sk.command_handler
    async def filter_wheel_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.disconnect("filterwheel")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: sk.Connect):
        await self.connect("filterwheel")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("filterwheel")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_set(self, cmd: SetFilter):
        self.require_connected()

        if isinstance(cmd.filter, int):
            filter_id = cmd.filter
        else:
            filter_id = self._name_to_id.get(cmd.filter)
            if filter_id is None:
                logger.error(f"Unknown filter name: {cmd.filter}")
                return

        logger.debug(f"changing filter to {filter_id}")
        await self.client.get("/equipment/filterwheel/change-filter", filterId=filter_id)

        # Wait for filter change to complete
        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("filterwheel")
                current = info.get("SelectedFilter", {})
                if current.get("Id") == filter_id:
                    break
                await asyncio.sleep(0.5)

        self.filter_position = filter_id
        logger.debug(f"set filter to {self.filter_position}")

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
                        self.filter_position = position

                        # logger.debug(
                        #     f"NINA filter wheel status: connected={connected}, "
                        #     f"selected_filter={selected_filter}, "
                        #     f"position={position}, name={name}"
                        # )

                        await device.publish(Filter(name=name, position=position))
            except Exception as e:
                logger.exception(f"Error in filter wheel status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaFilterWheelConfig(NinaDeviceConfig[NinaFilterWheel]):
    device_type: Literal["filter_wheel"] = "filter_wheel"
    timeout: float = 60.0
    status_frequency: float = 5.0

    @override
    def create_device(self):
        return NinaFilterWheel(self)


class NinaFilterWheelState(NinaDeviceState):
    device_type: Literal["filter_wheel"] = "filter_wheel"
