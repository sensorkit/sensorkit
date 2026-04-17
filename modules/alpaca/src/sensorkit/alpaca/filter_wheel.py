from __future__ import annotations

import asyncio
import contextlib
from typing import Literal, override

from alpaca.filterwheel import FilterWheel
from loguru import logger

import sensorkit.api as sk
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected
from sensorkit.std.optics import Filter, SetFilter


@sk.declare_device
class AlpacaFilterWheel(AlpacaDevice):
    """Alpaca FilterWheel implementation."""

    config: AlpacaFilterWheelConfig

    def __init__(self, config: AlpacaFilterWheelConfig):
        super().__init__(config=config)
        self._init_task: asyncio.Task | None = None

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaFilterWheelState)
        except Exception:
            self.state = AlpacaFilterWheelState()

        self._init_task = asyncio.create_task(self.filter_wheel_init(sk.Init()))
        self._init_task.add_done_callback(self._on_init_done)

    def _on_init_done(self, task: asyncio.Task):
        if task.cancelled():
            return
        if exc := task.exception():
            logger.opt(exception=exc).error("filter_wheel init failed")

    async def _wait_init(self):
        if self._init_task is not None and not self._init_task.done():
            await self._init_task

    @sk.on_detach
    async def entity_deinit(self):
        if self._init_task is not None and not self._init_task.done():
            self._init_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._init_task
        await self.filter_wheel_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def filter_wheel_init(self, cmd: sk.Init):
        self.device_name = "FilterWheel"
        self.filter_wheel = FilterWheel(
            self.address, self.config.device_number, self.config.protocol
        )
        self.filter_position: int | None = None
        self._filter_names: list[str] = []

        await self.filter_wheel_connect(sk.Connect())

        # Read capabilities
        self._filter_names = await self.get(self.filter_wheel, "Names", [])
        self._name_to_index: dict[str, int] = {
            name: i for i, name in enumerate(self._filter_names)
        }

        self.start_status_loop(self.status_publish())

        # Wait for initial position
        async with asyncio.timeout(self.config.timeout):
            while self.filter_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def filter_wheel_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.filter_wheel_disconnect(sk.Disconnect())

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: sk.Connect):
        await self.connect(self.filter_wheel, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.filter_wheel)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def filter_wheel_set(self, cmd: SetFilter):
        await self._wait_init()
        self.require_connected()

        if isinstance(cmd.filter, int):
            position = cmd.filter
        else:
            position = self._name_to_index.get(cmd.filter)
            if position is None:
                logger.error(f"Unknown filter name: {cmd.filter}")
                return

        logger.debug(f"setting position to {position}")

        await self.put(self.filter_wheel, "Position", position)

        # Wait for filter change — position reads as -1 while moving
        async with asyncio.timeout(self.config.timeout):
            while True:
                pos = await self.get(self.filter_wheel, "Position", -1)
                if pos >= 0:
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.filter_position = pos

        logger.debug(f"set position to {self.filter_position}")

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
                        self.filter_position = position
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


class AlpacaFilterWheelConfig(AlpacaDeviceConfig[AlpacaFilterWheel]):
    device_type: Literal["filter_wheel"] = "filter_wheel"
    timeout: float = 60.0
    status_frequency: float = 5.0

    @override
    def create_device(self):
        return AlpacaFilterWheel(self)


class AlpacaFilterWheelState(AlpacaDeviceState):
    device_type: Literal["filter_wheel"] = "filter_wheel"
