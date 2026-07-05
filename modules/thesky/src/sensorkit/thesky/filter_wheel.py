# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.std.optics import Filter, Filters, SetFilter
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyFilterWheel(TheSkyDevice):
    """TheSky FilterWheel implementation."""

    config: TheSkyFilterWheelConfig
    device_name = "Filter Wheel"

    # NOTE: the TheSky API allows for querying the name of a filter slot based on its index, but: 1) if you provide an
    # index beyond the actual available indices, it does not error, rather it appears to move to the first index; and
    # 2) looping through indices to retrieve the names would take an unnecessarily long time. And so we enforce that
    # the user define the names via config (e.g. {R: 0, G: 1, B: 2})

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyFilterWheelState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyFilterWheelState()

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
        await self.filter_wheel_connect(Connect())

        # Build an inverted index for name lookups
        self._filter_index = {idx: name for name, idx in self.config.filters.items()}
        assert len(self._filter_index) == len(self.config.filters)

        await sk.device().publish(
            Filters(
                filters=[
                    Filter(name=name, position=idx) for name, idx in self.config.filters.items()
                ]
            )
        )

    @sk.command_handler
    async def filter_wheel_connect(self, cmd: Connect):
        logger.debug("connecting to filter wheel")

        await self.execute(
            """
            ccdsoftCamera.Asynchronous = 1;
            ccdsoftCamera.filterWheelConnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.filterWheelIsConnected();""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to filter wheel")

    @sk.command_handler
    async def filter_wheel_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from filter wheel")

        await self.execute(
            """
            ccdsoftCamera.filterWheelDisconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.filterWheelIsConnected();""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from filter wheel")

    @sk.command_handler
    async def filter_wheel_set_filter(self, cmd: SetFilter):
        # NOTE: TheSky does not actually change the filter until a call to TakeImage() is received, so we will have to
        # account for that delay wherever it makes sense to do so.
        await self.require_connected()
        logger.debug(f"setting filter wheel position to {cmd.filter}")

        match cmd.filter:
            case int():
                index = cmd.filter
                if index not in self._filter_index:
                    logger.error(f"Filter ({cmd.filter}) unavailable")
                    raise RuntimeError(f"Filter ({cmd.filter}) unavailable")
            case str():
                index = self.config.filters.get(cmd.filter)

                if index is None:
                    logger.error(f"Filter ({cmd.filter}) unavailable")
                    raise RuntimeError(f"Filter ({cmd.filter}) unavailable")

        await self.execute(
            f"""
            ccdsoftCamera.FilterIndexZeroBased = {index};
            """
        )

        logger.debug(f"set filter wheel position to {cmd.filter}")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        ccdsoftCamera.filterWheelIsConnected(),
                        ccdsoftCamera.FilterIndexZeroBased
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                connected, position = [float(x) for x in resp.split(",")]

                connected = bool(connected)
                self.device_connected = connected

                self.filter_wheel_position = int(position)

                # logger.debug(
                #     f"TheSky filter wheel status: connected={connected}, name={self.filter_wheel_name}, position={self.filter_wheel_position}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(
                    Filter(name=self.filter_wheel_name, position=self.filter_wheel_position)
                )

            except Exception as e:
                logger.warning(f"Failed to update TheSky filter wheel status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)

    @property
    def filter_wheel_name(self):
        return self._filter_index[self.filter_wheel_position]


class TheSkyFilterWheelConfig(TheSkyDeviceConfig[TheSkyFilterWheel]):
    """TheSky FilterWheel configuration."""

    device_type: Literal["filter_wheel"] = "filter_wheel"
    filters: dict[str, int] = {}
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return TheSkyFilterWheel(self)


class TheSkyFilterWheelState(TheSkyDeviceState):
    """TheSky FilterWheel state."""

    device_type: Literal["filter_wheel"] = "filter_wheel"
