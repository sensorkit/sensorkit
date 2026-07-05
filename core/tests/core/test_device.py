# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal

import pytest
from pydantic import BaseModel

from sensorkit.backend.request import CallError
from sensorkit.core.device import Abort, DeviceCommand, DeviceState
from sensorkit.core.impl.device import DeviceImpl


@pytest.mark.asyncio
async def test_device_command(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        dev = await sc.register_device("mydevice")

    done = asyncio.Event()

    class MyCommand(DeviceCommand):
        command_id: Literal["MyCommand"] = "MyCommand"
        my_key: str

    @dev.command_handler(MyCommand)
    async def my_handler(cmd: MyCommand):
        assert DeviceImpl.current.get() is dev
        assert cmd.my_key == "value"
        done.set()

    async with asyncio.timeout(1.0):
        await kit.device("mydevice").command(MyCommand(my_key="value"))

    done.clear()

    class MyData(BaseModel):
        val: int

    @dev.command_handler(MyCommand)
    async def my_responder(cmd: MyCommand):
        assert cmd.my_key == "value2"
        done.set()
        return MyData(val=42)

    async with asyncio.timeout(1.0):
        result = await kit.device("mydevice").command(MyCommand(my_key="value2"))
        data = MyData.model_validate(result.data)
        assert data.val == 42


@pytest.mark.asyncio
async def test_device_enable_disable(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        dev = await sc.register_device("mydevice")

    disabled = asyncio.Event()
    enabled = asyncio.Event()

    @dev.on_disable
    async def on_disable():
        disabled.set()

    @dev.on_enable
    async def on_enable():
        enabled.set()

    @dev.command_handler(Abort)
    async def abort_handler(cmd: Abort):
        return 42

    cli = kit.device("mydevice")

    async with asyncio.timeout(1.0):
        for i in range(2):
            state = await cli.kv_get_model(DeviceState)
            assert state.enable_state.enabled

            assert (await cli.command(Abort())).data == 42

            if i == 1:
                break

            await cli.disable()
            await disabled.wait()

            state = await cli.kv_get_model(DeviceState)
            assert not state.enable_state.enabled

            with pytest.raises(CallError):
                await cli.command(Abort())

            await cli.enable()
            await enabled.wait()
