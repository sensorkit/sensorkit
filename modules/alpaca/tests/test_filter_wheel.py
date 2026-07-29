# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca filter wheel device."""

import pytest

from sensorkit.alpaca.filter_wheel import (
    AlpacaFilterWheelConfig,
    AlpacaFilterWheelState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.std import Connect, Disconnect


@pytest.fixture
def filter_wheel():
    config = AlpacaFilterWheelConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    fw = config.create_device()
    fw.state = AlpacaFilterWheelState()
    fw.device_name = "FilterWheel"
    device = FakeAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        Position=0,
        Names=["Red", "Green", "Blue", "Luminance"],
    )
    fw.filter_wheel = device
    fw.device_connected = True
    fw.filter_wheel_position = 0
    fw._filter_names = ["Red", "Green", "Blue", "Luminance"]
    fw._name_to_index = {"Red": 0, "Green": 1, "Blue": 2, "Luminance": 3}
    return fw


@pytest.mark.asyncio
async def test_filter_wheel_connect(filter_wheel):
    filter_wheel.device_connected = False
    filter_wheel.filter_wheel._properties["Connected"] = False
    await filter_wheel.filter_wheel_connect(Connect())
    assert filter_wheel.device_connected is True


@pytest.mark.asyncio
async def test_filter_wheel_disconnect(filter_wheel):
    await filter_wheel.filter_wheel_disconnect(Disconnect())
    assert filter_wheel.device_connected is False


@pytest.mark.asyncio
async def test_filter_wheel_set_by_name(filter_wheel):
    from sensorkit.std.optics import SetFilter

    filter_wheel.filter_wheel._properties["Position"] = 2
    await filter_wheel.filter_wheel_set_filter(SetFilter(filter="Blue"))
    assert filter_wheel.filter_wheel_position == 2


@pytest.mark.asyncio
async def test_filter_wheel_set_by_index(filter_wheel):
    from sensorkit.std.optics import SetFilter

    filter_wheel.filter_wheel._properties["Position"] = 3
    await filter_wheel.filter_wheel_set_filter(SetFilter(filter=3))
    assert filter_wheel.filter_wheel_position == 3


@pytest.mark.asyncio
async def test_filter_wheel_set_invalid_name(filter_wheel):
    from sensorkit.std.optics import SetFilter

    # Setting an unknown filter name should not raise, just log and return
    await filter_wheel.filter_wheel_set_filter(SetFilter(filter="Nonexistent"))
    # Position should remain unchanged
    assert filter_wheel.filter_wheel_position == 0
