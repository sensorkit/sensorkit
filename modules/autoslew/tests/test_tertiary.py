# SPDX-License-Identifier: Apache-2.0
"""Autoslew tertiary — publishes the current Nasmyth port read via the backbone."""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.tertiary import AutoslewTertiaryConfig, AutoslewTertiaryState


@pytest.mark.asyncio
async def test_tertiary_publishes_current_port(_mock_sk_device):
    config = AutoslewTertiaryConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewTertiaryState()
    d.telescope = MockAutoslewSDKDevice(
        Connected=True, action_returns={"getcurrentnasmythport": "2"}
    )
    d.device_connected = True
    d._port = None

    await d._publish_status()

    assert d._port == 2
    statuses = [
        c.args[0]
        for c in _mock_sk_device.publish.call_args_list
        if type(c.args[0]).__name__ == "AutoslewTertiaryStatus"
    ]
    assert statuses and statuses[-1].port == 2
