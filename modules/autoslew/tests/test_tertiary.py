# SPDX-License-Identifier: Apache-2.0
"""Autoslew tertiary — publishes the current Nasmyth port read via the backbone."""

import pytest

from sensorkit.autoslew.tertiary import (
    AutoslewTertiaryConfig,
    AutoslewTertiaryState,
    AutoslewTertiaryStatus,
)

from .fakes import FakeAutoslewSDKDevice


@pytest.mark.asyncio
async def test_tertiary_publishes_current_port(recorder):
    published = await recorder()
    config = AutoslewTertiaryConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewTertiaryState()
    d.telescope = FakeAutoslewSDKDevice(
        Connected=True, action_returns={"getcurrentnasmythport": "2"}
    )
    d.device_connected = True
    d._port = None

    await d._publish_status()

    assert d._port == 2
    assert (await published.wait_for(AutoslewTertiaryStatus)).port == 2
