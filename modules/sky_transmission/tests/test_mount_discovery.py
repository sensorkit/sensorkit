# SPDX-License-Identifier: Apache-2.0
"""Discover controller pointing sources from entity metadata."""

import pytest
from allclear.instrument import InstrumentModel

import sensorkit.api as sk
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.sky_transmission.analyzer import SkyTransmissionAnalyzer
from sensorkit.sky_transmission.models import SkyTransmissionConfig
from sensorkit.std.mount import FollowTarget


@pytest.fixture
def analyzer(kit, tmp_path):
    model_path = tmp_path / "instrument.json"
    InstrumentModel(image_width=100, image_height=100).save(model_path)
    config = SkyTransmissionConfig.model_validate(
        {
            "controllers": ["sensor"],
            "acquisition": {"watch_path": str(tmp_path)},
            "allclear": {"instrument_model_path": str(model_path)},
            "output": {"directory": str(tmp_path)},
            "server": {},
        }
    )
    analyzer = SkyTransmissionAnalyzer(config)
    analyzer._sk = kit
    return analyzer


async def publish_device(service_context, name: str, *keywords: str) -> DeviceImpl:
    device = DeviceImpl.for_service_context(service_context, name)

    for keyword in keywords:
        device.declare_published_keyword(keyword)

    await service_context.register_impl(device)
    return device


async def publish_controller(service_context, name: str, *devices: str):
    controller = ControllerImpl.for_service_context(service_context, name)

    for device in devices:
        controller.use_device(device)

    await service_context.register_impl(controller)


@pytest.mark.asyncio
async def test_discovery_uses_pointing_publication_and_controller_membership(
    service_context, analyzer
):
    await publish_device(service_context, "mount-a", "AltAzPointing")
    await publish_device(service_context, "mount-b", "AltAzPointing")
    await publish_device(service_context, "unrelated", "AltAzPointing")
    await publish_device(service_context, "camera", "CameraTelemetry")
    command_only = await publish_device(service_context, "command-only")

    @command_only.command_handler(FollowTarget)
    async def follow(command):
        pass

    await command_only.publish_entity_info()
    await publish_controller(
        service_context, "sensor", "mount-a", "mount-b", "camera", "command-only"
    )
    await publish_controller(service_context, "other", "mount-a")
    analyzer.config.controllers.append("other")

    assert await analyzer._discover_mounts() == {"mount-a", "mount-b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "absent", "wrong-type"])
async def test_unreadable_controller_does_not_hide_other_pointing_sources(
    kit, service_context, analyzer, failure
):
    await publish_device(service_context, "mount", "AltAzPointing")
    await publish_controller(service_context, "sensor", "mount")
    analyzer.config.controllers.insert(0, "unreadable")

    match failure:
        case "absent":
            await kit.controller("unreadable").kv_put_model(
                sk.EntityInfo(entity_type="generic", details=None)
            )
        case "wrong-type":
            await publish_device(service_context, "unreadable", "AltAzPointing")

    assert await analyzer._discover_mounts() == {"mount"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "absent", "wrong-type"])
async def test_unreadable_device_does_not_hide_other_pointing_sources(
    kit, service_context, analyzer, failure
):
    await publish_device(service_context, "mount", "AltAzPointing")
    await publish_controller(service_context, "sensor", "unreadable", "mount")

    match failure:
        case "absent":
            await kit.device("unreadable").kv_put_model(
                sk.EntityInfo(entity_type="generic", details=None)
            )
        case "wrong-type":
            await publish_controller(service_context, "unreadable")

    assert await analyzer._discover_mounts() == {"mount"}


@pytest.mark.asyncio
async def test_controller_without_devices_has_no_pointing_sources(service_context, analyzer):
    await publish_controller(service_context, "sensor")

    assert await analyzer._discover_mounts() == set()
