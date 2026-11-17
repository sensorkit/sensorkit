# SPDX-License-Identifier: Apache-2.0
"""Read current entity details through general and specialized clients."""

import pytest

from sensorkit.backend.base import Entity, KeyNotFound
from sensorkit.core.controller import ControllerClient
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import (
    ControllerDetails,
    DeviceDetails,
    EntityClient,
    EntityDetails,
    EntityInfo,
)
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.program import ProgramClient
from sensorkit.core.trait import Trait


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type, impl_type, details_type",
    [
        (EntityClient, EntityImpl, EntityDetails),
        (DeviceClient, DeviceImpl, DeviceDetails),
        (ControllerClient, ControllerImpl, ControllerDetails),
        (ProgramClient, ProgramImpl, EntityDetails),
    ],
)
async def test_details_follow_republished_metadata(
    kit, service_context, client_type, impl_type, details_type
):
    impl = impl_type.for_service_context(service_context, "provider")
    await service_context.register_impl(impl)
    client = client_type(kit, Entity.at("provider"))
    before = await client.get_details()

    assert type(before) is details_type
    assert before.published_keywords == frozenset()

    impl.declare_published_keyword("ResultA")
    await impl.publish_entity_info()
    after = await client.get_details()

    assert type(after) is details_type
    assert after.published_keywords == {"ResultA"}
    assert before.published_keywords == frozenset()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type", [EntityClient, DeviceClient, ControllerClient, ProgramClient]
)
async def test_missing_metadata_raises(kit, client_type):
    client = client_type(kit, Entity.at("missing"))

    with pytest.raises(KeyNotFound):
        await client.get_details()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type, details",
    [
        (DeviceClient, EntityDetails()),
        (DeviceClient, ControllerDetails(supported_tasks=[], controlled_devices=[])),
        (DeviceClient, None),
        (ControllerClient, EntityDetails()),
        (ControllerClient, DeviceDetails(supported_commands=set())),
        (ControllerClient, None),
    ],
)
async def test_specialized_clients_reject_other_details(kit, client_type, details):
    client = client_type(kit, Entity.at("mismatch"))
    await client.kv_put_model(EntityInfo(entity_type="generic", details=details))

    with pytest.raises(ValueError, match="mismatch"):
        await client.get_details()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type", [EntityClient, DeviceClient, ControllerClient, ProgramClient]
)
async def test_absent_details_raise(kit, client_type):
    client = client_type(kit, Entity.at("historical"))
    await client.kv_put_model(EntityInfo(entity_type="program", details=None))

    with pytest.raises(ValueError, match="Entity 'historical' has no advertised details"):
        await client.get_details()


@pytest.mark.asyncio
async def test_device_trait_matching_reads_republished_keywords(kit, service_context):
    impl = DeviceImpl.for_service_context(service_context, "mount")
    await service_context.register_impl(impl)
    client = kit.device("mount")
    pointing = Trait("pointing", required_keywords=("AltAzPointing",))

    assert not await client.has_trait(pointing)

    impl.declare_published_keyword("AltAzPointing")
    await impl.publish_entity_info()

    assert await client.has_trait(pointing)
