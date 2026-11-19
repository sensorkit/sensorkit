# SPDX-License-Identifier: Apache-2.0
"""Declared archetype identity survives publication independently of trait matching."""

import asyncio

import pytest

import sensorkit.api as sk
from sensorkit.api.declarative import DeclarationError


class RoleCommand(sk.DeviceCommand):
    pass


def test_declared_traits_deduplicate_archetype_and_trait_requirements():
    archetype = sk.declare_archetype("deduplicated_role", required_commands=(RoleCommand,))
    trait = sk.declare_trait("deduplicated_capability", required_commands=(RoleCommand,))
    device = sk.declare_device(name="device", type=archetype, traits=[archetype, trait, trait])

    assert device.declared_traits == frozenset({archetype, trait})
    assert isinstance(device.declared_traits, frozenset)


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["decorator", "named", "class_argument"])
@pytest.mark.parametrize("identity", ["declared", "traits_only", "omitted"])
async def test_published_archetype_identity(kit, service, form, identity):
    principal = sk.declare_archetype(
        f"{form}_{identity}_principal",
        required_commands=(RoleCommand,),
        required_keywords=("PrincipalKeyword",),
    )
    additional = sk.declare_archetype(
        f"{form}_{identity}_additional", required_commands=(RoleCommand,)
    )
    archetype = principal if identity == "declared" else None
    traits = [additional, principal] if identity == "traits_only" else [additional]

    match form:
        case "decorator":

            @sk.declare_device(type=archetype, traits=traits)
            class Device:
                @sk.command_handler
                async def handle(self, cmd: RoleCommand):
                    pass

            service.include(Device(), name="device")
        case "class_argument":

            class Device:
                @sk.command_handler
                async def handle(self, cmd: RoleCommand):
                    pass

            device_cls = sk.declare_device(Device, type=archetype, traits=traits)
            service.include(device_cls(), name="device")
        case "named":
            device = sk.declare_device(name="device", type=archetype, traits=traits)

            @sk.command_handler(device)
            async def handle(cmd: RoleCommand):
                pass

            service.add(device)

    async with asyncio.timeout(5):
        await service.start()
        client = kit.device("device")
        details = await client.get_details()
        stored = await client.kv_get_model(sk.EntityInfo)

        assert details.archetype == (principal.name if archetype else None)
        assert details.get_archetype() == archetype
        assert await client.get_archetype() == archetype
        assert stored.details.archetype == details.archetype
        assert {principal, additional} <= set(details.traits)
        assert set(await client.get_traits([principal, additional])) == {principal, additional}
        assert await client.has_trait(additional)

        if identity != "omitted":
            assert "PrincipalKeyword" in details.published_keywords


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["decorator", "named"])
async def test_declared_archetype_requires_commands(service, form):
    archetype = sk.declare_archetype(
        f"required_role_command_{form}", required_commands=(RoleCommand,)
    )

    match form:
        case "decorator":

            @sk.declare_device(type=archetype)
            class Device:
                pass

            service.include(Device(), name="device")
        case "named":
            service.add(sk.declare_device(name="device", type=archetype))

    async with asyncio.timeout(5):
        with pytest.raises(DeclarationError, match="does not implement RoleCommand"):
            await service.start()


@pytest.mark.asyncio
async def test_same_name_trait_cannot_replace_principal_requirements(service):
    archetype = sk.declare_archetype("principal_validation_role", required_commands=(RoleCommand,))
    device = sk.declare_device(name="device", type=archetype, traits=[sk.Trait(archetype.name)])
    service.add(device)

    async with asyncio.timeout(5):
        with pytest.raises(DeclarationError, match="does not implement RoleCommand"):
            await service.start()


@pytest.mark.asyncio
async def test_device_instances_have_independent_declarations(kit, service):
    archetype = sk.declare_archetype("independent_device_role", required_commands=(RoleCommand,))

    @sk.declare_device(type=archetype)
    class Device:
        def __init__(self):
            self.calls = 0

        @sk.command_handler
        async def handle(self, cmd: RoleCommand):
            self.calls += 1

    first = Device()
    second = Device()
    service.include(first, name="first")
    service.include(second, name="second")

    async with asyncio.timeout(5):
        await service.start()
        first_client = kit.device("first")
        second_client = kit.device("second")

        assert await first_client.get_archetype() == archetype
        assert await second_client.get_archetype() == archetype

        await first_client.command(RoleCommand())
        assert (first.calls, second.calls) == (1, 0)

        await second_client.command(RoleCommand())
        assert (first.calls, second.calls) == (1, 1)


@pytest.mark.asyncio
async def test_bare_decorator_omits_inherited_archetype(kit, service):
    archetype = sk.declare_archetype("inherited_role", required_commands=(RoleCommand,))

    @sk.declare_device(type=archetype)
    class BaseDevice:
        @sk.command_handler
        async def handle(self, cmd: RoleCommand):
            pass

    @sk.declare_device
    class Device(BaseDevice):
        pass

    service.include(Device(), name="device")

    async with asyncio.timeout(5):
        await service.start()
        client = kit.device("device")

        assert await client.get_archetype() is None
        assert await client.has_trait(archetype)
