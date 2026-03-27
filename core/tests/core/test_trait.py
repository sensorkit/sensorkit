"""Tests for the device trait infrastructure."""

from __future__ import annotations

import asyncio

import pytest

from sensorkit.core.entity import DeviceDetails
from sensorkit.core.trait import (
    Archetype,
    Trait,
    _archetype_registry,
    _trait_registry,
    declare_archetype,
    declare_trait,
    get_registered_archetypes,
    get_registered_traits,
    match_archetype,
    match_traits,
)
from sensorkit.models.devices import (
    Connect,
    Disconnect,
    FollowTarget,
    Init,
    Stop,
)


@pytest.fixture(autouse=True)
def _clean_trait_registries():
    """Snapshot and restore the global trait registries around each test."""
    saved_traits = _trait_registry.copy()
    saved_archetypes = _archetype_registry.copy()
    yield
    _trait_registry.clear()
    _trait_registry.update(saved_traits)
    _archetype_registry.clear()
    _archetype_registry.update(saved_archetypes)


class TestTraitMatching:
    def test_simple_trait_matches(self):
        t = declare_trait("t_simple_match", required_commands=(Stop,))
        details = DeviceDetails(supported_commands={"Stop", "Init"})
        assert t.match(details)

    def test_simple_trait_no_match(self):
        t = declare_trait("t_simple_no_match", required_commands=(Stop,))
        details = DeviceDetails(supported_commands={"Init"})
        assert not t.match(details)

    def test_empty_trait_matches_anything(self):
        t = declare_trait("t_empty")
        details = DeviceDetails(supported_commands={"Init"})
        assert t.match(details)

    def test_multiple_commands_required(self):
        t = declare_trait("t_multi_cmd", required_commands=(Connect, Disconnect))
        details_ok = DeviceDetails(supported_commands={"Connect", "Disconnect", "Init"})
        details_partial = DeviceDetails(supported_commands={"Connect"})
        assert t.match(details_ok)
        assert not t.match(details_partial)


class TestArchetypeMatching:
    def test_archetype_with_sub_traits(self):
        can_stop = declare_trait("t_can_stop_a", required_commands=(Stop,))
        arch = declare_archetype(
            "t_arch_a",
            required_commands=(Init, FollowTarget),
            required_traits=(can_stop,),
        )
        details = DeviceDetails(supported_commands={"Init", "FollowTarget", "Stop"})
        assert arch.match(details)

    def test_archetype_missing_sub_trait_command(self):
        can_stop = declare_trait("t_can_stop_b", required_commands=(Stop,))
        arch = declare_archetype(
            "t_arch_b",
            required_commands=(Init, FollowTarget),
            required_traits=(can_stop,),
        )
        details = DeviceDetails(supported_commands={"Init", "FollowTarget"})
        assert not arch.match(details)

    def test_optional_commands_do_not_affect_matching(self):
        arch = declare_archetype(
            "t_arch_opt_cmds",
            required_commands=(Init,),
            optional_commands=(Stop,),
        )
        details = DeviceDetails(supported_commands={"Init"})
        assert arch.match(details)


class TestEffectiveCommandIds:
    def test_flattens_sub_traits(self):
        can_stop = declare_trait("t_cs_flat", required_commands=(Stop,))
        arch = declare_archetype(
            "t_arch_flat",
            required_commands=(Init,),
            required_traits=(can_stop,),
        )
        assert arch.effective_command_ids() == frozenset({"Init", "Stop"})

    def test_simple_trait_ids(self):
        t = declare_trait("t_ids_simple", required_commands=(Init, Stop))
        assert t.effective_command_ids() == frozenset({"Init", "Stop"})


class TestDeclareArchetype:
    def test_archetype_sub_trait_cannot_be_archetype(self):
        arch_sub = declare_archetype("t_arch_sub_v", required_commands=(Stop,))
        with pytest.raises(ValueError, match="cannot be used as a sub-trait"):
            declare_archetype(
                "t_bad_v3",
                required_commands=(Init,),
                required_traits=(arch_sub,),
            )


class TestRegistry:
    def test_trait_registered(self):
        t = declare_trait("t_reg_1", required_commands=(Stop,))
        assert t in get_registered_traits()

    def test_archetype_registered_in_both(self):
        arch = declare_archetype("t_reg_2", required_commands=(Init,))
        assert arch in get_registered_traits()
        assert arch in get_registered_archetypes()

    def test_non_archetype_not_in_archetype_registry(self):
        t = declare_trait("t_reg_3", required_commands=(Stop,))
        assert t not in get_registered_archetypes()


class TestMatchHelpers:
    def test_match_traits(self):
        t1 = declare_trait("t_mh_1", required_commands=(Stop,))
        t2 = declare_trait("t_mh_2", required_commands=(Init,))
        t3 = declare_trait("t_mh_3", required_commands=(FollowTarget,))
        details = DeviceDetails(supported_commands={"Stop", "Init"})
        matched = match_traits(details, [t1, t2, t3])
        assert t1 in matched
        assert t2 in matched
        assert t3 not in matched

    def test_match_archetype(self):
        can_stop = declare_trait("t_mh_cs", required_commands=(Stop,))
        arch = declare_archetype(
            "t_mh_arch",
            required_commands=(Init, FollowTarget),
            required_traits=(can_stop,),
        )
        details = DeviceDetails(supported_commands={"Init", "FollowTarget", "Stop"})
        assert match_archetype(details) == arch

    def test_match_archetype_no_match(self):
        # With no matching archetype for this device, should return None.
        details = DeviceDetails(supported_commands={"SomeUnknownCommand"})
        assert match_archetype(details) is None


class TestTraitIdentity:
    def test_traits_equal_by_name(self):
        t1 = Trait(name="X", required_commands=())
        t2 = Trait(name="X", required_commands=(Stop,))
        assert t1 == t2
        assert hash(t1) == hash(t2)

    def test_traits_not_equal_different_names(self):
        t1 = Trait(name="A", required_commands=())
        t2 = Trait(name="B", required_commands=())
        assert t1 != t2

    def test_repr(self):
        t = Trait(name="Foo", required_commands=())
        assert repr(t) == "Trait('Foo')"
        a = Archetype(name="Bar", required_commands=())
        assert repr(a) == "Archetype('Bar')"

    def test_archetype_is_subclass_of_trait(self):
        a = Archetype(name="Baz", required_commands=())
        assert isinstance(a, Trait)


@pytest.mark.asyncio
async def test_device_has_trait(kit):
    """Test that DeviceClient.has_trait queries work against a running device."""
    async with asyncio.timeout(2.0):
        sc = await kit.register_service("trait_test_svc", "0.1.0")
        dev = await sc.register_device("trait_test_device")

    @dev.command_handler(Stop)
    async def handle_stop(cmd: Stop):
        pass

    @dev.command_handler(Init)
    async def handle_init(cmd: Init):
        pass

    await dev.publish_entity_info()

    can_stop = declare_trait("t_int_can_stop", required_commands=(Stop,))
    can_init = declare_trait("t_int_can_init", required_commands=(Init,))
    needs_follow = declare_trait("t_int_follow", required_commands=(FollowTarget,))

    cli = kit.device("trait_test_device")

    async with asyncio.timeout(2.0):
        assert await cli.has_trait(can_stop)
        assert await cli.has_trait(can_init)
        assert not await cli.has_trait(needs_follow)
