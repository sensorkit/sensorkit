# SPDX-License-Identifier: Apache-2.0
import asyncclick as click

from sensorkit.cli.utils import entity_option, with_kit


@click.group("device")
async def device_group():
    """Device management commands."""

@device_group.command("connect")
@entity_option(required=True)
@with_kit
async def connect_device(kit, entity: str):
    """Connect to a device."""
    from sensorkit.std import Connect

    await kit.device(entity).command(Connect())

@device_group.command("disconnect")
@entity_option(required=True)
@with_kit
async def disconnect_device(kit, entity: str):
    """Disconnect from a device."""
    from sensorkit.std import Disconnect

    await kit.device(entity).command(Disconnect())

@device_group.command("abort")
@entity_option(required=True)
@with_kit
async def stop_device(kit, entity: str):
    """Abort any in-progress motion on a device."""
    from sensorkit.std import Stop

    await kit.device(entity).command(Stop())


