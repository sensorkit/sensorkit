# SPDX-License-Identifier: Apache-2.0
import datetime as dt
import json

import asyncclick as click

from sensorkit.cli.utils import entity_option, with_kit


@click.group("controller")
async def controller_group():
    """Controller management commands."""

@controller_group.command("abort")
@entity_option(required=True)
@with_kit
async def abort(kit, entity: str):
    """Abort the currently running task on a controller."""
    await kit.controller(entity).abort_task()


@controller_group.command("init")
@entity_option(required=True)
@click.option("-f", "--force", is_flag=True, default=False)
@with_kit
async def startup(kit, entity: str, force: bool):
    """Initialize a controller."""
    from sensorkit.core.task import InitTask

    await kit.controller(entity).execute_task(
        InitTask(),
        expiry_time=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=10),
        interrupt=force,
    )


@controller_group.command("shutdown")
@entity_option(required=True)
@click.option("-f", "--force", is_flag=True, default=False)
@with_kit
async def shutdown(kit, entity: str, force: bool):
    """Shutdown a controller."""
    from sensorkit.core.task import ShutdownTask

    await kit.controller(entity).execute_task(
        ShutdownTask(),
        expiry_time=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=10),
        interrupt=force,
    )

@controller_group.command("collect")
@entity_option(required=True)
@click.option("-t", "--target", required=True)
@click.option("-b", "--binning", type=int, default=1)
@click.option("-c", "--frame-count", type=int, default=1)
@click.option("-i", "--integration-time-seconds", type=float, default=1.0)
@with_kit
async def collect(
    kit,
    entity: str,
    target: str,
    binning: int,
    frame_count: int,
    integration_time_seconds: float,
):
    """Execute a StandardCollectTask on a controller with the given target and camera parameters."""
    from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

    ctl = kit.controller(entity)

    data = json.loads(target)
    collect_task = StandardCollectTask(
        target=data,
        camera_params=CameraParameterSet(
            integration_time_seconds=integration_time_seconds,
            frame_count=frame_count,
            binning_x=binning,
            binning_y=binning,
        )
    )

    await ctl.execute_task(
        collect_task,
        expiry_time=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=10),
    )


@controller_group.command("autofocus")
@entity_option(required=True)
@click.option("-n", "--instrument", required=True, help="Controller instrument name.")
@click.option("-s", "--step", type=float, required=True, help="Focuser travel between positions.")
@click.option("-c", "--count", type=int, default=7, show_default=True, help="Number of positions.")
@click.option("--center", type=float, default=None, help="Sweep center. Default: current position.")
@click.option("-i", "--integration-time-seconds", type=float, default=1.0)
@click.option("-b", "--binning", type=int, default=None)
@click.option("--filter", "filter_name", default=None)
@click.option("-x", "--context", default=None, help="JSON object of task context keywords.")
@with_kit
async def autofocus(
    kit,
    entity: str,
    instrument: str,
    step: float,
    count: int,
    center: float | None,
    integration_time_seconds: float,
    binning: int | None,
    filter_name: str | None,
    context: str | None,
):
    """Execute an AutoFocus sweep on a controller instrument."""
    from sensorkit.common.keyword import KeywordDict, validated_items
    from sensorkit.std.focus import AutoFocus
    from sensorkit.std.instrument import Binning

    task = AutoFocus(
        instrument=instrument,
        step=step,
        count=count,
        center=center,
        integration_time_seconds=integration_time_seconds,
        binning=None if binning is None else Binning(x=binning, y=binning),
        filter_name=filter_name,
    )

    await kit.controller(entity).execute_task(
        task,
        context=KeywordDict(validated_items(json.loads(context))) if context else None,
        expiry_time=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=10),
    )
