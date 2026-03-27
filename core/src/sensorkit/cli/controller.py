import datetime as dt
import json
import uuid

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
        InitTask(
            task_id=uuid.uuid1(),
            controller_id=entity,
            end_time=dt.datetime.now(dt.UTC)+dt.timedelta(minutes=10)),
        interrupt=force
    )


@controller_group.command("shutdown")
@entity_option(required=True)
@click.option("-f", "--force", is_flag=True, default=False)
@with_kit
async def shutdown(kit, entity: str, force: bool):
    """Shutdown a controller."""
    from sensorkit.core.task import ShutdownTask

    await kit.controller(entity).execute_task(
        ShutdownTask(
            task_id=uuid.uuid1(),
            controller_id=entity,
            end_time=dt.datetime.now(dt.UTC)+dt.timedelta(minutes=10)
        ), 
        interrupt=force
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
        task_id=uuid.uuid1(),
        controller_id=entity,
        end_time=dt.datetime.now(dt.UTC)+dt.timedelta(minutes=10),
        target=data,
        camera_params=CameraParameterSet(
            integration_time_seconds=integration_time_seconds,
            frame_count=frame_count,
            binning_x=binning,
            binning_y=binning,
        )
    )

    await ctl.execute_task(collect_task)
