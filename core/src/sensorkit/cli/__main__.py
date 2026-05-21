from typing import cast

import asyncclick as click

from sensorkit.cli.agent import agent_group
from sensorkit.cli.config import config_group
from sensorkit.cli.controller import controller_group
from sensorkit.cli.device import device_group
from sensorkit.cli.go import go_command
from sensorkit.cli.kv import kv_group
from sensorkit.cli.service import service_group


@click.group()
async def cli(): ...


cli.add_command(cast(click.Command, config_group))
cli.add_command(cast(click.Command, kv_group))
cli.add_command(cast(click.Command, service_group))
cli.add_command(cast(click.Command, go_command))
cli.add_command(cast(click.Command, controller_group))
cli.add_command(cast(click.Command, device_group))
cli.add_command(cast(click.Command, agent_group))


def root():
    cli(_anyio_backend="asyncio")


if __name__ == "__main__":
    root()
