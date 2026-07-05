# SPDX-License-Identifier: Apache-2.0
"""Slack notification service entrypoint."""

from __future__ import annotations

import os

import sensorkit.api as sk
from dotenv import dotenv_values
from sensorkit.slack.client import SlackClient
from sensorkit.slack.models import SlackConfig
from sensorkit.slack.notifier import SlackNotifier


@sk.service_entrypoint(version=sk.VERSION)
async def slack_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SlackConfig)

    # Load bot token from .env file or environment
    env = dotenv_values(config.env_file)
    token = env.get("SLACK_BOT_TOKEN") or os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            f"SLACK_BOT_TOKEN must be set in {config.env_file} or as an environment variable"
        )

    client = SlackClient(token)
    notifier = SlackNotifier(config, client, service.client)
    service.include(notifier)

    await service.run()
