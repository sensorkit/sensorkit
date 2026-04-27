"""Slack notification service entrypoint."""

from __future__ import annotations

import sensorkit.api as sk
from sensorkit.slack.client import SlackClient
from sensorkit.slack.models import SlackConfig
from sensorkit.slack.notifier import SlackNotifier


@sk.service_entrypoint(version=sk.VERSION)
async def slack_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SlackConfig)
    client = SlackClient(config.token)
    notifier = SlackNotifier(config, client, service.client)
    service.include(notifier, name=config.entity)

    await service.run()
