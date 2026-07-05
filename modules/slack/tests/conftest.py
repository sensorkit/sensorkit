# SPDX-License-Identifier: Apache-2.0
"""Shared test fixtures for the Slack module."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sensorkit.slack.models import (
    ChannelConfig,
    NotificationRule,
    SeverityLevel,
    SlackConfig,
    StateWatch,
)
from sensorkit.common.condition import BecomesCondition, ChangesCondition


@pytest.fixture(autouse=True)
def _mock_sk_entity(monkeypatch):
    """Mock sk.entity() globally for all tests."""

    mock_entity = MagicMock()
    mock_entity.publish = AsyncMock()

    import sensorkit.slack.notifier as notifier_mod

    monkeypatch.setattr(notifier_mod, "sk", MagicMock())
    notifier_mod.sk.entity.return_value = mock_entity
    notifier_mod.sk.declare_entity = lambda cls: cls
    notifier_mod.sk.declare_keyword = lambda cls: cls
    notifier_mod.sk.on_attach = lambda fn: fn
    notifier_mod.sk.on_detach = lambda fn: fn


@pytest.fixture
def sample_config() -> SlackConfig:
    """A sample SlackConfig for testing."""

    return SlackConfig(
        entity="slack_notifier",
        channels={
            "alerts": ChannelConfig(
                channel="#alerts",
                severity=[SeverityLevel.CRITICAL, SeverityLevel.WARNING],
            ),
            "log": ChannelConfig(
                channel="#log",
                severity=[SeverityLevel.INFO],
            ),
            "summary": ChannelConfig(
                channel="#summary",
                post_at="06:00",
                timezone="UTC",
            ),
        },
        rules=[
            NotificationRule(
                name="device_disconnect",
                severity=SeverityLevel.CRITICAL,
                state_watches=[
                    StateWatch(
                        keyword="Connected",
                        field="is_connected",
                        condition=BecomesCondition(threshold=False),
                    ),
                ],
                deduplicate=300,
            ),
            NotificationRule(
                name="observation_complete",
                severity=SeverityLevel.INFO,
                events=["TaskExecutionState"],
            ),
            NotificationRule(
                name="weather_change",
                severity=SeverityLevel.WARNING,
                state_watches=[
                    StateWatch(
                        keyword="Safety",
                        field="is_safe",
                        condition=ChangesCondition(),
                    ),
                ],
            ),
        ],
    )


@pytest.fixture
def mock_slack_client():
    """A mock SlackClient."""

    client = AsyncMock()
    client.post_message = AsyncMock(return_value=("C0123456789", "1234567890.123456"))
    client.add_reaction = AsyncMock(return_value=True)
    client.close = AsyncMock()
    return client
