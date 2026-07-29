# SPDX-License-Identifier: Apache-2.0
"""Shared test fixtures for the Slack module."""

from __future__ import annotations

import pytest

from .fakes import FakeSlackClient
from sensorkit.common.condition import BecomesCondition, ChangesCondition
from sensorkit.slack.models import (
    ChannelConfig,
    NotificationRule,
    SeverityLevel,
    SlackConfig,
    StateWatch,
)


@pytest.fixture(autouse=True)
def _entity_context(entity_impl):
    """Run every Slack test inside a live entity context.

    A live service enters the entity context around every lifecycle hook, so the `sk.entity()`
    calls the notifier makes when publishing its status resolve without any special-casing here.
    """


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
def slack_client() -> FakeSlackClient:
    """A SlackClient stand-in that records what was posted instead of calling Slack."""

    return FakeSlackClient()
