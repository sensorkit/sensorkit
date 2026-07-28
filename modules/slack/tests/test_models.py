# SPDX-License-Identifier: Apache-2.0
"""Tests for Slack module configuration models."""

from __future__ import annotations

from sensorkit.common.condition import (
    BecomesCondition,
    ChangesCondition,
    CrossesAboveCondition,
)
from sensorkit.slack.models import (
    ChannelConfig,
    NotificationRule,
    SeverityLevel,
    SlackConfig,
    StateWatch,
)


class TestSeverityLevel:
    def test_values(self):
        assert SeverityLevel.CRITICAL == "critical"
        assert SeverityLevel.WARNING == "warning"
        assert SeverityLevel.INFO == "info"

    def test_from_string(self):
        assert SeverityLevel("critical") == SeverityLevel.CRITICAL


class TestStateWatch:
    def test_default_condition(self):
        watch = StateWatch(keyword="Connected")
        assert isinstance(watch.condition, ChangesCondition)

    def test_custom_condition(self):
        watch = StateWatch(
            keyword="Connected",
            field="is_connected",
            condition=BecomesCondition(threshold=False),
        )
        assert isinstance(watch.condition, BecomesCondition)
        assert watch.condition.threshold is False

    def test_from_dict(self):
        watch = StateWatch.model_validate({
            "keyword": "Temperature",
            "field": "temperature",
            "condition": {"kind": "crosses_above", "threshold": 30.0},
        })
        assert isinstance(watch.condition, CrossesAboveCondition)
        assert watch.condition.threshold == 30.0


class TestNotificationRule:
    def test_defaults(self):
        rule = NotificationRule(name="test")
        assert rule.severity == SeverityLevel.INFO
        assert rule.entities is None
        assert rule.events is None
        assert rule.state_watches is None
        assert rule.deduplicate is None

    def test_with_deduplication(self):
        rule = NotificationRule(name="test", deduplicate=300)
        assert rule.deduplicate == 300

    def test_with_events(self):
        rule = NotificationRule(
            name="test",
            severity=SeverityLevel.CRITICAL,
            events=["CommandDone", "TaskExecutionState"],
        )
        assert rule.severity == SeverityLevel.CRITICAL
        assert len(rule.events) == 2


class TestChannelConfig:
    def test_basic(self):
        ch = ChannelConfig(channel="#alerts")
        assert ch.channel == "#alerts"
        assert ch.severity is None
        assert ch.post_at is None

    def test_summary_channel(self):
        ch = ChannelConfig(
            channel="#summary",
            post_at="06:00",
            timezone="US/Hawaii",
        )
        assert ch.post_at == "06:00"
        assert ch.timezone == "US/Hawaii"


class TestSlackConfig:
    def test_parse_from_dict(self):
        config = SlackConfig.model_validate({
            "channels": {
                "alerts": {
                    "channel": "#observatory-alerts",
                    "severity": ["critical", "warning"],
                },
                "log": {
                    "channel": "#observatory-log",
                    "severity": ["info"],
                },
            },
            "rules": [
                {
                    "name": "device_disconnect",
                    "severity": "critical",
                    "state_watches": [
                        {
                            "keyword": "Connected",
                            "field": "is_connected",
                            "condition": {"kind": "becomes", "threshold": False},
                        },
                    ],
                    "deduplicate": 300,
                },
            ],
        })

        assert config.env_file == ".env"
        assert len(config.channels) == 2
        assert len(config.rules) == 1
        assert config.rules[0].severity == SeverityLevel.CRITICAL
        assert config.rules[0].deduplicate == 300

    def test_empty_rules(self):
        config = SlackConfig(channels={"log": ChannelConfig(channel="#log")})
        assert config.rules == []
