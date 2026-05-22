"""Tests for the SlackNotifier entity."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sensorkit.slack.models import (
    SeverityLevel,
)
from sensorkit.slack.notifier import SlackNotifier, _cache_key, _safe_durable_name


@pytest.fixture
def notifier(sample_config, mock_slack_client):
    """Create a SlackNotifier with mocked dependencies."""

    sk_client = MagicMock()
    return SlackNotifier(sample_config, mock_slack_client, sk_client)


class TestSeverityRouting:
    def test_critical_routes_to_alerts_channel(self, notifier):
        channels = notifier._severity_channels[SeverityLevel.CRITICAL]
        channel_names = [ch.channel for ch in channels]
        assert "#alerts" in channel_names

    def test_info_routes_to_log_channel(self, notifier):
        channels = notifier._severity_channels[SeverityLevel.INFO]
        channel_names = [ch.channel for ch in channels]
        assert "#log" in channel_names

    def test_summary_channel_excluded(self, notifier):
        # Summary channels should not appear in any severity routing
        for sev, channels in notifier._severity_channels.items():
            channel_names = [ch.channel for ch in channels]
            assert "#summary" not in channel_names

    @pytest.mark.asyncio
    async def test_route_message_posts_to_matching_channels(self, notifier, mock_slack_client):
        await notifier._route_message(
            SeverityLevel.CRITICAL,
            [{"type": "section", "text": {"type": "mrkdwn", "text": "test"}}],
            "fallback",
        )

        # Should post to #alerts (the only channel with critical severity)
        mock_slack_client.post_message.assert_awaited_once()
        call_kwargs = mock_slack_client.post_message.call_args.kwargs
        assert call_kwargs["channel"] == "#alerts"

    @pytest.mark.asyncio
    async def test_route_message_increments_count(self, notifier, mock_slack_client):
        assert notifier._messages_sent == 0

        await notifier._route_message(
            SeverityLevel.INFO,
            [{"type": "section", "text": {"type": "mrkdwn", "text": "test"}}],
            "fallback",
        )

        assert notifier._messages_sent == 1


class TestDeduplication:
    def test_first_trigger_sends(self, notifier):
        rule = notifier.config.rules[0]  # device_disconnect, deduplicate=300
        assert notifier._should_send(rule, "test") is True

    def test_second_trigger_within_window_suppresses(self, notifier):
        rule = notifier.config.rules[0]
        notifier._should_send(rule, "test")  # first
        assert notifier._should_send(rule, "test") is False

    def test_trigger_after_window_sends(self, notifier):
        rule = notifier.config.rules[0]
        notifier._should_send(rule, "test")

        # Manually expire the window
        key = rule.name
        entry = notifier._dedup_cache[key]
        notifier._dedup_cache[key] = (entry[0] - 301, entry[1], entry[2])

        assert notifier._should_send(rule, "test") is True

    def test_no_dedup_always_sends(self, notifier):
        rule = notifier.config.rules[1]  # observation_complete, no deduplicate
        assert notifier._should_send(rule, "test") is True
        assert notifier._should_send(rule, "test") is True


class TestAlertLifecycle:
    @pytest.mark.asyncio
    async def test_resolved_posts_thread_reply(self, notifier, mock_slack_client):
        # Simulate an active alert
        alert_key = "test_key"
        notifier._active_alerts[alert_key] = [("#alerts", "111.222", SeverityLevel.CRITICAL)]

        await notifier._route_resolved(alert_key, "Device reconnected")

        # Should post a thread reply
        post_calls = mock_slack_client.post_message.call_args_list
        assert len(post_calls) == 1
        assert post_calls[0].kwargs["thread_ts"] == "111.222"

        # Should add checkmark reaction
        mock_slack_client.add_reaction.assert_awaited_once_with(
            "#alerts", "111.222", "white_check_mark"
        )

    @pytest.mark.asyncio
    async def test_resolved_clears_active_alerts(self, notifier, mock_slack_client):
        alert_key = "test_key"
        notifier._active_alerts[alert_key] = [("#alerts", "111.222", SeverityLevel.CRITICAL)]

        await notifier._route_resolved(alert_key, "Resolved")

        assert alert_key not in notifier._active_alerts


class TestStateEvaluation:
    @pytest.mark.asyncio
    async def test_first_value_does_not_notify(self, notifier, mock_slack_client):
        rule = notifier.config.rules[2]  # weather_change
        watch = rule.state_watches[0]

        await notifier._evaluate_state_watch(
            rule, "safety1", watch, {"is_safe": True}
        )

        mock_slack_client.post_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_change_triggers_notification(self, notifier, mock_slack_client):
        rule = notifier.config.rules[2]  # weather_change (ChangesCondition)
        watch = rule.state_watches[0]

        # First value — cached
        await notifier._evaluate_state_watch(
            rule, "safety1", watch, {"is_safe": True}
        )
        # Second value — changed
        await notifier._evaluate_state_watch(
            rule, "safety1", watch, {"is_safe": False}
        )

        mock_slack_client.post_message.assert_awaited()


class TestHelpers:
    def test_safe_durable_name(self):
        name = _safe_durable_name("slack-events-", "my rule (special)")
        assert name == "slack-events-my_rule__special_"

    def test_cache_key(self):
        key = _cache_key("rule1", "mount1", "Connected", "is_connected")
        assert key == "rule1:mount1:Connected:is_connected"

    def test_cache_key_no_field(self):
        key = _cache_key("rule1", "mount1", "Connected", None)
        assert key == "rule1:mount1:Connected:*"
