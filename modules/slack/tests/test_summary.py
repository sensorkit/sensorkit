"""Tests for the nightly summary accumulator."""

from __future__ import annotations

import time as _time

import pytest

from sensorkit.slack.models import ChannelConfig
from sensorkit.slack.summary import SummaryAccumulator


@pytest.fixture
def accumulator(mock_slack_client):
    """Create a SummaryAccumulator with test config."""

    ch = ChannelConfig(
        channel="#summary",
        post_at="06:00",
        timezone="UTC",
    )
    sk_client = None  # Not needed for unit tests of accumulation logic
    return SummaryAccumulator(mock_slack_client, ch, sk_client)


class TestAccumulation:
    def test_initial_state(self, accumulator):
        assert accumulator._observation_counts["attempted"] == 0
        assert accumulator._observation_counts["completed"] == 0
        assert accumulator._observation_counts["failed"] == 0
        assert accumulator._health_events == []
        assert accumulator._state_durations == {}

    def test_reset_clears_all(self, accumulator):
        accumulator._observation_counts["attempted"] = 10
        accumulator._observation_counts["completed"] = 8
        accumulator._health_events.append("test event")
        accumulator._state_durations["idle"] = 3600.0

        accumulator._reset()

        assert accumulator._observation_counts["attempted"] == 0
        assert accumulator._observation_counts["completed"] == 0
        assert accumulator._health_events == []
        assert accumulator._state_durations == {}

    def test_state_transition_tracks_duration(self, accumulator):
        accumulator._transition_state("idle")
        entered = accumulator._state_entered_at

        # Simulate time passing
        accumulator._state_entered_at = entered - 10.0  # 10 seconds ago
        accumulator._transition_state("executing")

        assert "idle" in accumulator._state_durations
        assert accumulator._state_durations["idle"] >= 9.0  # ~10 seconds

    def test_finalize_current_state(self, accumulator):
        accumulator._transition_state("executing")
        accumulator._state_entered_at = _time.monotonic() - 5.0

        accumulator._finalize_current_state()

        assert "executing" in accumulator._state_durations
        assert accumulator._state_durations["executing"] >= 4.0


class TestScheduling:
    def test_seconds_until_post_future(self, accumulator):
        seconds = accumulator._seconds_until_post()
        # Should be positive (next occurrence is in the future)
        assert seconds > 0
        # Should be less than 24 hours
        assert seconds <= 86400


class TestPostSummary:
    @pytest.mark.asyncio
    async def test_posts_to_channel(self, accumulator, mock_slack_client):
        accumulator._observation_counts["attempted"] = 5
        accumulator._observation_counts["completed"] = 4
        accumulator._observation_counts["failed"] = 1

        await accumulator._post_summary()

        mock_slack_client.post_message.assert_awaited_once()
        call_kwargs = mock_slack_client.post_message.call_args.kwargs
        assert call_kwargs["channel"] == "#summary"
        assert "blocks" in call_kwargs
