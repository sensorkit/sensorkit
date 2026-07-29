# SPDX-License-Identifier: Apache-2.0
"""Tests for the Slack API client wrapper."""

from __future__ import annotations

import pytest
from slack_sdk.errors import SlackApiError

from .fakes import FakeWebClient
from sensorkit.slack import client as client_mod
from sensorkit.slack.client import SlackClient


@pytest.fixture
def web_client(monkeypatch) -> FakeWebClient:
    """Stand the Slack SDK's web client down so `SlackClient` talks to the fake instead."""

    fake = FakeWebClient()
    monkeypatch.setattr(client_mod, "AsyncWebClient", lambda token: fake)

    return fake


class TestPostMessage:
    @pytest.mark.asyncio
    async def test_returns_ts_on_success(self, web_client):
        client = SlackClient("xoxb-test")
        channel_id, ts = await client.post_message("#test", "hello")

        assert channel_id == "C0123456789"
        assert ts == "1234567890.123456"
        assert web_client.posts == [
            {
                "channel": "#test",
                "text": "hello",
                "blocks": None,
                "attachments": None,
                "thread_ts": None,
            }
        ]

    @pytest.mark.asyncio
    async def test_passes_blocks(self, web_client):
        client = SlackClient("xoxb-test")
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]
        await client.post_message("#test", "fallback", blocks=blocks)

        assert web_client.posts[0]["blocks"] == blocks

    @pytest.mark.asyncio
    async def test_passes_thread_ts(self, web_client):
        client = SlackClient("xoxb-test")
        await client.post_message("#test", "reply", thread_ts="111.222")

        assert web_client.posts[0]["thread_ts"] == "111.222"

    @pytest.mark.asyncio
    async def test_returns_none_on_api_error(self, web_client):
        web_client.post_response = SlackApiError("error", response={"error": "channel_not_found"})

        client = SlackClient("xoxb-test")
        channel_id, ts = await client.post_message("#bad-channel", "hello")

        assert channel_id is None
        assert ts is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, web_client):
        web_client.post_response = ConnectionError("network down")

        client = SlackClient("xoxb-test")
        channel_id, ts = await client.post_message("#test", "hello")

        assert channel_id is None
        assert ts is None


class TestAddReaction:
    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, web_client):
        client = SlackClient("xoxb-test")
        result = await client.add_reaction("#test", "123.456", "white_check_mark")

        assert result is True
        assert web_client.reactions == [
            {"channel": "#test", "timestamp": "123.456", "name": "white_check_mark"}
        ]

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self, web_client):
        web_client.reaction_error = SlackApiError("error", response={"error": "already_reacted"})

        client = SlackClient("xoxb-test")
        result = await client.add_reaction("#test", "123.456", "white_check_mark")

        assert result is False
