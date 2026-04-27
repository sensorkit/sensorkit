"""Tests for the Slack API client wrapper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sensorkit.slack.client import SlackClient


@pytest.fixture
def mock_web_client():
    """Patch AsyncWebClient and return the mock instance."""

    with patch("sensorkit.slack.client.AsyncWebClient") as MockWebClient:
        mock_instance = AsyncMock()
        MockWebClient.return_value = mock_instance
        yield mock_instance


class TestPostMessage:
    @pytest.mark.asyncio
    async def test_returns_ts_on_success(self, mock_web_client):
        mock_web_client.chat_postMessage = AsyncMock(
            return_value={"ok": True, "ts": "1234567890.123456"}
        )

        client = SlackClient("xoxb-test")
        ts = await client.post_message("#test", "hello")

        assert ts == "1234567890.123456"
        mock_web_client.chat_postMessage.assert_awaited_once_with(
            channel="#test",
            text="hello",
            blocks=None,
            attachments=None,
            thread_ts=None,
        )

    @pytest.mark.asyncio
    async def test_passes_blocks(self, mock_web_client):
        mock_web_client.chat_postMessage = AsyncMock(
            return_value={"ok": True, "ts": "123"}
        )

        client = SlackClient("xoxb-test")
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]
        await client.post_message("#test", "fallback", blocks=blocks)

        call_kwargs = mock_web_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["blocks"] == blocks

    @pytest.mark.asyncio
    async def test_passes_thread_ts(self, mock_web_client):
        mock_web_client.chat_postMessage = AsyncMock(
            return_value={"ok": True, "ts": "123"}
        )

        client = SlackClient("xoxb-test")
        await client.post_message("#test", "reply", thread_ts="111.222")

        call_kwargs = mock_web_client.chat_postMessage.call_args.kwargs
        assert call_kwargs["thread_ts"] == "111.222"

    @pytest.mark.asyncio
    async def test_returns_none_on_api_error(self, mock_web_client):
        from slack_sdk.errors import SlackApiError

        mock_web_client.chat_postMessage = AsyncMock(
            side_effect=SlackApiError("error", response={"error": "channel_not_found"})
        )

        client = SlackClient("xoxb-test")
        ts = await client.post_message("#bad-channel", "hello")

        assert ts is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, mock_web_client):
        mock_web_client.chat_postMessage = AsyncMock(
            side_effect=ConnectionError("network down")
        )

        client = SlackClient("xoxb-test")
        ts = await client.post_message("#test", "hello")

        assert ts is None


class TestAddReaction:
    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, mock_web_client):
        mock_web_client.reactions_add = AsyncMock()

        client = SlackClient("xoxb-test")
        result = await client.add_reaction("#test", "123.456", "white_check_mark")

        assert result is True
        mock_web_client.reactions_add.assert_awaited_once_with(
            channel="#test",
            timestamp="123.456",
            name="white_check_mark",
        )

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self, mock_web_client):
        from slack_sdk.errors import SlackApiError

        mock_web_client.reactions_add = AsyncMock(
            side_effect=SlackApiError("error", response={"error": "already_reacted"})
        )

        client = SlackClient("xoxb-test")
        result = await client.add_reaction("#test", "123.456", "white_check_mark")

        assert result is False
