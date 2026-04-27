"""Async Slack API client wrapping the slack_sdk."""

from __future__ import annotations

from loguru import logger
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient


class SlackClient:
    """Async Slack client using the official SDK.

    Uses bot token authentication, supporting chat messages,
    threading, and reactions.
    """

    def __init__(self, token: str):
        self._client = AsyncWebClient(token=token)

    async def post_message(
        self,
        channel: str,
        text: str,
        blocks: list[dict] | None = None,
        attachments: list[dict] | None = None,
        thread_ts: str | None = None,
    ) -> str | None:
        """Post a message to a Slack channel.

        Returns the message timestamp (ts) for threading, or None on failure.
        """

        try:
            response = await self._client.chat_postMessage(
                channel=channel,
                text=text,
                blocks=blocks,
                attachments=attachments,
                thread_ts=thread_ts,
            )
            return response.get("ts")
        except SlackApiError as e:
            logger.warning(f"Slack API error posting to {channel}: {e.response['error']}")
            return None
        except Exception as e:
            logger.warning(f"Error posting to Slack channel {channel}: {e}")
            return None

    async def add_reaction(
        self,
        channel: str,
        timestamp: str,
        reaction: str,
    ) -> bool:
        """Add a reaction emoji to a message."""

        try:
            await self._client.reactions_add(
                channel=channel,
                timestamp=timestamp,
                name=reaction,
            )
            return True
        except SlackApiError as e:
            logger.warning(f"Slack API error adding reaction: {e.response['error']}")
            return False
        except Exception as e:
            logger.warning(f"Error adding Slack reaction: {e}")
            return False

    async def close(self):
        """Close the underlying aiohttp session."""

        if self._client._session:
            await self._client._session.close()
