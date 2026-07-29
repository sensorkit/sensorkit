# SPDX-License-Identifier: Apache-2.0
"""Stand-ins for the Slack Web API.

`FakeWebClient` replaces `slack_sdk`'s `AsyncWebClient` so `SlackClient`'s own error handling can
be exercised; `FakeSlackClient` replaces `SlackClient` itself for the notifier and summary tests,
which care about what was posted, not how it was serialised.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeWebClient:
    """Stands in for slack_sdk's AsyncWebClient.

    `post_response` and `reaction_error` script what the Slack API returns: an exception is
    raised, anything else is returned. Calls are recorded as the kwargs the SDK received.
    """

    post_response: Any = field(
        default_factory=lambda: {"ok": True, "channel": "C0123456789", "ts": "1234567890.123456"}
    )
    reaction_error: BaseException | None = None
    posts: list[dict[str, Any]] = field(default_factory=list)
    reactions: list[dict[str, Any]] = field(default_factory=list)
    session: Any = None

    async def chat_postMessage(self, **kwargs):  # noqa: N802 — mirrors the SDK's method name
        self.posts.append(kwargs)

        if isinstance(self.post_response, BaseException):
            raise self.post_response

        return self.post_response

    async def reactions_add(self, **kwargs):
        self.reactions.append(kwargs)

        if self.reaction_error is not None:
            raise self.reaction_error


@dataclass
class FakeSlackClient:
    """Stands in for SlackClient, recording every message and reaction the notifier sends.

    `post_message` hands back a distinct timestamp per call so thread replies and reactions can be
    matched to the message that opened the alert.
    """

    channel_id: str = "C0123456789"
    posts: list[dict[str, Any]] = field(default_factory=list)
    reactions: list[tuple[str, str, str]] = field(default_factory=list)
    closed: bool = False

    async def post_message(self, **kwargs) -> tuple[str | None, str | None]:
        self.posts.append(kwargs)
        return self.channel_id, f"1234567890.{len(self.posts):06d}"

    async def add_reaction(self, channel: str, timestamp: str, reaction: str) -> bool:
        self.reactions.append((channel, timestamp, reaction))
        return True

    async def close(self) -> None:
        self.closed = True

    def channels(self) -> list[str]:
        """The channel of every message posted, oldest first."""
        return [post["channel"] for post in self.posts]
