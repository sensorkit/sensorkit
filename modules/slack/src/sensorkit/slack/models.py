from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel

from sensorkit.common.condition import AnyCondition, ChangesCondition


class SeverityLevel(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class StateWatch(BaseModel):
    """Watch a keyword field and evaluate a condition on changes."""

    keyword: str
    field: str | None = None
    condition: AnyCondition = ChangesCondition()


class NotificationRule(BaseModel):
    """A rule that triggers notifications based on events or state changes."""

    name: str
    severity: SeverityLevel = SeverityLevel.INFO
    entities: list[str] | None = None
    events: list[str] | None = None
    state_watches: list[StateWatch] | None = None
    message_template: str | None = None
    deduplicate: int | None = None  # seconds


class ChannelConfig(BaseModel):
    """Configuration for a single Slack channel."""

    channel: str  # e.g. "#observatory-alerts"
    severity: list[SeverityLevel] | None = None
    post_at: str | None = None  # e.g. "06:00" — makes this a summary channel
    timezone: str | None = None  # e.g. "US/Hawaii"


class SlackConfig(BaseModel):
    """Root configuration for the Slack notification service."""

    entity: str
    token: str
    channels: dict[str, ChannelConfig]
    rules: list[NotificationRule] = []
