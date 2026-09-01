# SPDX-License-Identifier: Apache-2.0
"""Slack notification entity that watches events and state changes."""

from __future__ import annotations

import asyncio
import json
import re
import time as _time
from typing import TYPE_CHECKING

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.backend.base import SpecialProperty, Subject
from sensorkit.backend.event import Event
from sensorkit.common.condition import resolve_field
from sensorkit.slack.client import SlackClient
from sensorkit.slack.formatter import format_alert, format_resolved
from sensorkit.slack.models import (
    ChannelConfig,
    NotificationRule,
    SeverityLevel,
    SlackConfig,
    StateWatch,
)
from sensorkit.slack.summary import SummaryAccumulator

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit


@sk.declare_keyword
class NotifierStatus(BaseModel):
    active: bool = False
    rules: int = 0
    messages_sent: int = 0
    last_error: str | None = None


_UNSET = object()
"""Sentinel for cache entries that have never been seen."""


def _safe_durable_name(prefix: str, rule_name: str) -> str:
    """Sanitise a rule name into a valid NATS durable consumer name."""
    return prefix + re.sub(r"[^A-Za-z0-9_-]", "_", rule_name)


def _cache_key(rule_name: str, entity_name: str, keyword: str, field: str | None) -> str:
    return f"{rule_name}:{entity_name}:{keyword}:{field or '*'}"


@sk.declare_entity
class SlackNotifier:
    """SensorKit entity that watches events and state changes, forwarding notifications to Slack."""

    def __init__(self, config: SlackConfig, client: SlackClient, sk_client: SensorKit):
        self.config = config
        self._client = client
        self.sk_client = sk_client

        # Build severity-to-channels routing map (exclude summary channels)
        self._severity_channels: dict[SeverityLevel, list[ChannelConfig]] = {
            s: [] for s in SeverityLevel
        }
        for ch_config in config.channels.values():
            if ch_config.post_at:
                continue  # summary channel, handled separately
            if ch_config.severity:
                for sev in ch_config.severity:
                    self._severity_channels[sev].append(ch_config)

        self._tasks: list[asyncio.Task] = []
        self._state_cache: dict[str, object] = {}
        self._active_cache: dict[str, bool] = {}
        self._messages_sent = 0
        self._last_error: str | None = None

        # Deduplication: rule_key -> (first_trigger_time, count, last_title)
        self._dedup_cache: dict[str, tuple[float, int, str]] = {}

        # Alert lifecycle: cache_key -> (channel, message_ts, severity)
        self._active_alerts: dict[str, list[tuple[str, str, SeverityLevel]]] = {}

        # Summary accumulators
        self._summary_accumulators: list[SummaryAccumulator] = []

    @sk.on_attach
    async def start(self):
        entity = sk.entity()
        await entity.publish(
            NotifierStatus(active=True, rules=len(self.config.rules))
        )

        # Start rule watchers
        for rule in self.config.rules:
            if rule.events:
                self._tasks.append(
                    asyncio.create_task(self._watch_events(rule))
                )
            if rule.state_watches:
                self._tasks.append(
                    asyncio.create_task(self._watch_state(rule))
                )

        # Start dedup flush task
        self._tasks.append(asyncio.create_task(self._dedup_flush_loop()))

        # Start summary accumulators for channels with post_at
        for ch_config in self.config.channels.values():
            if ch_config.post_at:
                accumulator = SummaryAccumulator(
                    self._client, ch_config, self.sk_client
                )
                self._summary_accumulators.append(accumulator)
                self._tasks.append(asyncio.create_task(accumulator.run()))

    @sk.on_detach
    async def stop(self):
        for task in self._tasks:
            task.cancel()
        for accumulator in self._summary_accumulators:
            await accumulator.stop()
        await self._client.close()

    # -- Severity routing --

    async def _route_message(
        self,
        severity: SeverityLevel,
        blocks_or_attachments: list[dict],
        fallback_text: str,
        alert_key: str | None = None,
    ):
        """Post a message to all channels that accept the given severity."""

        channels = self._severity_channels.get(severity, [])
        is_attachment = blocks_or_attachments and "color" in blocks_or_attachments[0]

        for ch_config in channels:
            channel_id, ts = await self._client.post_message(
                channel=ch_config.channel,
                text=fallback_text,
                blocks=blocks_or_attachments if not is_attachment else None,
                attachments=blocks_or_attachments if is_attachment else None,
            )

            if ts and alert_key and channel_id:
                alerts = self._active_alerts.setdefault(alert_key, [])
                alerts.append((channel_id, ts, severity))

            if ts:
                self._messages_sent += 1
            else:
                self._last_error = f"Failed to post to {ch_config.channel}"

        await self._publish_status()

    async def _route_resolved(self, alert_key: str, title: str):
        """Post resolved notifications as thread replies and add checkmark reactions."""

        alerts = self._active_alerts.pop(alert_key, [])
        for channel, ts, severity in alerts:
            blocks, fallback = format_resolved(title, severity)
            await self._client.post_message(
                channel=channel,
                text=fallback,
                blocks=blocks,
                thread_ts=ts,
            )
            await self._client.add_reaction(channel, ts, "white_check_mark")

    # -- Deduplication --

    def _should_send(self, rule: NotificationRule, title: str) -> bool:
        """Check deduplication window. Returns True if the notification should be sent."""

        if not rule.deduplicate:
            return True

        key = rule.name
        now = _time.monotonic()
        entry = self._dedup_cache.get(key)

        if entry is None or (now - entry[0]) >= rule.deduplicate:
            self._dedup_cache[key] = (now, 1, title)
            return True

        # Within window — increment count, suppress
        self._dedup_cache[key] = (entry[0], entry[1] + 1, title)
        return False

    async def _dedup_flush_loop(self):
        """Periodically check for expired dedup windows and send consolidated messages."""

        while True:
            try:
                await asyncio.sleep(10)
                now = _time.monotonic()
                expired = []

                for key, (first_time, count, title) in self._dedup_cache.items():
                    # Find the rule to get the dedup window
                    rule = next((r for r in self.config.rules if r.name == key), None)
                    if not rule or not rule.deduplicate:
                        continue

                    if (now - first_time) >= rule.deduplicate and count > 1:
                        expired.append((key, count, title, rule.severity))

                for key, count, title, severity in expired:
                    del self._dedup_cache[key]
                    blocks, fallback = format_alert(
                        severity,
                        title,
                        f"{count - 1} additional occurrences suppressed",
                    )
                    await self._route_message(severity, blocks, fallback)

            except Exception as e:
                logger.warning(f"Error in dedup flush: {e}")

    # -- Event filtering --

    @staticmethod
    def _event_matches_filters(event: Event, filters: dict[str, object] | None) -> bool:
        """Check if an event's fields match the rule's filters."""

        if not filters:
            return True
        event_data = event.model_dump(exclude={"event_model", "event_id"})
        for key, expected in filters.items():
            actual = event_data.get(key)
            # Coerce types for comparison (e.g. "false" vs False from YAML)
            if isinstance(expected, bool):
                if actual is not None:
                    actual = bool(actual)
            elif isinstance(expected, (int, float)) and actual is not None:
                try:
                    actual = type(expected)(actual)
                except (TypeError, ValueError):
                    pass
            if actual != expected:
                return False
        return True

    # -- Event watching --

    async def _watch_events(self, rule: NotificationRule):
        """Watch for events matching a rule and send Slack notifications."""

        while True:
            try:
                if rule.entities:
                    await self._watch_events_for_entities(rule, rule.entities)
                else:
                    await self._watch_events_all(rule)
                return
            except Exception as e:
                logger.warning(f"Error in event watcher for rule '{rule.name}': {e}")
                await asyncio.sleep(5)

    async def _watch_events_for_entities(
        self, rule: NotificationRule, entity_names: list[str]
    ):
        """Monitor events from a specific list of entities."""

        tasks = [
            asyncio.create_task(self._watch_entity_events(rule, name))
            for name in entity_names
        ]
        await asyncio.gather(*tasks)

    async def _watch_entity_events(self, rule: NotificationRule, entity_name: str):
        """Monitor all events from a single entity and filter by rule."""

        client = self.sk_client.entity(entity_name)
        async for event in await client.monitor_all_events():
            if rule.events and event.event_model in rule.events:
                if self._event_matches_filters(event, rule.filters):
                    await self._send_event_notification(rule, entity_name, event)

    async def _watch_events_all(self, rule: NotificationRule):
        """Monitor events from all entities using backend wildcard subscription."""

        subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
        consumer = await self.sk_client.backend.impl.stream_consume(
            subject, durable_name=_safe_durable_name("slack-events-", rule.name)
        )
        async for msg in consumer:
            if msg.subject.prop != SpecialProperty.EVENTS:
                continue
            try:
                event = Event.model_validate_json(msg.data)
            except Exception:
                continue

            if rule.events and event.event_model in rule.events:
                if self._event_matches_filters(event, rule.filters):
                    entity_name = ".".join(msg.subject.path)
                    await self._send_event_notification(rule, entity_name, event)

    # -- State watching --

    async def _watch_state(self, rule: NotificationRule):
        """Watch for state changes matching a rule and send Slack notifications."""

        while True:
            try:
                if rule.entities:
                    tasks = [
                        asyncio.create_task(self._watch_entity_state(rule, name))
                        for name in rule.entities
                    ]
                    await asyncio.gather(*tasks)
                else:
                    await self._watch_state_all(rule)
                return
            except Exception as e:
                logger.warning(f"Error in state watcher for rule '{rule.name}': {e}")
                await asyncio.sleep(5)

    async def _watch_entity_state(self, rule: NotificationRule, entity_name: str):
        """Monitor state from a single entity using raw stream consumption."""

        watched_keywords = {w.keyword for w in rule.state_watches or []}
        client = self.sk_client.entity(entity_name)
        consumer = await client._stream.consume(include_latest=True)
        async for msg in consumer:
            keyword_name = msg.subject.prop
            if keyword_name not in watched_keywords:
                continue
            data = _parse_raw_json(msg.data)
            if data is None:
                continue
            for watch in rule.state_watches or []:
                if watch.keyword == keyword_name:
                    await self._evaluate_state_watch(rule, entity_name, watch, data)

    async def _watch_state_all(self, rule: NotificationRule):
        """Monitor state from all entities using backend wildcard subscription."""

        watched_keywords = {w.keyword for w in rule.state_watches or []}
        subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
        consumer = await self.sk_client.backend.impl.stream_consume(
            subject, durable_name=_safe_durable_name("slack-state-", rule.name)
        )
        async for msg in consumer:
            if msg.subject.prop == SpecialProperty.EVENTS:
                continue
            keyword_name = msg.subject.prop
            if keyword_name not in watched_keywords:
                continue
            entity_name = ".".join(msg.subject.path)
            data = _parse_raw_json(msg.data)
            if data is None:
                continue
            for watch in rule.state_watches or []:
                if watch.keyword == keyword_name:
                    await self._evaluate_state_watch(rule, entity_name, watch, data)

    async def _evaluate_state_watch(
        self,
        rule: NotificationRule,
        entity_name: str,
        watch: StateWatch,
        model: object,
    ):
        """Evaluate a state watch condition and send notification if triggered."""

        value = resolve_field(model, watch.field) if watch.field else model
        key = _cache_key(rule.name, entity_name, watch.keyword, watch.field)
        previous = self._state_cache.get(key, _UNSET)
        self._state_cache[key] = value

        if previous is _UNSET:
            # For 'becomes' conditions, seed with the opposite of the threshold
            # so the first matching value fires immediately.
            from sensorkit.common.condition import BecomesCondition
            if isinstance(watch.condition, BecomesCondition):
                if isinstance(watch.condition.threshold, bool):
                    previous = not watch.condition.threshold
                else:
                    previous = None
            else:
                return

        was_active = self._active_cache.get(key, False)
        should_notify, is_active = watch.condition.evaluate(value, previous, was_active)
        self._active_cache[key] = is_active

        if should_notify:
            await self._send_state_notification(rule, entity_name, watch, previous, value, model)
        elif was_active and not is_active:
            # Condition cleared — send resolved notification
            field_label = watch.field or watch.keyword
            title = f"{watch.keyword} on `{entity_name}` ({field_label})"
            await self._route_resolved(key, title)

    # -- Notification formatting and sending --

    async def _send_event_notification(
        self,
        rule: NotificationRule,
        entity_name: str,
        event: Event,
    ):
        """Format and send an event notification."""

        if rule.message_template:
            extra: dict[str, object] = {}
            for key, val in event.model_dump(exclude={"event_model", "event_id"}).items():
                if isinstance(val, dict):
                    extra.update(val)
                else:
                    extra[key] = val
            body = rule.message_template.format(
                event_model=event.event_model,
                entity=entity_name,
                rule=rule.name,
                **extra,
            )
        else:
            body = _format_event_fields(event)

        title = f"{event.event_model} on `{entity_name}`"

        if not self._should_send(rule, title):
            return

        blocks, fallback = format_alert(rule.severity, title, body)
        await self._route_message(rule.severity, blocks, fallback)

    async def _send_state_notification(
        self,
        rule: NotificationRule,
        entity_name: str,
        watch: StateWatch,
        previous: object,
        current: object,
        model: object | None = None,
    ):
        """Format and send a state change notification."""

        field_label = watch.field or watch.keyword

        if rule.message_template:
            # Flatten the full keyword model so template can access sibling fields
            # (e.g. {reason}, {constraint_kind} even when watching just {active})
            extra: dict[str, object] = {}
            if isinstance(model, dict):
                extra.update(model)
            body = rule.message_template.format(
                keyword=watch.keyword,
                field=field_label,
                entity=entity_name,
                previous=previous,
                value=current,
                rule=rule.name,
                **extra,
            )
        else:
            body = f"{field_label}: `{previous}` -> `{current}`"

        title = f"{watch.keyword} on `{entity_name}`"
        alert_key = _cache_key(rule.name, entity_name, watch.keyword, watch.field)

        if not self._should_send(rule, title):
            return

        blocks, fallback = format_alert(
            rule.severity,
            title,
            body,
        )
        await self._route_message(rule.severity, blocks, fallback, alert_key=alert_key)

    async def _publish_status(self):
        """Publish current notifier status."""

        try:
            entity = sk.entity()
            if entity:
                await entity.publish(
                    NotifierStatus(
                        active=True,
                        rules=len(self.config.rules),
                        messages_sent=self._messages_sent,
                        last_error=self._last_error,
                    )
                )
        except Exception as e:
            logger.opt(exception=e).debug("failed to publish notifier status")


def _parse_raw_json(raw: bytes) -> dict | None:
    """Parse raw JSON bytes into a dict without requiring keyword registration."""

    try:
        return json.loads(raw)
    except Exception:
        return None


def _format_event_fields(event: Event) -> str:
    """Format event fields as a human-readable string."""

    lines = []
    for key, value in event.model_dump(exclude={"event_model", "event_id"}).items():
        if value is not None:
            lines.append(f"{key}: `{value}`")
    return "\n".join(lines)
