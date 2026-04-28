"""Nightly summary accumulator for observatory operations."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from loguru import logger

from sensorkit.backend.base import SpecialProperty, Subject
from sensorkit.core.client import SensorKit
from sensorkit.slack.client import SlackClient
from sensorkit.slack.formatter import format_summary
from sensorkit.slack.models import ChannelConfig


class SummaryAccumulator:
    """Accumulates observatory data and posts a daily summary.

    Watches TaskExecutionState events, Connected keywords, and Safety
    keywords to build a picture of the night's operations. Posts a
    formatted summary to Slack at the configured time.
    """

    def __init__(
        self,
        client: SlackClient,
        channel_config: ChannelConfig,
        sk_client: SensorKit,
    ):
        self._client = client
        self._channel = channel_config.channel
        self._post_at = channel_config.post_at
        self._tz = ZoneInfo(channel_config.timezone or "UTC")
        self._sk_client = sk_client

        self._state_durations: dict[str, float] = {}
        self._observation_counts: dict[str, int] = {
            "attempted": 0,
            "completed": 0,
            "failed": 0,
        }
        self._health_events: list[str] = []

        # Track current state for duration calculation
        self._current_state: str | None = None
        self._state_entered_at: float | None = None

        self._tasks: list[asyncio.Task] = []

    async def run(self):
        """Main loop: accumulate data and post at scheduled time."""

        # Start data collection tasks
        self._tasks.append(asyncio.create_task(self._watch_task_execution()))
        self._tasks.append(asyncio.create_task(self._watch_device_health()))
        self._tasks.append(asyncio.create_task(self._watch_safety()))

        try:
            while True:
                sleep_seconds = self._seconds_until_post()
                logger.debug(
                    f"summary accumulator sleeping {sleep_seconds:.0f}s "
                    f"until {self._post_at} {self._tz}"
                )
                await asyncio.sleep(sleep_seconds)
                await self._post_summary()
                self._reset()
        except asyncio.CancelledError:
            for task in self._tasks:
                task.cancel()
            raise

    def _seconds_until_post(self) -> float:
        """Calculate seconds until the next post_at time."""

        now = datetime.now(self._tz)
        hour, minute = (int(x) for x in self._post_at.split(":"))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if target <= now:
            target += timedelta(days=1)

        return (target - now).total_seconds()

    def _reset(self):
        """Clear accumulated data for a new day."""

        self._state_durations.clear()
        self._observation_counts = {
            "attempted": 0,
            "completed": 0,
            "failed": 0,
        }
        self._health_events.clear()
        self._current_state = None
        self._state_entered_at = None

    async def _post_summary(self):
        """Format and post the daily summary."""

        # Finalize any in-progress state duration
        self._finalize_current_state()

        now = datetime.now(self._tz)
        date_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

        blocks, fallback = format_summary(
            date_str,
            self._state_durations,
            self._observation_counts,
            self._health_events,
        )

        ts = await self._client.post_message(
            channel=self._channel,
            text=fallback,
            blocks=blocks,
        )

        if ts:
            logger.debug(f"posted daily summary to {self._channel}")
        else:
            logger.warning(f"failed to post daily summary to {self._channel}")

    def _finalize_current_state(self):
        """Add duration for the current in-progress state."""

        if self._current_state and self._state_entered_at:
            import time as _time

            elapsed = _time.monotonic() - self._state_entered_at
            self._state_durations[self._current_state] = (
                self._state_durations.get(self._current_state, 0.0) + elapsed
            )
            self._state_entered_at = _time.monotonic()

    def _transition_state(self, new_state: str):
        """Record a state transition."""

        import time as _time

        self._finalize_current_state()
        self._current_state = new_state
        self._state_entered_at = _time.monotonic()

    async def _watch_task_execution(self):
        """Watch TaskExecutionState events for observation counting."""

        while True:
            try:
                subject = Subject(path=(), prop=SpecialProperty.EVENTS)
                consumer_name = "slack-summary-tasks"

                consumer = await self._sk_client.backend.impl.stream_consume(
                    subject, durable_name=consumer_name,
                )
                async for msg in consumer:
                    try:
                        data = json.loads(msg.data)
                        event_model = data.get("event_model")

                        if event_model != "TaskExecutionState":
                            continue

                        executing = data.get("executing", False)
                        finished = data.get("finished")

                        if executing:
                            self._observation_counts["attempted"] += 1
                            self._transition_state("executing")
                        elif finished:
                            success = finished.get("success", False) if isinstance(finished, dict) else False
                            if success:
                                self._observation_counts["completed"] += 1
                            else:
                                self._observation_counts["failed"] += 1
                            self._transition_state("idle")

                    except Exception as e:
                        logger.debug(f"Error parsing task event: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error in summary task watcher: {e}")
                await asyncio.sleep(5)

    async def _watch_device_health(self):
        """Watch Connected keywords for device health events."""

        while True:
            try:
                subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
                consumer_name = "slack-summary-health"

                consumer = await self._sk_client.backend.impl.stream_consume(
                    subject, durable_name=consumer_name,
                )
                async for msg in consumer:
                    try:
                        data = json.loads(msg.data)
                        keyword_model = data.get("keyword_model", "")

                        if keyword_model != "Connected":
                            continue

                        entity = msg.subject.split(".")[0] if "." in msg.subject else "unknown"
                        is_connected = data.get("is_connected", True)

                        now = datetime.now(self._tz).strftime("%H:%M:%S")
                        if not is_connected:
                            self._health_events.append(f"{now} {entity} disconnected")
                        else:
                            self._health_events.append(f"{now} {entity} connected")

                    except Exception as e:
                        logger.debug(f"Error parsing health event: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error in summary health watcher: {e}")
                await asyncio.sleep(5)

    async def _watch_safety(self):
        """Watch Safety keywords for safe/unsafe transitions."""

        while True:
            try:
                subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
                consumer_name = "slack-summary-safety"

                consumer = await self._sk_client.backend.impl.stream_consume(
                    subject, durable_name=consumer_name,
                )
                async for msg in consumer:
                    try:
                        data = json.loads(msg.data)
                        keyword_model = data.get("keyword_model", "")

                        if "Safety" not in keyword_model:
                            continue

                        is_safe = data.get("is_safe", True)
                        now = datetime.now(self._tz).strftime("%H:%M:%S")

                        if not is_safe:
                            self._health_events.append(f"{now} weather unsafe")
                            self._transition_state("weather_closed")
                        else:
                            self._health_events.append(f"{now} weather safe")
                            self._transition_state("idle")

                    except Exception as e:
                        logger.debug(f"Error parsing safety event: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error in summary safety watcher: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        """Cancel all watcher tasks."""

        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
