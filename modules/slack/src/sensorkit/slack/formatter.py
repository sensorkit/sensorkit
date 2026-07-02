"""Block Kit message formatting for Slack notifications."""

from __future__ import annotations

from sensorkit.slack.models import SeverityLevel

_SEVERITY_COLORS = {
    SeverityLevel.CRITICAL: "#E01E5A",
    SeverityLevel.WARNING: "#ECB22E",
}

_SEVERITY_LABELS = {
    SeverityLevel.CRITICAL: "CRITICAL",
    SeverityLevel.WARNING: "WARNING",
    SeverityLevel.INFO: "INFO",
}

# Cap on per-device lines in the health summary, so the section stays well within
# Slack's 3000-char-per-section limit regardless of how many devices flapped.
_MAX_HEALTH_LINES = 20


def _plural(n: int) -> str:
    return "" if n == 1 else "s"


def _summarize_health(events: list[tuple[str, str, str]]) -> dict:
    """Collapse a chronological list of (time, entity, state) health records into
    transition-aware counts.

    Consecutive identical states for the same entity are collapsed, so status-loop
    republishes (which can number in the thousands over a night) don't inflate the
    counts. Only genuine state changes are tallied.
    """

    last_state: dict[str, str] = {}
    weather_unsafe_periods = 0
    weather_final: str | None = None
    device_disconnects: dict[str, int] = {}
    device_final: dict[str, str] = {}

    for _time_str, entity, state in events:
        if last_state.get(entity) == state:
            continue  # republish of the same state — not a transition
        last_state[entity] = state
        if entity == "weather":
            weather_final = state
            if state == "unsafe":
                weather_unsafe_periods += 1
        else:
            device_final[entity] = state
            if state == "disconnected":
                device_disconnects[entity] = device_disconnects.get(entity, 0) + 1

    return {
        "weather_unsafe_periods": weather_unsafe_periods,
        "weather_final": weather_final,
        "device_disconnects": device_disconnects,
        "device_final": device_final,
    }


def format_alert(
    severity: SeverityLevel,
    title: str,
    body: str,
    fields: dict[str, str] | None = None,
) -> tuple[list[dict], str]:
    """Format a real-time alert as Block Kit blocks.

    Returns (blocks_or_attachments, fallback_text).
    Critical/warning use attachments with colored sidebars.
    Info uses plain section blocks.
    """

    color = _SEVERITY_COLORS.get(severity)

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{title}*\n{body}"},
        },
    ]

    if fields:
        field_blocks = [
            {"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in fields.items()
        ]
        blocks.append({"type": "section", "fields": field_blocks})

    # Fallback text for notifications/accessibility
    fallback = f"[{_SEVERITY_LABELS[severity]}] {title}: {body}"

    if color:
        # Use attachments for colored sidebar (critical/warning).
        # Set text to empty so Slack doesn't duplicate above the attachment.
        return [{"color": color, "blocks": blocks, "fallback": fallback}], ""
    else:
        # Plain blocks for info
        return blocks, fallback


def format_resolved(
    title: str,
    original_severity: SeverityLevel,
) -> tuple[list[dict], str]:
    """Format a resolved notification (posted as a thread reply)."""

    fallback = f"Resolved: {title}"
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Resolved:* {title}"},
        },
    ]

    return blocks, fallback


def format_summary(
    date: str,
    state_durations: dict[str, float],
    observation_counts: dict[str, int],
    health_events: list[tuple[str, str, str]],
) -> tuple[list[dict], str]:
    """Format a nightly summary as Block Kit blocks.

    `health_events` is a chronological list of (time, entity, state) records;
    it is collapsed to transition counts so a night of status-loop republishes
    can't blow past Slack's message-size limit.
    """

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Daily Summary -- {date}"},
        },
    ]

    # Operational states
    if state_durations:
        duration_fields = []
        for state, seconds in state_durations.items():
            hours = seconds / 3600
            duration_fields.append(
                {"type": "mrkdwn", "text": f"*{state}*\n{hours:.1f}h"}
            )
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Operational States*"},
            }
        )
        blocks.append({"type": "section", "fields": duration_fields})

    # Observation counts
    if observation_counts:
        count_fields = [
            {"type": "mrkdwn", "text": f"*{k}*\n{v}"}
            for k, v in observation_counts.items()
        ]
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Observations*"},
            }
        )
        blocks.append({"type": "section", "fields": count_fields})

    # Health events
    health = _summarize_health(health_events)
    lines: list[str] = []

    periods = health["weather_unsafe_periods"]
    if periods:
        currently = " (currently unsafe)" if health["weather_final"] == "unsafe" else ""
        lines.append(f"\u2022 Weather: {periods} unsafe period{_plural(periods)}{currently}")

    disconnects = health["device_disconnects"]
    device_final = health["device_final"]
    ranked = sorted(disconnects.items(), key=lambda kv: (-kv[1], kv[0]))
    for entity, count in ranked[:_MAX_HEALTH_LINES]:
        down = " (still down)" if device_final.get(entity) == "disconnected" else ""
        lines.append(f"\u2022 {entity}: {count} disconnect{_plural(count)}{down}")
    if len(ranked) > _MAX_HEALTH_LINES:
        lines.append(f"\u2022 \u2026and {len(ranked) - _MAX_HEALTH_LINES} more device(s)")

    health_text = "\n".join(lines) if lines else "No notable events"
    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Health Events*\n{health_text}"},
        }
    )

    # Fallback text
    total_obs = sum(observation_counts.values()) if observation_counts else 0
    total_disconnects = sum(disconnects.values())
    fallback = (
        f"Daily Summary -- {date}: {total_obs} observations, "
        f"{periods} weather closure{_plural(periods)}, "
        f"{total_disconnects} disconnect{_plural(total_disconnects)}"
    )

    return blocks, fallback
