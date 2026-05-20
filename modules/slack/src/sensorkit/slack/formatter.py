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
    health_events: list[str],
) -> tuple[list[dict], str]:
    """Format a nightly summary as Block Kit blocks."""

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
    if health_events:
        event_text = "\n".join(f"\u2022 {e}" for e in health_events)
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Health Events*\n{event_text}"},
            }
        )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Health Events*\nNo events"},
            }
        )

    # Fallback text
    total_obs = sum(observation_counts.values()) if observation_counts else 0
    fallback = f"Daily Summary -- {date}: {total_obs} observations, {len(health_events)} health events"

    return blocks, fallback
