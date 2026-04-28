"""Tests for Block Kit message formatting."""

from __future__ import annotations

from sensorkit.slack.formatter import format_alert, format_resolved, format_summary
from sensorkit.slack.models import SeverityLevel


class TestFormatAlert:
    def test_critical_uses_red_color(self):
        blocks, fallback = format_alert(SeverityLevel.CRITICAL, "Test", "Body")
        # Critical uses attachments with color sidebar
        assert len(blocks) == 1
        assert blocks[0]["color"] == "#E01E5A"
        assert "CRITICAL" in blocks[0]["fallback"]
        assert fallback == ""

    def test_warning_uses_yellow_color(self):
        blocks, fallback = format_alert(SeverityLevel.WARNING, "Test", "Body")
        assert len(blocks) == 1
        assert blocks[0]["color"] == "#ECB22E"
        assert "WARNING" in blocks[0]["fallback"]
        assert fallback == ""

    def test_info_uses_plain_blocks(self):
        blocks, fallback = format_alert(SeverityLevel.INFO, "Test", "Body")
        # Info uses plain section blocks, no color
        assert blocks[0]["type"] == "section"
        assert "color" not in blocks[0]
        assert "INFO" in fallback

    def test_includes_title_and_body(self):
        blocks, _ = format_alert(SeverityLevel.INFO, "My Title", "My Body")
        text = blocks[0]["text"]["text"]
        assert "*My Title*" in text
        assert "My Body" in text

    def test_includes_fields(self):
        blocks, _ = format_alert(
            SeverityLevel.INFO,
            "Title",
            "Body",
            fields={"Entity": "dome1", "State": "disconnected"},
        )
        # Should have section + fields blocks
        assert len(blocks) == 2
        field_block = blocks[1]
        assert field_block["type"] == "section"
        assert len(field_block["fields"]) == 2

    def test_fallback_text(self):
        blocks, _ = format_alert(SeverityLevel.CRITICAL, "Alert!", "Something broke")
        fallback = blocks[0]["fallback"]
        assert "Alert!" in fallback
        assert "Something broke" in fallback


class TestFormatResolved:
    def test_resolved_format(self):
        blocks, fallback = format_resolved("Dome reconnected", SeverityLevel.CRITICAL)
        assert "Resolved" in fallback
        text = blocks[0]["text"]["text"]
        assert "*Resolved:*" in text
        assert "Dome reconnected" in text


class TestFormatSummary:
    def test_summary_structure(self):
        blocks, fallback = format_summary(
            "2026-04-26",
            {"executing": 18000, "idle": 3600, "weather_closed": 7200},
            {"attempted": 50, "completed": 48, "failed": 2},
            ["03:15 dome1 disconnected", "03:16 dome1 connected"],
        )

        # Header
        assert blocks[0]["type"] == "header"
        assert "2026-04-26" in blocks[0]["text"]["text"]

        # Should have sections for states, observations, health
        assert len(blocks) >= 5  # header + 2 state sections + 2 obs sections + health
        assert "2026-04-26" in fallback

    def test_summary_no_health_events(self):
        blocks, _ = format_summary(
            "2026-04-26",
            {"idle": 28800},
            {"attempted": 0, "completed": 0, "failed": 0},
            [],
        )

        # Last block should say "No events"
        last_text = blocks[-1]["text"]["text"]
        assert "No events" in last_text

    def test_summary_empty(self):
        blocks, fallback = format_summary("2026-04-26", {}, {}, [])
        assert blocks[0]["type"] == "header"
        assert "0 observations" in fallback
