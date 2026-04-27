# Slack Module

Real-time Slack notifications and daily summaries for SensorKit observatory operations.

## Features

- **Real-time alerts** for device failures, weather changes, and operational events
- **Severity-based routing** (critical, warning, info) to different Slack channels
- **Alert deduplication** to prevent notification floods
- **Alert lifecycle** with resolved notifications (thread replies + checkmark reactions)
- **Nightly summaries** with observation counts, operational state durations, and health events
- **Block Kit formatting** with color-coded severity indicators

## Setup

### 1. Create a Slack Bot

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and create a new app
2. Under **OAuth & Permissions**, add these scopes:
   - `chat:write` — post messages
   - `reactions:write` — add reaction emojis
3. Install the app to your workspace
4. Copy the **Bot User OAuth Token** (`xoxb-...`)

### 2. Invite the Bot

Invite the bot to each channel it will post to:
```
/invite @YourBotName
```

### 3. Configure

Create a config file (see example below) and load it:
```sh
sensorkit kv load slack_config.yaml
```

### 4. Run

```sh
sensorkit service run slack_service sensorkit.slack.service
```

## Example Config

```yaml
entity: slack_service
key: SlackConfig
value:
  entity: slack_notifier
  token: ${SLACK_BOT_TOKEN}
  channels:
    alerts:
      channel: "#observatory-alerts"
      severity: [critical, warning]
    log:
      channel: "#observatory-log"
      severity: [info]
    summary:
      channel: "#daily-summary"
      post_at: "06:00"
      timezone: "US/Hawaii"
  rules:
    - name: device_disconnect
      severity: critical
      state_watches:
        - keyword: Connected
          field: is_connected
          condition: {kind: becomes, threshold: false}
      deduplicate: 300

    - name: weather_unsafe
      severity: warning
      state_watches:
        - keyword: Safety
          field: is_safe
          condition: {kind: becomes, threshold: false}

    - name: observation_complete
      severity: info
      events: [TaskExecutionState]
```

## Config Reference

### Channels

Each channel defines where messages are routed:

| Field | Type | Description |
|-------|------|-------------|
| `channel` | string | Slack channel name (e.g. `#observatory-alerts`) |
| `severity` | list | Severity levels routed here: `critical`, `warning`, `info` |
| `post_at` | string | Time to post daily summary (e.g. `"06:00"`). Makes this a summary channel. |
| `timezone` | string | Timezone for summary scheduling (e.g. `"US/Hawaii"`) |

### Rules

Each rule defines what triggers a notification:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Rule name (used for dedup keys and logging) |
| `severity` | string | `critical`, `warning`, or `info` (default: `info`) |
| `entities` | list | Entity names to watch (omit for all entities) |
| `events` | list | Event model names to match (e.g. `CommandDone`, `TaskExecutionState`) |
| `state_watches` | list | State keyword watches with conditions |
| `message_template` | string | Custom message template with `{variable}` interpolation |
| `deduplicate` | int | Seconds to suppress repeated triggers (omit for no dedup) |

### State Watch Conditions

Available condition types (set via `kind`):

| Condition | Description | Parameters |
|-----------|-------------|------------|
| `changes` | Any value change (default) | — |
| `becomes` | Value transitions to threshold | `threshold` |
| `equals` | Value equals threshold | `threshold` |
| `above` | Value exceeds threshold | `threshold`, `deadband` |
| `below` | Value below threshold | `threshold`, `deadband` |
| `crosses_above` | Upward crossing (fires once) | `threshold`, `deadband` |
| `crosses_below` | Downward crossing (fires once) | `threshold`, `deadband` |

### Message Templates

Templates support variable interpolation:

**Event templates:** `{event_model}`, `{entity}`, `{rule}`, plus flattened event fields

**State templates:** `{keyword}`, `{field}`, `{entity}`, `{previous}`, `{value}`, `{rule}`

## Severity Levels

- **Critical** — Red sidebar. Device failures, dome stuck, hardware errors. Things a technician needs to address.
- **Warning** — Yellow sidebar. Weather unsafe, minor anomalies. Informational but noteworthy.
- **Info** — Plain text. Observation completions, tracking acquired. Normal operational events.

## Deduplication

When `deduplicate` is set on a rule (in seconds), repeated triggers within that window are suppressed. After the window expires, a consolidated message reports how many additional occurrences were suppressed. This prevents notification floods from flapping devices.

## Daily Summary

Channels with `post_at` configured receive a daily summary at the specified local time. The summary includes:

- **Operational state durations** — time spent executing, idle, weather-closed, etc.
- **Observation counts** — attempted, completed, failed
- **Health events** — device connect/disconnect, safety state changes
