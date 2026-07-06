# INDIGO Module

The `indigo` module provides integration for INDIGO devices via the INDIGO WebSocket protocol.

## Supported Devices
- Weather

## Example Config

`host`, `port`, and `client_name` set on the endpoint propagate to every device;
override them per-device when a device lives on a different server. An INDIGO device is **push-based**: it opens a WebSocket
to the server and republishes whenever a property changes, so there is no
`status_frequency` to set like in other polling modules — only a `timeout` for the initial property handshake.

The one device-specific field is `indigo_device`: the name of the INDIGO
driver/device to subscribe to on the server (e.g. `AAG CloudWatcher`). This is
distinct from the entity key (`AAGCloudWatcher` below), which is the SensorKit
entity name another config (e.g. a constraint `provider`) refers to.

```yaml
entity: indigo_service
key: IndigoConfig
value:
  endpoints:
    - host: localhost
      port: 7624
      client_name: SensorKit
      devices:
        AAGCloudWatcher:
          device_type: weather
          indigo_device: AAG CloudWatcher   # INDIGO driver name on the server
          timeout: 30.0
```

## Example: Weather Constraint

A weather constraint pauses a controller (agent) when conditions go out of
bounds. Stations like the AAG CloudWatcher report their assessment as
*categorical* states rather than raw numbers, which the device republishes on
the `WeatherConditions` keyword. Each field carries one of a fixed set of
Title-cased states:

| `field`    | states                      |
| ---------- | --------------------------- |
| `cloud`    | Clear / Cloudy / Overcast   |
| `humidity` | Dry / Normal / Humid        |
| `rain`     | Dry / Wet / Raining         |
| `sky`      | Dark / Light / Very Light   |
| `wind`     | Calm / Moderate / Strong    |

Constrain on these with a `kind: conditional` constraint and an `equals`
condition — one constraint per state you want to hold on. Below, the controller
is constrained whenever it's humid or the wind is strong:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: conditional
          entity: AAGCloudWatcher     # entity publishing WeatherConditions
          keyword: WeatherConditions
          field: humidity
          condition:
            kind: equals
            threshold: Humid          # Title-cased state from the table above
          hold: 300.0                 # seconds to stay constrained after clearing
          ttl: 30.0                   # constrain if no data within this window
          optional: true              # categorical states publish only on change;
                                      # fail-open avoids blocking until the 2nd update
        - kind: conditional
          entity: AAGCloudWatcher
          keyword: WeatherConditions
          field: wind
          condition:
            kind: equals
            threshold: Strong
          hold: 300.0
          ttl: 30.0
          optional: true
```

Add one conditional constraint per state you care about (e.g. another for
`field: rain`, `threshold: Raining`). Set `optional: false` for any you want to
fail closed — be aware that, because these states are only republished when they
change, a fail-closed conditional stays constrained until it has seen two
updates.

### Numeric thresholds (`BasicWeather`)

This module also publishes `BasicWeather` whenever the configured device exposes
an `AUX_WEATHER` property with numeric readings (temperature, humidity, wind
speed, rain rate, …) — not every weather station does. When those numbers are
available, you can constrain on them directly with a `kind: weather` constraint,
either instead of or alongside the categorical conditions above:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: weather
          provider: AAGCloudWatcher   # entity publishing BasicWeather
          hold: 300.0                 # seconds to stay constrained after clearing
          ttl: 30.0                   # constrain if no data within this window
          optional: false             # fail-closed: missing provider => constrained
          humidity_max: 85.0
          humidity_deadband: 2.0
          wind_max: 30.0
          wind_deadband: 2.0
```

Each `*_max` threshold is optional — omit a field to skip that check. The
matching `*_deadband` widens the clear-side threshold (hysteresis) so the
constraint doesn't chatter around the limit.

## Usage
```sh
sensorkit service run indigo_service sensorkit.indigo.service
```
