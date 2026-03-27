# Configuration

SensorKit services read their configuration from the NATS key-value store at startup. Configuration is not stored in files on disk — you write YAML files locally, load them into NATS once, and services pick them up when they start.

## Config record format

Every config record is a YAML object with three fields:

```yaml
entity: <service-name>     # which entity's KV namespace to write to
key: <ModelName>           # the key within that namespace
value:                     # the configuration data
  field_one: ...
  field_two: ...
```

A single file can hold multiple records separated by `---`:

```yaml
entity: ascom-service
key: AscomConfig
value:
  endpoints:
    - host: 192.168.1.10
      port: 32323
      devices:
        - entity: my-mount
          device_type: telescope
---
entity: my-sensor
key: SensorConfig
value:
  controller_name: my-sensor
  devices:
    mount: my-mount
    camera: my-camera
  site_position:
    latitude_degrees: 34.05
    longitude_degrees: -118.25
    altitude_km: 0.3
```

## Loading config into NATS

```bash
# Load a single file
sensorkit kv load config.yaml

# Load all files in a directory
sensorkit kv load configs/*.yaml

# Skip keys that already exist (do not overwrite)
sensorkit kv load --no-clobber config.yaml
```

Config must be loaded before the service that reads it is started. Services read their config once at startup; if you change a value, restart the service.

## Inspecting the KV store

```bash
# List all entries
sensorkit kv ls

# List entries for one entity
sensorkit kv ls -e my-sensor

# Read a specific key
sensorkit kv get -e my-sensor SensorConfig

# Write a value directly
sensorkit kv put -e my-sensor SomeKey '{"field": "value"}'

# Delete a key
sensorkit kv delete -e my-sensor SomeKey

# Delete all keys for an entity
sensorkit kv delete -e my-sensor
```

## Applying a config change

To update a service's configuration:

```bash
# Overwrite the key with the new value
sensorkit kv load new-config.yaml

# Then restart the service
sensorkit service run sensorkit.std.sensor my-sensor
```
