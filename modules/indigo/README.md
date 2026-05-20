# INDIGO Module

The INDIGO module provides integration for INDIGO devices via the INDIGO WebSocket protocol.

## Supported Devices
- Weather

## Example Config
```yaml
entity: indigo_service
key: IndigoConfig
value:
  endpoints:
    - host: localhost
      port: 7624
      client_name: SensorKit
      devices:
        weather1:
          device_type: weather
          indigo_device: AAG CloudWatcher
          status_frequency: 5
          timeout: 60
```

## Usage
```sh
sensorkit service run indigo_service sensorkit.indigo.service
```
