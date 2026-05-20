# Node Platform Module

The Node Platform module provides integration for the Observable Space Node Platform via the `ourskyai_node_platform_api` package.

## Supported Devices
- Enclosure (dome)
- Focuser
- Mount
- Rotator
- Weather

## Example Config
```yaml
entity: node_platform
key: NodePlatformConfig
value:
  endpoints:
    - host: nodeplatformhost
      port: 8000
      env_file: .env                  # defines NODE_PLATFORM_API_KEY, NODE_PLATFORM_LINEAGE_ID
      operatoin_mode: assisted        # ASSISTED = Node Platform controls shutter, MANUAL = SensorKit controls shutter
      devices:
        weather1:
          device_type: weather
          metric_lookback_seconds: 300.0
          status_frequency: 5.0
          timeout: 60
        enclosure1:
          device_type: dome
          status_frequency: 1
          timeout: 120
        mount1:
          device_type: mount
          heater_power:
            M1: 1.0                   # percent
            M2: 1.0
            M3: 0.0
          status_frequency_slow: 1.0
          status_frequency_fast: 0.1  # increased publishing rate during tracking
          timeout: 120
        rotator1:
          device_type: rotator
          derotate: false
          status_frequency: 1
          timeout: 120
```

## Usage
```sh
sensorkit service run node_platform_service sensorkit.node_platform.service
```

