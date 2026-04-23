# PWI4 Module

The PWI4 module provides integration for PlaneWave Instruments devices via the PWI4 HTTP API.

## Supported Devices
- Cover (mirror cover)
- Focuser
- Mount
- Rotator

## Example Config
```yaml
entity: pwi4_service
key: PWI4Config
value:
  endpoints:
    - host: localhost
      port: 8220
      devices:
        mount1:
          device_type: mount
          disable_axis_on_deinit: false
          status_frequency: 1
          timeout: 120
          wrap_autocenter: true
          wrap_interval: 60
          wrap_deadband_deg: 10.0
```

## Usage
```sh
sensorkit service run pwi4_service sensorkit.pwi4.service
```
