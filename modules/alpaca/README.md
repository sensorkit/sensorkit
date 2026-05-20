# Alpaca Module

The Alpaca module provides integration for ASCOM Alpaca devices via the `alpyca` package.

## Supported Devices
- Camera
- CoverCalibrator (mirror cover)
- Dome
- FilterWheel
- Focuser
- ObservingConditions (weather)
- Rotator
- SafetyMonitor
- Switch
- Telescope (mount)

## Example Config
```yaml
entity: alpaca_service
key: AlpacaConfig
value:
  endpoints:
    - host: localhost
      port: 11111
      protocol: http
      devices:
        camera1:
          device_type: camera
          device_number: 0
          status_frequency: 1
          timeout: 60
        dome1:
          device_type: dome
          device_number: 0
          status_frequency: 5
          timeout: 60
```

## Usage
```sh
sensorkit service run alpaca_service sensorkit.alpaca.service
```