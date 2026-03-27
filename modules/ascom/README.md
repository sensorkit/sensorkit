# ASCOM Module

The ASCOM module provides integration with ASCOM Alpaca devices (mounts, cameras, domes, etc.).

## Features
- Mount, camera, dome, weather, focuser, filter wheel, and rotator support
- Device status polling and control

## Example Config
```yaml
entity: ascom_service
key: AscomConfig
value:
  endpoints:
    - host: alpacahost
      port: 32323
      protocol: http
      devices:
        - entity: mycamera
          device_type: camera
          device_number: 0
          status_frequency: 1.0
        - entity: mydome
          device_type: dome
          device_number: 0
          status_frequency: 1.0
        - entity: myweather
          device_type: observingconditions
          device_number: 0
          status_frequency: 10.0
```

## Usage
- Add the config above to your main configuration YAML.
- Use the CLI to start the service:
  ```sh
  sensorkit service run -s sensorkit.ascom.service -n ascom_service
  ```