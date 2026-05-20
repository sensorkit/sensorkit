# NINA Module

The NINA module provides integration for N.I.N.A. (Nighttime Imaging 'N' Astronomy) via the NINA Advanced API.

## Supported Devices
- Camera
- Dome
- FilterWheel
- Focuser
- Guider
- Mount
- Rotator
- SafetyMonitor
- Switch
- Weather

## Example Config
```yaml
entity: nina_service
key: NinaConfig
value:
  endpoints:
    - host: localhost
      port: 1888
      env_file: .env                  # defines optional NINA_USERNAME, NINA_PASSWORD
      devices:
        camera1:
          device_type: camera
          status_frequency: 1
          timeout: 60
        mount1:
          device_type: mount
          status_frequency: 1
          timeout: 60
```

## Usage
```sh
sensorkit service run nina_service sensorkit.nina.service
```
